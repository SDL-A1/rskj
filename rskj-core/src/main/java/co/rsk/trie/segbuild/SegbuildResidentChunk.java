/*
 * This file is part of RskJ
 * Copyright (C) 2026 RSK Labs Ltd.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package co.rsk.trie.segbuild;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * A whole segbuild chunk held in memory, as byte arenas with an open-addressed index.
 *
 * <h2>Why not a {@code HashMap}</h2>
 *
 * A trie walk is a chain of <em>dependent</em> reads: the next node's hash is inside the node just
 * read, so nothing can be issued in parallel and every read's latency is paid in full. That makes
 * residency worth far more than it looks, and it is why rustock measures ~10x from holding a
 * segment in memory rather than paging it in
 * ({@code docs/trie-segments-design.md} 6.9b in the rustock repository).
 *
 * <p>The obvious Java implementation defeats the purpose. A {@code HashMap<ByteArrayWrapper,
 * byte[]>} spends, per node, a map entry, a wrapper object, a key array and a value array -- about
 * 200 bytes of overhead on a node whose message averages 69. Measured on a real chunk of
 * 10,791,796 nodes that is roughly 2.3 GB of heap for 1.06 GB of data, and 10.8 million live
 * objects for the collector to trace on every full GC.
 *
 * <p>So nodes go into flat {@code byte[]} arenas and the index is a {@code long[]} of packed
 * offsets. Three objects, not thirty million. The same chunk costs about 1.19 GB, barely above the
 * data itself, and the collector has nothing to walk.
 *
 * <h2>Layout</h2>
 *
 * Each entry is written into an arena as its 32-byte key immediately followed by its message:
 *
 * <pre>
 *   arena: [ key(32) | message(len) ][ key(32) | message(len) ]...
 * </pre>
 *
 * Keeping the key beside the message is what makes the index one array rather than two. A probe
 * has to confirm it found the right key, and reading those 32 bytes from the arena touches the
 * same cache lines the message will be read from anyway.
 *
 * <p>A slot packs the entry's location into one {@code long}:
 *
 * <pre>
 *   bits  0..29  offset of the key within its arena   (arenas are 1 GiB, so 30 bits)
 *   bits 30..45  message length in bytes              (16 bits; nodes observed up to ~250)
 *   bits 46..53  arena index                          (8 bits, so up to 256 GiB)
 *   bit  63      set, to distinguish an occupied slot from an empty one
 * </pre>
 *
 * Arenas are 1 GiB because a Java array is limited to {@code Integer.MAX_VALUE} elements; several
 * arenas also keep each allocation small enough to succeed on a fragmented heap.
 *
 * <h2>Hashing</h2>
 *
 * Keys are node hashes, so they are already uniformly distributed and need no mixing: the first
 * eight bytes are taken as the hash. Collisions are resolved by linear probing, which suits the
 * access pattern -- a probe that misses lands on the adjacent cache line.
 *
 * <h2>What this does not do</h2>
 *
 * It is read-only and immutable once built, and it is not thread-safe to build (one instance is
 * built by one thread, then shared freely for reading, which needs no synchronisation because
 * nothing mutates).
 */
public final class SegbuildResidentChunk {

    private static final Logger logger = LoggerFactory.getLogger("segbuild");

    /** Node hashes are 32 bytes, and the layout depends on that. */
    public static final int KEY_LENGTH = 32;

    /** 1 GiB, which is both under {@code Integer.MAX_VALUE} and what the 30 offset bits allow. */
    static final int ARENA_BYTES = 1 << 30;

    private static final int OFFSET_BITS = 30;
    /**
     * 24 bits, so 16 MB. Node messages are mostly ~69 bytes, but a trie *long value* is inlined
     * in the same column family and is far larger: 21,812 bytes was observed in the smallest
     * chunk of the real set. 16 bits would have covered that and then failed on the first
     * contract whose code ran past 65,535, so the extra byte buys the difference between a
     * comfortable margin and a latent one. The bits were spare.
     */
    private static final int LENGTH_BITS = 24;
    private static final int ARENA_BITS = 8;

    private static final long OFFSET_MASK = (1L << OFFSET_BITS) - 1;
    private static final long LENGTH_MASK = (1L << LENGTH_BITS) - 1;
    private static final long ARENA_MASK = (1L << ARENA_BITS) - 1;
    private static final long OCCUPIED = 1L << 63;

    /** The largest message the 16 length bits can describe. */
    static final int MAX_MESSAGE_LENGTH = (1 << LENGTH_BITS) - 1;


    private final byte[][] arenas;
    private final long[] slots;
    private final int slotMask;
    private final int size;
    private final long dataBytes;
    private long loadMillis;

    private SegbuildResidentChunk(byte[][] arenas, long[] slots, int slotMask, int size, long dataBytes) {
        this.arenas = arenas;
        this.slots = slots;
        this.slotMask = slotMask;
        this.size = size;
        this.dataBytes = dataBytes;
    }

    /**
     * How long the load took. Worth separating from replay in any measurement:
     * loading a 2.7 GB chunk costs tens of seconds, which swamps a short run
     * and would otherwise be reported as slow execution.
     */
    public long loadMillis() {
        return loadMillis;
    }

    /** How many nodes are held. */
    public int size() {
        return size;
    }

    /** Key and message bytes held, excluding the index. */
    public long dataBytes() {
        return dataBytes;
    }

    /** Bytes of heap this occupies: the arenas as allocated, plus the index. */
    public long heapBytes() {
        long total = (long) slots.length * Long.BYTES;
        for (byte[] arena : arenas) {
            total += arena.length;
        }
        return total;
    }

    /**
     * Loads every node in {@code columnFamily} into memory.
     *
     * <p>Two passes. The first counts entries and bytes so the arenas and index can be sized
     * exactly; the second fills them. Counting first costs a second scan of a file already in page
     * cache, and buys never having to grow -- growing a 1 GB arena means holding two of them, which
     * is how a load that fits becomes a load that does not.
     *
     * @param expectedEntries a hint used only to size the first pass's reporting; may be 0
     */
    /**
     * Loads every node in {@code columnFamily} into memory.
     *
     * <h3>One pass, and no allocation per node</h3>
     *
     * The obvious implementation costs far more than the data. {@code it.key()}
     * and {@code it.value()} each allocate a fresh {@code byte[]}, so a
     * two-pass load -- one to measure, one to fill -- allocates four arrays per
     * node, and the measuring pass allocates two of them purely to read their
     * lengths. On a 25.3M-node chunk that is 101 million throwaway arrays:
     * measured at 92 s for 2,468 MB, or 27 MB/s, which is nowhere near the
     * disk. The time was garbage collection.
     *
     * So this reads with {@code key(byte[], int, int)} and
     * {@code value(byte[], int, int)}, whose destination is the arena itself.
     * One copy out of RocksDB, no intermediate object, nothing for the
     * collector to trace.
     *
     * The measuring pass is gone with it. Arenas are appended to as they fill,
     * so no byte estimate is needed at all, and the index is sized from
     * {@code rocksdb.estimate-num-keys} and rehashed if that estimate proves
     * low -- which costs one rehash at worst, against a guaranteed second scan.
     */
    public static SegbuildResidentChunk load(
            @Nonnull RocksDB db, @Nonnull ColumnFamilyHandle columnFamily, long expectedEntries) {
        long start = System.nanoTime();

        long estimate = expectedEntries;
        if (estimate <= 0) {
            try {
                estimate = db.getLongProperty(columnFamily, "rocksdb.estimate-num-keys");
            } catch (RocksDBException e) {
                estimate = 0;
            }
        }
        // Generous: a rehash costs a full re-insert, while slack costs 8 bytes
        // a slot. The estimate is usually within a few percent.
        int slotCount = tableSizeFor(Math.max(estimate, 1024) * 4 / 3);
        long[] slots = new long[slotCount];

        // Size arenas from RocksDB's own estimate of the data, and shrink as the
        // remainder does. Allocating a full ARENA_BYTES up front costs 1 GiB of
        // heap for a 1 MB chunk -- the test asserting heap < 2x data catches
        // exactly that -- and sizing only the first arena leaves the last one a
        // full gigabyte for its tail.
        long sizeHint;
        try {
            sizeHint = db.getLongProperty(columnFamily, "rocksdb.total-sst-files-size");
        } catch (RocksDBException e) {
            sizeHint = 0;
        }
        if (sizeHint <= 0) {
            // A chunk whose data is still in the write-ahead log reports no SST
            // bytes. Start modestly and grow.
            sizeHint = 64L << 20;
        }
        long remainingHint = sizeHint;

        List<byte[]> arenas = new ArrayList<>();
        remainingHint -= appendArena(arenas, remainingHint, 0);
        int arenaIndex = 0;
        int arenaOffset = 0;

        long count = 0;
        long bytes = 0;
        int maxLength = 0;

        try (ReadOptions options = readOptions(); RocksIterator it = db.newIterator(columnFamily, options)) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                byte[] arena = arenas.get(arenaIndex);

                // There must be somewhere to attempt the read before asking how
                // long the value is: an exactly-full arena leaves no room, and
                // standing in for that with a sentinel length is how the size
                // check below came to see a 2 GB node.
                if (arena.length - arenaOffset <= KEY_LENGTH) {
                    remainingHint -= nextArena(arenas, remainingHint, 0);
                    arenaIndex++;
                    arena = arenas.get(arenaIndex);
                    arenaOffset = 0;
                }

                // Read the value straight into where it will live. The call
                // returns the value's true length even when it did not fit, so
                // a short arena is detected without having asked its size.
                int room = arena.length - arenaOffset - KEY_LENGTH;
                int valueLength = it.value(arena, arenaOffset + KEY_LENGTH, room);
                if (valueLength > room) {
                    // Whatever was written into the old arena's tail is
                    // abandoned: nothing indexes it. The failed read told us the
                    // true length, so a rare large value gets an arena that fits
                    // it without inflating every other arena to suit.
                    remainingHint -= nextArena(arenas, remainingHint, KEY_LENGTH + valueLength);
                    arenaIndex++;
                    arena = arenas.get(arenaIndex);
                    arenaOffset = 0;
                    int reread = it.value(arena, KEY_LENGTH, arena.length - KEY_LENGTH);
                    if (reread != valueLength) {
                        throw new IllegalStateException(String.format(
                                "Segbuild node message read as %d bytes then %d; the store "
                                        + "changed under us", valueLength, reread));
                    }
                }
                if (valueLength > MAX_MESSAGE_LENGTH) {
                    throw new IllegalStateException(String.format(
                            "Segbuild node message is %d bytes, over the %d the index can "
                                    + "describe; widen LENGTH_BITS",
                            valueLength, MAX_MESSAGE_LENGTH));
                }

                int keyLength = it.key(arena, arenaOffset, KEY_LENGTH);
                if (keyLength != KEY_LENGTH) {
                    throw new IllegalStateException(String.format(
                            "Segbuild node key is %d bytes, expected %d; this store is not a trie "
                                    + "node store or its layout has changed",
                            keyLength, KEY_LENGTH));
                }

                if ((count + 1) * 4 > (long) slots.length * 3) {
                    slots = rehash(slots, arenas);
                }
                insertAt(slots, slots.length - 1, arenas, pack(arenaIndex, arenaOffset, valueLength),
                        arena, arenaOffset);

                arenaOffset += KEY_LENGTH + valueLength;
                bytes += KEY_LENGTH + valueLength;
                if (valueLength > maxLength) {
                    maxLength = valueLength;
                }
                count++;
            }
        }

        if (count > Integer.MAX_VALUE) {
            throw new IllegalStateException("Segbuild chunk holds " + count + " nodes, too many to index");
        }

        // Trim the final arena to what was used. The size hint comes from
        // rocksdb.total-sst-files-size, which counts index and filter blocks as
        // well as keys and values -- for a 2,468 MB chunk it reports 2,718 MB --
        // so the tail would otherwise carry a few hundred MB of slack that
        // counts against how many segments fit in a given heap. One copy of the
        // last arena costs a fraction of a second against a 45 s load.
        int last = arenas.size() - 1;
        if (arenaOffset < arenas.get(last).length) {
            arenas.set(last, java.util.Arrays.copyOf(arenas.get(last), Math.max(arenaOffset, 1)));
        }

        SegbuildResidentChunk chunk = new SegbuildResidentChunk(
                arenas.toArray(new byte[0][]), slots, slots.length - 1, (int) count, bytes);
        chunk.loadMillis = (System.nanoTime() - start) / 1_000_000L;
        logger.info(
                "Loaded {} nodes ({} MB of data, {} MB of heap, longest message {} B) in {} ms",
                count, bytes >> 20, chunk.heapBytes() >> 20, maxLength, chunk.loadMillis);
        return chunk;
    }

    /**
     * Adds an arena sized to what is left of the estimate, never smaller than
     * {@code mustFit} and never larger than {@link #ARENA_BYTES} (a Java array
     * cannot exceed {@code Integer.MAX_VALUE} anyway).
     *
     * @return the number of bytes allocated, to subtract from the estimate
     */
    /**
     * Appends an arena, refusing rather than silently exceeding the index's
     * capacity for arena indices.
     */
    private static long nextArena(List<byte[]> arenas, long remainingHint, int mustFit) {
        if (arenas.size() > ARENA_MASK) {
            throw new IllegalStateException(
                    "Segbuild chunk needs more than " + (ARENA_MASK + 1) + " arenas");
        }
        return appendArena(arenas, remainingHint, mustFit);
    }

    private static long appendArena(List<byte[]> arenas, long remainingHint, int mustFit) {
        long want = Math.max(remainingHint, 0) + 64 * 1024;
        want = Math.max(want, mustFit);
        want = Math.max(want, 1L << 20);
        want = Math.min(want, ARENA_BYTES);
        if (want < mustFit) {
            throw new IllegalStateException(
                    "Segbuild entry of " + mustFit + " bytes does not fit an arena");
        }
        arenas.add(new byte[(int) want]);
        return want;
    }

    private static int tableSizeFor(long wanted) {
        long n = 16;
        while (n < wanted && n < (1L << 30)) {
            n <<= 1;
        }
        return (int) n;
    }

    /** Doubles the index and re-inserts every entry, reading keys from the arenas. */
    private static long[] rehash(long[] old, List<byte[]> arenas) {
        long[] bigger = new long[old.length << 1];
        int mask = bigger.length - 1;
        for (long slot : old) {
            if (slot == 0) {
                continue;
            }
            byte[] arena = arenas.get(arenaOf(slot));
            int at = bucketOfArena(arena, offsetOf(slot), mask);
            while (bigger[at] != 0) {
                at = (at + 1) & mask;
            }
            bigger[at] = slot;
        }
        return bigger;
    }

    private static ReadOptions readOptions() {
        // A full scan should not evict whatever the block cache holds, and it does not benefit
        // from readahead bookkeeping per key.
        return new ReadOptions().setFillCache(false).setVerifyChecksums(false);
    }

    private static long pack(int arenaIndex, int offset, int length) {
        return OCCUPIED
                | ((long) arenaIndex & ARENA_MASK) << (OFFSET_BITS + LENGTH_BITS)
                | ((long) length & LENGTH_MASK) << OFFSET_BITS
                | ((long) offset & OFFSET_MASK);
    }

    private static int arenaOf(long slot) {
        return (int) ((slot >>> (OFFSET_BITS + LENGTH_BITS)) & ARENA_MASK);
    }

    private static int lengthOf(long slot) {
        return (int) ((slot >>> OFFSET_BITS) & LENGTH_MASK);
    }

    private static int offsetOf(long slot) {
        return (int) (slot & OFFSET_MASK);
    }

    /** Node hashes are uniform already, so the leading eight bytes are the hash. */
    private static int bucketOf(byte[] key, int mask) {
        long h = ((long) (key[0] & 0xff) << 56)
                | ((long) (key[1] & 0xff) << 48)
                | ((long) (key[2] & 0xff) << 40)
                | ((long) (key[3] & 0xff) << 32)
                | ((long) (key[4] & 0xff) << 24)
                | ((long) (key[5] & 0xff) << 16)
                | ((long) (key[6] & 0xff) << 8)
                | (key[7] & 0xff);
        return (int) (h ^ (h >>> 32)) & mask;
    }

    /** The bucket for a key that is already sitting in an arena. */
    private static int bucketOfArena(byte[] arena, int offset, int mask) {
        long h = ((long) (arena[offset] & 0xff) << 56)
                | ((long) (arena[offset + 1] & 0xff) << 48)
                | ((long) (arena[offset + 2] & 0xff) << 40)
                | ((long) (arena[offset + 3] & 0xff) << 32)
                | ((long) (arena[offset + 4] & 0xff) << 24)
                | ((long) (arena[offset + 5] & 0xff) << 16)
                | ((long) (arena[offset + 6] & 0xff) << 8)
                | (arena[offset + 7] & 0xff);
        return (int) (h ^ (h >>> 32)) & mask;
    }

    /**
     * Inserts an entry whose key is already written into {@code arena} at
     * {@code offset}, so the key never exists as a separate object.
     */
    private static void insertAt(long[] slots, int mask, List<byte[]> arenas, long packed,
                                 byte[] arena, int offset) {
        int at = bucketOfArena(arena, offset, mask);
        while (slots[at] != 0) {
            // Content-addressed, so a repeated key carries a repeated message.
            if (keyMatchesArena(arenas, slots[at], arena, offset)) {
                return;
            }
            at = (at + 1) & mask;
        }
        slots[at] = packed;
    }

    private static boolean keyMatchesArena(List<byte[]> arenas, long slot, byte[] other, int otherOffset) {
        byte[] arena = arenas.get(arenaOf(slot));
        int at = offsetOf(slot);
        for (int i = 0; i < KEY_LENGTH; i++) {
            if (arena[at + i] != other[otherOffset + i]) {
                return false;
            }
        }
        return true;
    }

    private static boolean keyMatches(byte[][] arenas, long slot, byte[] key) {
        byte[] arena = arenas[arenaOf(slot)];
        int at = offsetOf(slot);
        for (int i = 0; i < KEY_LENGTH; i++) {
            if (arena[at + i] != key[i]) {
                return false;
            }
        }
        return true;
    }


    /**
     * The message for {@code key}, or null if this chunk does not hold it.
     *
     * <p>Returns a fresh array. Handing out a view into the arena would be faster still, but
     * callers of a {@code TrieStore} are entitled to keep and even modify what they are given, and
     * a shared mutable arena is not worth the one copy of 69 average bytes.
     */
    @Nullable
    public byte[] get(@Nonnull byte[] key) {
        if (key.length != KEY_LENGTH) {
            return null;
        }
        int at = bucketOf(key, slotMask);
        long slot;
        while ((slot = slots[at]) != 0) {
            if (keyMatches(arenas, slot, key)) {
                int length = lengthOf(slot);
                byte[] out = new byte[length];
                System.arraycopy(arenas[arenaOf(slot)], offsetOf(slot) + KEY_LENGTH, out, 0, length);
                return out;
            }
            at = (at + 1) & slotMask;
        }
        return null;
    }

    /** Whether this chunk holds {@code key}, without copying its message. */
    public boolean contains(@Nonnull byte[] key) {
        if (key.length != KEY_LENGTH) {
            return false;
        }
        int at = bucketOf(key, slotMask);
        long slot;
        while ((slot = slots[at]) != 0) {
            if (keyMatches(arenas, slot, key)) {
                return true;
            }
            at = (at + 1) & slotMask;
        }
        return false;
    }
}
