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
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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

    /**
     * Index load factor. 0.75 keeps the {@code long[]} modest -- 134 MB for 10.8M nodes -- while
     * leaving linear probing short. Lower would cost memory to save probes that are already cheap,
     * since a probe walks forward into the next cache line.
     */
    private static final double LOAD_FACTOR = 0.75;

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
    public static SegbuildResidentChunk load(
            @Nonnull RocksDB db, @Nonnull ColumnFamilyHandle columnFamily, long expectedEntries) {
        long start = System.nanoTime();

        long count = 0;
        long bytes = 0;
        int maxLength = 0;
        try (ReadOptions options = readOptions(); RocksIterator it = db.newIterator(columnFamily, options)) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                int keyLength = it.key().length;
                int valueLength = it.value().length;
                if (keyLength != KEY_LENGTH) {
                    throw new IllegalStateException(String.format(
                            "Segbuild node key is %d bytes, expected %d; this store is not a trie "
                                    + "node store or its layout has changed",
                            keyLength, KEY_LENGTH));
                }
                if (valueLength > MAX_MESSAGE_LENGTH) {
                    throw new IllegalStateException(String.format(
                            "Segbuild node message is %d bytes, over the %d the index can "
                                    + "describe; widen LENGTH_BITS",
                            valueLength, MAX_MESSAGE_LENGTH));
                }
                count++;
                bytes += KEY_LENGTH + valueLength;
                if (valueLength > maxLength) {
                    maxLength = valueLength;
                }
            }
        }

        if (count > Integer.MAX_VALUE) {
            throw new IllegalStateException("Segbuild chunk holds " + count + " nodes, too many to index");
        }

        int slotCount = Integer.highestOneBit(Math.max(16, (int) Math.ceil(count / LOAD_FACTOR))) << 1;
        long[] slots = new long[slotCount];
        byte[][] arenas = allocateArenas(bytes, KEY_LENGTH + maxLength);

        long filled = 0;
        int arenaIndex = 0;
        int arenaOffset = 0;
        try (ReadOptions options = readOptions(); RocksIterator it = db.newIterator(columnFamily, options)) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                byte[] key = it.key();
                byte[] value = it.value();
                int need = KEY_LENGTH + value.length;

                // Never split an entry across arenas: the message must be one contiguous run so a
                // reader can hand out a single array.
                if (arenaOffset + need > arenas[arenaIndex].length) {
                    arenaIndex++;
                    arenaOffset = 0;
                    if (arenaIndex >= arenas.length) {
                        throw new IllegalStateException("Segbuild arenas were undersized; this is a bug");
                    }
                }

                System.arraycopy(key, 0, arenas[arenaIndex], arenaOffset, KEY_LENGTH);
                System.arraycopy(value, 0, arenas[arenaIndex], arenaOffset + KEY_LENGTH, value.length);

                insert(slots, slotCount - 1, arenas, pack(arenaIndex, arenaOffset, value.length), key);
                arenaOffset += need;
                filled++;
            }
        }

        if (filled != count) {
            throw new IllegalStateException(String.format(
                    "Segbuild load counted %d nodes then wrote %d; the store changed under us",
                    count, filled));
        }

        SegbuildResidentChunk chunk =
                new SegbuildResidentChunk(arenas, slots, slotCount - 1, (int) count, bytes);
        chunk.loadMillis = (System.nanoTime() - start) / 1_000_000L;
        logger.info(
                "Loaded {} nodes ({} MB of data, {} MB of heap, longest message {} B) in {} ms",
                count, bytes >> 20, chunk.heapBytes() >> 20, maxLength,
                (System.nanoTime() - start) / 1_000_000L);
        if (expectedEntries > 0 && expectedEntries != count) {
            logger.info("Expected {} entries, found {}", expectedEntries, count);
        }
        return chunk;
    }

    private static ReadOptions readOptions() {
        // A full scan should not evict whatever the block cache holds, and it does not benefit
        // from readahead bookkeeping per key.
        return new ReadOptions().setFillCache(false).setVerifyChecksums(false);
    }

    /**
     * Arenas sized to the data, with enough slack that no entry straddles a boundary.
     *
     * <p>The slack is the largest entry actually present, not the largest the index could
     * describe. Using the theoretical maximum would floor every arena at 16 MB and make a 1 MB
     * chunk cost sixteen times its size.
     */
    private static byte[][] allocateArenas(long bytes, int maxEntry) {
        // Everything in one arena cannot straddle anything, so size it exactly.
        if (bytes <= ARENA_BYTES) {
            return new byte[][] {new byte[(int) Math.max(bytes, maxEntry)]};
        }

        // Each arena may waste up to one entry's worth at its tail, so divide by the usable span.
        int usable = ARENA_BYTES - maxEntry;
        if (usable <= 0) {
            throw new IllegalStateException(
                    "Segbuild entry of " + maxEntry + " bytes does not fit an arena");
        }
        int arenaCount = (int) ((bytes + usable - 1) / usable);
        if (arenaCount > ARENA_MASK + 1) {
            throw new IllegalStateException(
                    "Segbuild chunk needs " + arenaCount + " arenas, over the " + (ARENA_MASK + 1)
                            + " the index can describe");
        }
        byte[][] arenas = new byte[arenaCount][];
        for (int i = 0; i < arenaCount - 1; i++) {
            arenas[i] = new byte[ARENA_BYTES];
        }
        // Size the last arena to what is left rather than giving it a full gigabyte. A chunk
        // holding just over 1 GiB otherwise gets two full arenas -- 2 GiB of heap for 1.05 GiB of
        // data -- and the pre-fork chunks sit exactly on that boundary.
        long placed = (long) (arenaCount - 1) * usable;
        long tail = Math.max(bytes - placed, 0) + maxEntry;
        arenas[arenaCount - 1] = new byte[(int) Math.min(tail, ARENA_BYTES)];
        return arenas;
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

    private static void insert(long[] slots, int mask, byte[][] arenas, long packed, byte[] key) {
        int at = bucketOf(key, mask);
        while (slots[at] != 0) {
            // Content-addressed, so an identical key carries an identical message; RocksDB will
            // not hand out the same key twice anyway, but tolerating it costs nothing.
            if (keyMatches(arenas, slots[at], key)) {
                return;
            }
            at = (at + 1) & mask;
        }
        slots[at] = packed;
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
