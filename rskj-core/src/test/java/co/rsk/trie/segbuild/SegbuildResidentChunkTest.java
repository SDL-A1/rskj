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

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks that a resident chunk answers exactly as the database it was loaded from.
 *
 * <p>The whole point of the arena is that it is indistinguishable from RocksDB except in speed, so
 * the test that matters iterates every node in a real chunk and compares. A sampled check would
 * not do: an open-addressing bug shows up on the colliding keys, which are by definition the ones
 * a sample is least likely to contain.
 *
 * <p>Skipped unless the store is present. Point it elsewhere with {@code -Dsegbuild.dir=...} and
 * choose the chunk with {@code -Dsegbuild.chunk=<directory name>}; the default is the smallest
 * chunk in the set, so the test stays quick.
 */
class SegbuildResidentChunkTest {

    private static final String DEFAULT_ROOT = "/srv/segbuild-ro";
    private static final String TRIE_NODES = "trie_nodes";

    private static Path sealed;
    private static String chunkName;

    @BeforeAll
    static void locateChunk() {
        Path root = Paths.get(System.getProperty("segbuild.dir", DEFAULT_ROOT));
        Assumptions.assumeTrue(Files.isDirectory(root), "segbuild store not present at " + root);

        String requested = System.getProperty("segbuild.chunk");
        if (requested != null) {
            chunkName = requested;
            sealed = root.resolve(requested).resolve(SegbuildChunk.SEALED_DIRECTORY);
            Assumptions.assumeTrue(Files.isDirectory(sealed), "no such chunk: " + sealed);
            return;
        }

        // Smallest chunk by on-disk size, so a correctness run does not cost a gigabyte of heap.
        SegbuildChunkIndex index = SegbuildChunkIndex.scan(root);
        SegbuildChunk smallest = null;
        long smallestBytes = Long.MAX_VALUE;
        for (SegbuildChunk chunk : index.getChunks()) {
            long bytes = directoryBytes(chunk.getSealedDirectory());
            if (bytes > 0 && bytes < smallestBytes) {
                smallestBytes = bytes;
                smallest = chunk;
            }
        }
        assertNotNull(smallest, "no readable chunk found");
        chunkName = smallest.getDirectory().getFileName().toString();
        sealed = smallest.getSealedDirectory();
    }

    private static long directoryBytes(Path dir) {
        try (var stream = Files.list(dir)) {
            return stream.filter(Files::isRegularFile).mapToLong(p -> {
                try {
                    return Files.size(p);
                } catch (Exception e) {
                    return 0L;
                }
            }).sum();
        } catch (Exception e) {
            return 0L;
        }
    }

    private interface WithStore {
        void run(RocksDB db, ColumnFamilyHandle trieNodes) throws Exception;
    }

    private static void withStore(WithStore body) throws Exception {
        RocksDB.loadLibrary();
        List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
        descriptors.add(new ColumnFamilyDescriptor(TRIE_NODES.getBytes(StandardCharsets.UTF_8)));
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        try (DBOptions options = new DBOptions();
             RocksDB db = RocksDB.openReadOnly(options, sealed.toString(), descriptors, handles)) {
            try {
                body.run(db, handles.get(1));
            } finally {
                for (ColumnFamilyHandle handle : handles) {
                    handle.close();
                }
            }
        }
    }

    @Test
    void everyNodeInTheChunkReadsBackIdentically() throws Exception {
        withStore((db, trieNodes) -> {
            SegbuildResidentChunk resident = SegbuildResidentChunk.load(db, trieNodes, 0);

            long seen = 0;
            try (RocksIterator it = db.newIterator(trieNodes)) {
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    byte[] key = it.key();
                    byte[] expected = it.value();
                    byte[] actual = resident.get(key);
                    assertNotNull(actual, () -> "resident chunk lost a node present in " + chunkName);
                    assertArrayEquals(expected, actual, "message differs for a node");
                    assertTrue(resident.contains(key));
                    seen++;
                }
            }

            assertEquals(seen, resident.size(),
                    "resident chunk holds a different number of nodes than the database");
            assertTrue(seen > 0, "chunk " + chunkName + " is empty");

            // Heap should be close to the data rather than a multiple of it; a regression that
            // reintroduced per-node objects would blow past this.
            assertTrue(resident.heapBytes() < resident.dataBytes() * 2,
                    "heap " + resident.heapBytes() + " is more than twice the data "
                            + resident.dataBytes());
        });
    }

    /**
     * A key the chunk does not hold must read as absent, not as some other node's message. With
     * linear probing a wrong answer here is the classic failure: the probe walks into an occupied
     * slot and returns it without checking the key.
     */
    @Test
    void absentKeysReadAsAbsent() throws Exception {
        withStore((db, trieNodes) -> {
            SegbuildResidentChunk resident = SegbuildResidentChunk.load(db, trieNodes, 0);

            // Derive probes from a real key by flipping bits, so they land in the same buckets as
            // occupied slots and exercise probing rather than hitting empty ones.
            byte[] real;
            try (RocksIterator it = db.newIterator(trieNodes)) {
                it.seekToFirst();
                assertTrue(it.isValid());
                real = it.key();
            }

            int absent = 0;
            for (int bit = 0; bit < 256; bit++) {
                byte[] probe = Arrays.copyOf(real, real.length);
                probe[bit / 8] ^= (byte) (1 << (bit % 8));
                if (Arrays.equals(probe, real)) {
                    continue;
                }
                byte[] got = resident.get(probe);
                if (got == null) {
                    absent++;
                } else {
                    // A flipped key may legitimately be another node. If so it must be *that*
                    // node's message, not the one we started from.
                    byte[] fromDb = db.get(trieNodes, probe);
                    assertNotNull(fromDb, "resident chunk invented a node the database lacks");
                    assertArrayEquals(fromDb, got);
                }
            }
            assertTrue(absent > 200, "expected most flipped keys to be absent, got " + absent);

            assertNull(resident.get(new byte[31]), "a short key must not match");
            assertFalse(resident.contains(new byte[33]), "a long key must not match");
        });
    }
}
