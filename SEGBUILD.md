# Replaying mainnet from a segbuild segment set

This branch can execute historical RSK blocks against a **segbuild segment
set** instead of a full-history unitrie. This file is the operational half: the
classes are documented in their javadoc, but none of it tells you which entry
point to use or what to pass it.

## What a segment set is

107 independent, read-only trie stores, one per block range, together covering
mainnet #1 to #9,429,888. Each is **self-contained for its own range**: during
the build every node *read* was copied in, not only every node written, so a
chunk holds everything its range touched. Executing a block needs exactly one
chunk open and nothing else — no cross-chunk probing, no fallback store, no
full-history trie.

The set is ~148 GB. Blocks and receipts are **not** in it; those come from an
ordinary rskj database directory.

## Requirements

**JDK 17.** `sourceCompatibility` is 17 and the Gradle version here cannot run
under a newer JDK. Under Java 25 the build dies immediately with

```
BUG! exception in phase 'semantic analysis' ... Unsupported class file major version 69
```

which looks alarming and has nothing to do with segbuild. Set `JAVA_HOME` to a
17 installation before anything else.

## Running it

`co.rsk.cli.tools.ExecuteBlocks` is the entry point. It describes itself as
experimental and unsupported, which is accurate about its status and
misleading about whether it works — this path is exercised.

```sh
JAVA_HOME=/path/to/jdk-17 ./gradlew :rskj-core:run \
  --args="--fromBlock 3810000 --toBlock 3814999 \
          --trieStore SEGBUILD \
          --segbuildDir /path/to/segbuild \
          --segbuildResident \
          -Xdatabase.dir=/path/to/rskj-snapshot"
```

| argument | what it is |
|---|---|
| `--trieStore SEGBUILD` | read state from a segment set rather than a unitrie |
| `--segbuildDir` | the directory holding the `NNN-start-end` segment directories |
| `--segbuildResident` | load the chunk into memory first; worth it for any range of size, costly for a handful of blocks |
| `-Xdatabase.dir` | an ordinary rskj database for **blocks**; state does not come from here |

The range must lie inside one chunk, or the run pays to load a second one
partway through.

## Parallelism

There is none inside the process, and that is deliberate. `SegbuildTrieStore`
holds **one chunk open at a time** and is moved across boundaries with
`positionAt(long)`.

Parallelism is **one process per chunk range** — the chunks share nothing, so N
processes over disjoint ranges scale with no coordination. There is no built-in
scheduler; pick the ranges from the directory names, which carry their own
bounds (`NNN-firstBlock-lastBlock`).

On a memory-bounded machine, note that `--segbuildResident` costs roughly the
chunk's size in heap, and chunk sizes vary by more than three orders of
magnitude -- from **2 MB** to **4.3 GB** -- because ranges were cut by bytes
written, not by block count. Schedule by free memory rather than by core
count, and do not assume a chunk fits just because the last one did.

## A missing node is an error, not an absence

Worth understanding before you see it, because the exception reads like a bug
in this code and is not.

Inside a chunk's own range, a node that is not found means the chunk is
**damaged or incomplete**. Reporting "absent" would let the caller compute a
wrong state root and carry on, so `SegbuildTrieStore` throws
`SegbuildMissingNodeException` instead.

If you hit one, the chunk is at fault — not the executor, and not consensus.
That distinction is expensive to learn the other way round: a missing node read
as a genuine absence produces a state-root mismatch that looks exactly like a
consensus divergence in whatever code happens to be running. It cost the
Rustock team a day of investigation before the same guard was added there.

Outside a chunk's range misses are expected and carry no information, which is
why the store must be positioned on the chunk matching the block.

## Verifying the artifact

Check the SHA-256 you were given against the archive before extracting. Inside,
each `NNN-start-end` directory must contain `segments.csv` and `sealed/`; the
reader uses the presence of `segments.csv` to tell a real segment from an empty
directory.

The archive deliberately omits superseded and abandoned segment directories
that were present during the build. Coverage is contiguous over #1..#9,429,888
with the 107 that remain.
