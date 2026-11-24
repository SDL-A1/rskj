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

### How long the whole chain takes, and how many processes

**RAM decides how many workers fit. Cores decide how many help.** Those are
different limits and the second one binds sooner than people expect.

A worker holds one segment resident. Measured per-worker footprint for rskj is
**~2.0 GB** on a median segment and **~3.7 GB** on the largest in the set used
for those measurements. The current set's largest segment is bigger again
(`094`, 4.3 GB on disk), so budget nearer **~5 GB** for the peak.

| Machine | cores | RAM | workers that *fit* | workers that *help* | whole chain, rskj |
|---|---|---|---|---|---|
| this test box (x86) | 4 | 8 GB | 3 / 1 at peak | 3 | **~16–17 h** *(measured basis)* |
| same box, more RAM | 4 | 16 GB | 7 / 3 | 3 | ~16–17 h — **unchanged** |
| same box, more RAM | 4 | 32 GB | 15 / 6 | 3 | ~16–17 h — **unchanged** |
| **MacBook Pro M4 Max** | **14–16** | **36 GB** | 17 / 6 | ~8–10 | **~4–8 h** *(projection)* |

Only the first row has measurement behind it: 3 workers at ~16 h replay, ~17 h
including segment loading. The 16 and 32 GB rows are arithmetic on the measured
per-worker footprint, and they are here because the answer is counter-intuitive
— **more RAM alone buys nothing**. On four cores the ceiling is three workers
whatever the memory, and that was measured rather than assumed: a fourth worker
contributed about **4% of throughput for 25% of the memory**.

### The M4 Max row, and how much to trust it

It is the machine most rskj developers actually have, so it is worth
estimating — but **nothing here has been measured on Apple Silicon**, and the
band is wide for two honest reasons.

The M4 Max has 14 or 16 CPU cores (10–12 performance plus 4 efficiency) and
36 GB of unified memory. At ~2.0 GB per worker on a median segment, memory
stops being the limit: 8–10 workers fit comfortably with headroom for one peak
segment. So unlike the 16 and 32 GB rows above, the extra RAM *is* usable
here, because the cores exist to use it.

Two things pull the estimate in opposite directions, and neither is measured:

- **Per-core speed is higher.** JVM throughput on an M4 performance core should
  beat this box's x86 cores, plausibly by 1.5–2x.
- **Scaling stays sub-linear.** Two workers measured **1.81x** and **1.62x**,
  not 2x, because memory bandwidth and page cache are shared. Eight workers
  will not be 8x. Unified memory is fast, but it is one pool, shared with the
  GPU.

Combining a sub-linear 8-worker speedup with a faster core gives roughly
**4–8 hours**. Treat the lower end as optimistic. If you run it, the measured
number is worth more than this paragraph.

**Disk matters more than you would expect on a laptop.** The archive is ~148 GB
and extracts to ~148 GB, so a straight `tar -xf` wants **~300 GB free** before
the tarball can be deleted. On a 512 GB machine that is most of the free space.
Extract onto an external SSD if you have one — throughput there is not the
bottleneck, since the work is CPU-bound once a chunk is resident.

**Use an ARM-native JDK 17** (Temurin or Zulu for `aarch64`). A Rosetta-
translated x86 JDK will run and will be substantially slower, which would make
any timing taken on it misleading.

Measured two-worker speedup is **1.81x** (pre-fork blocks) and **1.62x**
(Unitrie era) — not 2x, because memory bandwidth and page cache are shared. The
3-worker figures extrapolate ~2.2x from that.

So to actually go faster than ~16 h you need **more cores and more RAM
together**, roughly 2 GB of RAM per core you intend to use, plus headroom for
one peak segment. A 16-core, 32 GB machine should approach 8 workers; that is a
projection, not a measurement, and nobody has run it.

One caveat carried over from where these numbers come from: rskj's replay rate
was derived by scaling rustock's measured per-era times by an observed 0.618
ratio, and that ratio was measured on recent blocks only. Bridge-heavy ranges
may differ. It is the single number here most worth re-measuring before anyone
plans around it.

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
