# Dynoxide Benchmarks

Reproducible benchmarks comparing Dynoxide against DynamoDB Local and LocalStack.

## Quick Start

```bash
cd benchmarks

# Run all criterion micro-benchmarks (embedded mode)
cargo bench

# Run the full workload macro-benchmark (embedded mode)
cargo bench --bench embedded_macro

# Run the CI pipeline simulation (embedded + HTTP modes)
cargo run --release --bin ci_pipeline_bench

# Run against DynamoDB Local (requires Docker)
../benchmarks/scripts/start_dynamodb_local.sh
cargo run --release --bin ci_pipeline_bench -- --ddb-endpoint http://localhost:8000

# Run memory profiler
cargo run --release --bin memory_profiler

# Run startup benchmark
cargo run --release --bin startup_bench

# Run workload driver against a specific endpoint
cargo run --release --bin workload_driver -- --endpoint-url http://localhost:8123

# Run iai-callgrind instruction-count benchmarks (Linux only, requires Valgrind)
cargo bench --bench iai_core --features iai-callgrind

# Generate charts from results
python3 scripts/generate_report.py --results-dir results --output-dir results/charts
```

## Four-Tier Comparison Model

| Tier | What | How |
|------|------|-----|
| **Dynoxide Embedded** | Direct Rust API calls via `Database::memory()` | No network, no HTTP, no serialisation overhead |
| **Dynoxide HTTP** | Axum server via `http-server` feature | Quantifies the HTTP overhead eliminated by embedded mode |
| **DynamoDB Local** | Docker container, JVM-based | The incumbent to beat |
| **LocalStack** | Docker container, Python + DynamoDB Local | Startup, image size, idle memory only |

LocalStack uses DynamoDB Local internally as its DynamoDB engine — its emulation layer is DynamoDB Local's JVM process wrapped with Python routing. This is why its startup and memory numbers are higher than DynamoDB Local alone: you're paying for both runtimes. We benchmark LocalStack for startup time, image size, and idle memory only — operation-level benchmarks would measure DynamoDB Local's performance through an additional network hop.

## Results: Local Development (Apple Silicon)

Results from a Mac Studio (M-series). These reflect the experience of a developer running Dynoxide locally as a dev server or running tests directly.

### Cold Startup

| Target | Mean | vs DynamoDB Local |
|--------|------|-------------------|
| Dynoxide Embedded | <!-- bench:local_startup_embedded -->**~0.2ms**<!-- /bench --> | <!-- bench:local_startup_embedded_ratio -->**~10,046x faster**<!-- /bench --> |
| Dynoxide HTTP | <!-- bench:local_startup_http -->**~15ms**<!-- /bench --> | <!-- bench:local_startup_http_ratio -->**~148x faster**<!-- /bench --> |
| DynamoDB Local | <!-- bench:local_startup_ddb_local -->~2,287ms<!-- /bench --> | — |
| LocalStack | <!-- bench:local_startup_localstack -->~6,231ms<!-- /bench --> | <!-- bench:local_startup_localstack_ratio -->2.7x slower<!-- /bench --> |

### Per-Operation Comparison (Dynoxide HTTP vs DynamoDB Local)

| Operation | Dynoxide HTTP (p50) | DynamoDB Local (p50) | Speedup |
|-----------|-------------------|---------------------|---------|
| CreateTable | <!-- bench:local_op_CreateTable_http -->0.40ms<!-- /bench --> | <!-- bench:local_op_CreateTable_ddb -->13.5ms<!-- /bench --> | <!-- bench:local_op_CreateTable_ratio -->**34x**<!-- /bench --> |
| GetItem | <!-- bench:local_op_GetItem_http -->0.12ms<!-- /bench --> | <!-- bench:local_op_GetItem_ddb -->0.80ms<!-- /bench --> | <!-- bench:local_op_GetItem_ratio -->**6.7x**<!-- /bench --> |
| PutItem | <!-- bench:local_op_PutItem_http -->0.14ms<!-- /bench --> | <!-- bench:local_op_PutItem_ddb -->0.92ms<!-- /bench --> | <!-- bench:local_op_PutItem_ratio -->**6.5x**<!-- /bench --> |
| Query (base) | <!-- bench:local_op_Query_http -->0.13ms<!-- /bench --> | <!-- bench:local_op_Query_ddb -->1.7ms<!-- /bench --> | <!-- bench:local_op_Query_ratio -->**13x**<!-- /bench --> |
| Query (GSI) | <!-- bench:local_op_QueryGSI_http -->0.13ms<!-- /bench --> | <!-- bench:local_op_QueryGSI_ddb -->1.2ms<!-- /bench --> | <!-- bench:local_op_QueryGSI_ratio -->**8.7x**<!-- /bench --> |
| Query (paginated) | <!-- bench:local_op_QueryPaginated_http -->1.5ms<!-- /bench --> | <!-- bench:local_op_QueryPaginated_ddb -->4.7ms<!-- /bench --> | <!-- bench:local_op_QueryPaginated_ratio -->**3.1x**<!-- /bench --> |
| Scan (full table) | <!-- bench:local_op_Scan_http -->45.6ms<!-- /bench --> | <!-- bench:local_op_Scan_ddb -->101.3ms<!-- /bench --> | <!-- bench:local_op_Scan_ratio -->**2.2x**<!-- /bench --> |
| UpdateItem | <!-- bench:local_op_UpdateItem_http -->0.15ms<!-- /bench --> | <!-- bench:local_op_UpdateItem_ddb -->1.3ms<!-- /bench --> | <!-- bench:local_op_UpdateItem_ratio -->**9.1x**<!-- /bench --> |
| TransactWriteItems | <!-- bench:local_op_TransactWriteItems_http -->0.28ms<!-- /bench --> | <!-- bench:local_op_TransactWriteItems_ddb -->3.9ms<!-- /bench --> | <!-- bench:local_op_TransactWriteItems_ratio -->**14x**<!-- /bench --> |
| BatchGetItem (100 keys) | <!-- bench:local_op_BatchGetItem_http -->1.7ms<!-- /bench --> | <!-- bench:local_op_BatchGetItem_ddb -->11.7ms<!-- /bench --> | <!-- bench:local_op_BatchGetItem_ratio -->**6.9x**<!-- /bench --> |
| BatchWriteItem (25 items) | <!-- bench:local_op_BatchWriteItem_http -->1.1ms<!-- /bench --> | <!-- bench:local_op_BatchWriteItem_ddb -->7.5ms<!-- /bench --> | <!-- bench:local_op_BatchWriteItem_ratio -->**6.7x**<!-- /bench --> |
| DeleteItem | <!-- bench:local_op_DeleteItem_http -->0.12ms<!-- /bench --> | <!-- bench:local_op_DeleteItem_ddb -->0.97ms<!-- /bench --> | <!-- bench:local_op_DeleteItem_ratio -->**7.9x**<!-- /bench --> |
| **Total workload** | <!-- bench:local_workload_http -->**1.3s**<!-- /bench --> | <!-- bench:local_workload_ddb -->**10.0s**<!-- /bench --> | <!-- bench:local_workload_ratio -->**7.5x**<!-- /bench --> |

### CI Pipeline Simulation (50 integration tests)

| Mode | Wall Clock | Speedup vs DDB Local |
|------|-----------|---------------------|
| Dynoxide Embedded (sequential) | <!-- bench:local_ci_embedded_seq -->~484ms<!-- /bench --> | <!-- bench:local_ci_embedded_seq_ratio -->**5.0x**<!-- /bench --> |
| Dynoxide Embedded (4x parallel) | <!-- bench:local_ci_embedded_par -->~203ms<!-- /bench --> | <!-- bench:local_ci_embedded_par_ratio -->**5.9x**<!-- /bench --> |
| Dynoxide HTTP (sequential) | <!-- bench:local_ci_http_seq -->~569ms<!-- /bench --> | <!-- bench:local_ci_http_seq_ratio -->**4.2x**<!-- /bench --> |
| Dynoxide HTTP (4x parallel) | <!-- bench:local_ci_http_par -->~235ms<!-- /bench --> | <!-- bench:local_ci_http_par_ratio -->**5.1x**<!-- /bench --> |
| DynamoDB Local (sequential) | <!-- bench:local_ci_ddb_seq -->~2,407ms<!-- /bench --> | — |
| DynamoDB Local (4x parallel) | <!-- bench:local_ci_ddb_par -->~1,189ms<!-- /bench --> | — |

### Embedded Micro-benchmarks (criterion)

| Operation | Latency |
|-----------|---------|
| GetItem | <!-- bench:local_criterion_get_item -->9µs<!-- /bench --> |
| PutItem (small / medium / large) | <!-- bench:local_criterion_put_item_small -->11µs<!-- /bench --> / <!-- bench:local_criterion_put_item_medium -->19µs<!-- /bench --> / <!-- bench:local_criterion_put_item_large -->128µs<!-- /bench --> |
| Query (base, ~50 hits) | <!-- bench:local_criterion_query_base -->723µs<!-- /bench --> |
| Query (GSI) | <!-- bench:local_criterion_query_gsi -->14µs<!-- /bench --> |
| Scan (filter, 1K items) | <!-- bench:local_criterion_scan -->6.1ms<!-- /bench --> |
| UpdateItem | <!-- bench:local_criterion_update_item -->72µs<!-- /bench --> |
| DeleteItem | <!-- bench:local_criterion_delete_item -->24µs<!-- /bench --> |
| BatchWrite (25) | <!-- bench:local_criterion_batch_write -->497µs<!-- /bench --> |
| BatchGet (100) | <!-- bench:local_criterion_batch_get -->899µs<!-- /bench --> |
| TransactWrite (4) | <!-- bench:local_criterion_transact_write -->112µs<!-- /bench --> |

## Results: CI (GitHub Actions)

Results from `ubuntu-latest` (2-core AMD EPYC 7763, 8GB RAM). Commit <!-- bench:ci_commit_link_benchmarks -->[`006fa80`](../../../commit/006fa8060c37561d79f8d455e8a752a93188ac9a)<!-- /bench -->. Absolute wall-clock numbers vary between runners; ratios are stable across runs.

### Cold Startup

| Target | Mean | Stddev | vs DynamoDB Local |
|--------|------|--------|-------------------|
| Dynoxide Embedded | <!-- bench:ci_startup_embedded_mean -->0.3ms<!-- /bench --> | <!-- bench:ci_startup_embedded_stddev -->±0.3ms<!-- /bench --> | <!-- bench:ci_startup_embedded_ratio -->**~10,766x faster**<!-- /bench --> |
| Dynoxide HTTP | <!-- bench:ci_startup_http_mean -->2.5ms<!-- /bench --> | <!-- bench:ci_startup_http_stddev -->±3.3ms<!-- /bench --> | <!-- bench:ci_startup_http_ratio -->**~1,467x faster**<!-- /bench --> |
| DynamoDB Local | <!-- bench:ci_startup_ddb_local_mean -->3,715ms<!-- /bench --> | <!-- bench:ci_startup_ddb_local_stddev -->±385ms<!-- /bench --> | — |
| LocalStack | <!-- bench:ci_startup_localstack_mean -->5,243ms<!-- /bench --> | <!-- bench:ci_startup_localstack_stddev -->±636ms<!-- /bench --> | <!-- bench:ci_startup_localstack_ratio -->3.1x slower<!-- /bench --> |

DynamoDB Local warm start (after JVM JIT): ~2ms. The cold start cost is what CI pipelines actually pay.

### Per-Operation Comparison (Dynoxide HTTP vs DynamoDB Local)

| Operation | Dynoxide HTTP (p50) | DynamoDB Local (p50) | Speedup |
|-----------|-------------------|---------------------|---------|
| CreateTable | <!-- bench:ci_op_CreateTable_http -->0.93ms<!-- /bench --> | <!-- bench:ci_op_CreateTable_ddb -->33.0ms<!-- /bench --> | <!-- bench:ci_op_CreateTable_ratio -->**36x**<!-- /bench --> |
| GetItem | <!-- bench:ci_op_GetItem_http -->0.37ms<!-- /bench --> | <!-- bench:ci_op_GetItem_ddb -->0.93ms<!-- /bench --> | <!-- bench:ci_op_GetItem_ratio -->**2.5x**<!-- /bench --> |
| PutItem | <!-- bench:ci_op_PutItem_http -->0.43ms<!-- /bench --> | <!-- bench:ci_op_PutItem_ddb -->1.2ms<!-- /bench --> | <!-- bench:ci_op_PutItem_ratio -->**2.4x**<!-- /bench --> |
| Query (base) | <!-- bench:ci_op_Query_http -->0.38ms<!-- /bench --> | <!-- bench:ci_op_Query_ddb -->1.6ms<!-- /bench --> | <!-- bench:ci_op_Query_ratio -->**4.3x**<!-- /bench --> |
| Query (GSI) | <!-- bench:ci_op_QueryGSI_http -->0.40ms<!-- /bench --> | <!-- bench:ci_op_QueryGSI_ddb -->1.2ms<!-- /bench --> | <!-- bench:ci_op_QueryGSI_ratio -->**3.0x**<!-- /bench --> |
| Query (paginated) | <!-- bench:ci_op_QueryPaginated_http -->2.9ms<!-- /bench --> | <!-- bench:ci_op_QueryPaginated_ddb -->7.9ms<!-- /bench --> | <!-- bench:ci_op_QueryPaginated_ratio -->**2.7x**<!-- /bench --> |
| Scan (full table) | <!-- bench:ci_op_Scan_http -->74.5ms<!-- /bench --> | <!-- bench:ci_op_Scan_ddb -->416.0ms<!-- /bench --> | <!-- bench:ci_op_Scan_ratio -->**5.6x**<!-- /bench --> |
| UpdateItem | <!-- bench:ci_op_UpdateItem_http -->0.47ms<!-- /bench --> | <!-- bench:ci_op_UpdateItem_ddb -->1.5ms<!-- /bench --> | <!-- bench:ci_op_UpdateItem_ratio -->**3.2x**<!-- /bench --> |
| TransactWriteItems | <!-- bench:ci_op_TransactWriteItems_http -->0.71ms<!-- /bench --> | <!-- bench:ci_op_TransactWriteItems_ddb -->5.6ms<!-- /bench --> | <!-- bench:ci_op_TransactWriteItems_ratio -->**7.8x**<!-- /bench --> |
| BatchGetItem (100 keys) | <!-- bench:ci_op_BatchGetItem_http -->3.2ms<!-- /bench --> | <!-- bench:ci_op_BatchGetItem_ddb -->14.6ms<!-- /bench --> | <!-- bench:ci_op_BatchGetItem_ratio -->**4.5x**<!-- /bench --> |
| BatchWriteItem (25 items) | <!-- bench:ci_op_BatchWriteItem_http -->2.3ms<!-- /bench --> | <!-- bench:ci_op_BatchWriteItem_ddb -->14.1ms<!-- /bench --> | <!-- bench:ci_op_BatchWriteItem_ratio -->**4.4x**<!-- /bench --> |
| DeleteItem | <!-- bench:ci_op_DeleteItem_http -->0.39ms<!-- /bench --> | <!-- bench:ci_op_DeleteItem_ddb -->1.1ms<!-- /bench --> | <!-- bench:ci_op_DeleteItem_ratio -->**2.8x**<!-- /bench --> |
| **Total workload** | <!-- bench:ci_workload_http -->**3.2s**<!-- /bench --> | <!-- bench:ci_workload_ddb -->**15.4s**<!-- /bench --> | <!-- bench:ci_workload_ratio -->**3.9x**<!-- /bench --> |

The largest speedups are on read-heavy operations (GetItem, Query, Scan, BatchGetItem) and multi-item writes (BatchWriteItem, TransactWriteItems) where Dynoxide avoids JVM dispatch overhead and lock contention. Single-row writes (PutItem, DeleteItem) still show a clear win at 2-3x.

### CI Pipeline Simulation (50 integration tests)

| Mode | Wall Clock | Speedup vs DDB Local |
|------|-----------|---------------------|
| Dynoxide Embedded (sequential) | <!-- bench:ci_suite_embedded_seq -->775ms<!-- /bench --> | <!-- bench:ci_suite_embedded_seq_ratio -->**4.1x**<!-- /bench --> |
| Dynoxide Embedded (4x parallel) | <!-- bench:ci_suite_embedded_par -->694ms<!-- /bench --> | <!-- bench:ci_suite_embedded_par_ratio -->**5.8x**<!-- /bench --> |
| Dynoxide HTTP (sequential) | <!-- bench:ci_suite_http_seq -->784ms<!-- /bench --> | <!-- bench:ci_suite_http_seq_ratio -->**4.0x**<!-- /bench --> |
| Dynoxide HTTP (4x parallel) | <!-- bench:ci_suite_http_par -->586ms<!-- /bench --> | <!-- bench:ci_suite_http_par_ratio -->**4.4x**<!-- /bench --> |
| DynamoDB Local (sequential) | <!-- bench:ci_suite_ddb_seq -->3,156ms<!-- /bench --> | — |
| DynamoDB Local (4x parallel) | <!-- bench:ci_suite_ddb_par -->2,597ms<!-- /bench --> | — |

DynamoDB Local barely benefits from parallelism (<!-- bench:ci_suite_ddb_seq_prose -->3,156ms<!-- /bench --> → <!-- bench:ci_suite_ddb_par_prose -->2,597ms<!-- /bench -->). Under concurrent load, individual tests take 3-4x longer due to JVM contention — setup times spike to 200-1,000ms on some tests as `CreateTable` calls queue behind the JVM's single-threaded SQLite access. Dynoxide embedded scales better because each test gets its own isolated `Database::memory()` with no shared state.

### Embedded Micro-benchmarks (criterion)

These measure Dynoxide's embedded API directly — no HTTP, no serialisation. This is the performance you get when using `Database::memory()` in your Rust test suite.

| Operation | Latency |
|-----------|---------|
| GetItem | <!-- bench:ci_criterion_get_item -->16µs<!-- /bench --> |
| PutItem (small / medium / large) | <!-- bench:ci_criterion_put_item_small -->22µs<!-- /bench --> / <!-- bench:ci_criterion_put_item_medium -->40µs<!-- /bench --> / <!-- bench:ci_criterion_put_item_large -->296µs<!-- /bench --> |
| Query (base, ~50 hits) | <!-- bench:ci_criterion_query_base -->1.1ms<!-- /bench --> |
| Query (GSI) | <!-- bench:ci_criterion_query_gsi -->27µs<!-- /bench --> |
| Scan (filter, 1K items) | <!-- bench:ci_criterion_scan -->8.7ms<!-- /bench --> |
| UpdateItem | <!-- bench:ci_criterion_update_item -->168µs<!-- /bench --> |
| DeleteItem | <!-- bench:ci_criterion_delete_item -->48µs<!-- /bench --> |
| BatchWrite (25) | <!-- bench:ci_criterion_batch_write -->1.0ms<!-- /bench --> |
| BatchGet (100) | <!-- bench:ci_criterion_batch_get -->1.5ms<!-- /bench --> |
| TransactWrite (4) | <!-- bench:ci_criterion_transact_write -->245µs<!-- /bench --> |

<!-- Criterion generates detailed charts for each benchmark (PDF distributions,
regression plots, violin plots). These are available in the CI artifacts under
target/criterion/*/report/. The most useful for README inclusion would be the
PutItem violin plot (shows small/medium/large distributions side by side) and
the GetItem PDF (shows the tight distribution). To include them:
1. Copy the SVGs to a docs/charts/ directory in the repo
2. Reference them here: ![PutItem violin](docs/charts/put_item_violin.svg)
-->

## Memory & Disk

| Metric | In-Memory | File-Backed | DynamoDB Local (Docker) | LocalStack (Docker) |
|--------|-----------|-------------|------------------------|---------------------|
| Idle | <!-- bench:ci_mem_memory_idle -->4.9 MB<!-- /bench --> RSS | <!-- bench:ci_mem_file_idle -->45.6 MB<!-- /bench --> RSS | <!-- bench:ci_mem_ddb_local_idle -->162.6 MB<!-- /bench --> RSS | <!-- bench:ci_mem_localstack_idle -->258.6 MB<!-- /bench --> RSS |
| After 10K items (~1KB each) | <!-- bench:ci_mem_memory_loaded -->45.8 MB<!-- /bench --> RSS | <!-- bench:ci_mem_file_loaded -->45.6 MB<!-- /bench --> RSS | — | — |
| Disk (10K items) | — | <!-- bench:ci_disk_file_loaded -->15.6 MB<!-- /bench --> | — | — |
| Disk (empty table) | — | <!-- bench:ci_disk_file_empty -->121 KB<!-- /bench --> | — | — |

File-backed mode shows higher RSS at creation because SQLite memory-maps the file. Both modes converge at 10K items. The ~46MB for 10K items is ~4.6KB per item — roughly 4x the raw item size, which accounts for SQLite structures, indexes, and the GSI.

Docker idle memory is the mean of 3 samples taken 10s apart, 30s after first successful health check, no requests served.

## Why the Numbers Differ Between Local and CI

Local development (Apple Silicon) shows ~17x speedup for embedded CI pipeline; GitHub Actions shows ~7x. This is expected:

- Apple Silicon has significantly faster single-thread performance than the 2-core EPYC VM
- Dynoxide is CPU-bound (native code, in-process SQLite), so it scales roughly linearly with CPU speed
- DynamoDB Local is JVM-overhead-bound (class loading, JIT compilation, GC), so it benefits less from faster CPUs on cold start
- The net effect: faster hardware widens the gap between native code and JVM overhead

Both are real measurements of the same benchmark suite. The CI numbers are reproducible by anyone with a GitHub account; the local numbers reflect what developers actually experience day-to-day.

## Benchmark Binaries

| Binary | Purpose |
|--------|---------|
| `ci_pipeline_bench` | Simulates 50 integration tests across all modes |
| `workload_driver` | Configurable macro-benchmark against any HTTP endpoint |
| `startup_bench` | Cold/warm start measurement for all backends |
| `memory_profiler` | RSS and disk usage tracking over workload steps |
| `size_comparison` | Binary and Docker image size comparison |

## Criterion Benchmarks

| Benchmark | What it measures |
|-----------|-----------------|
| `embedded_micro` | Individual operations (12 benchmarks): PutItem, GetItem, Query, Scan, etc. |
| `embedded_macro` | Full 13-step workload against in-memory database |
| `embedded_file_backed` | 11-step workload against file-backed SQLite database |
| `http_macro` | 11-step workload against Dynoxide HTTP server |
| `iai_core` | Iai-Callgrind instruction-count benchmarks (Linux only, requires Valgrind) |

Criterion generates HTML reports with detailed charts (PDF distributions, regression plots, violin plots for grouped benchmarks like PutItem small/medium/large). These are available in the CI artifacts under `target/criterion/*/report/`.

## Methodology

### Standard Workload (13 Steps)

1. CreateTable (pk:S HASH, sk:N RANGE, GSI on email:S)
2. BatchWriteItem — 10,000 medium items in batches of 25
3. GetItem — 1,000 reads by primary key
4. PutItem — 1,000 individual writes (mixed sizes)
5. Query (base table) — 100 queries with key conditions + filters
6. Query (GSI) — 100 queries on the email index
7. Query (paginated) — 50 queries following LastEvaluatedKey
8. Scan — full table scan with filter
9. UpdateItem — 500 updates with condition expressions
10. TransactWriteItems — 50 transactions of 4 actions each
11. BatchGetItem — 100 batches of 100 keys
12. DeleteItem — 500 deletes
13. DeleteTable

### Item Definitions

| Size | Attributes | Approximate bytes |
|------|-----------|------------------|
| Small | 3 (pk, sk, name) | ~200B |
| Medium (default) | 10 (pk, sk, name, email, age, address map, tags string set, scores list, active bool, metadata map) | ~1KB |
| Large | Medium + binary payload | ~50KB |

### JVM Warmup Protocol

DynamoDB Local runs on the JVM, which benefits from JIT compilation. For fairness:
- **Cold start benchmarks** measure real-world CI experience (no warmup)
- **Warm start benchmarks** run 500 PutItem + 100 GetItem before timing
- **Workload driver** includes a warmup phase excluded from timing
- Both cold and warm numbers are reported and clearly labelled

### Build Profile

All benchmarks run with `--release` (`opt-level = 3`, `lto = "thin"`). Debug builds are 10-100x slower and would produce misleading numbers. Criterion uses release mode by default; custom binaries must be run with `cargo run --release`.

### CI Benchmark Philosophy

Wall-clock benchmarks on shared CI runners are noisy — up to 3x variance between runs due to noisy neighbours. But **comparative** benchmarks (Dynoxide vs DynamoDB Local in the same job) are reliable because the noise affects both equally. The ratio is stable.

We publish relative claims ("6.9x faster"), never absolute CI wall-clock numbers as headline figures. Instruction-count benchmarks (Iai-Callgrind) provide deterministic regression detection independent of runner load.

### Statistical Rigour

- Criterion benchmarks use configurable sample sizes (10-100)
- Startup benchmarks report mean and standard deviation over 5 repetitions
- CI pipeline benchmarks report per-test timing breakdowns (all 50 tests)
- Workload driver collects per-request latencies for p50/p95/p99 percentiles

## Honesty & Limitations

### What Dynoxide Does Better
- **Startup time**: No JVM, no Docker — microsecond-level embedded initialisation
- **CI pipeline speed**: Zero-cost per-test isolation in embedded mode
- **Resource usage**: ~3 MB download (~6 MB on disk) vs 225 MB Docker download (471 MB on disk); 5 MB idle RSS vs 163 MB
- **Embedded mode**: Eliminates HTTP overhead entirely for Rust and iOS consumers
- **Predictable latency**: No JVM GC pauses, no JIT warmup effects

### What DynamoDB Local Does Better
- **Feature completeness**: Full DynamoDB API surface with exact behaviour matching
- **AWS-maintained**: Official tooling with ongoing updates

### Known Limitations
- Dynoxide's `Arc<Mutex<Storage>>` serialises all operations — parallel benchmarks on a single `Database` instance measure mutex contention, not true parallelism (the CI pipeline benchmark avoids this by giving each parallel test its own `Database::memory()`)
- JVM warmup means DynamoDB Local's warm-start performance is dramatically better than cold-start — both are reported
- CI wall-clock numbers vary between runners — use relative ratios, not absolute numbers
- Apple Silicon and x86_64 show different absolute numbers but consistent ratios
- PutItem and DeleteItem show near-parity with DynamoDB Local — Dynoxide's advantage is largest on read operations and batch/scan workloads

## Reproducibility

Fork the repo, install Docker, and run:

```bash
cd benchmarks
cargo bench                                                    # criterion
cargo run --release --bin ci_pipeline_bench                    # embedded + HTTP
../benchmarks/scripts/start_dynamodb_local.sh                  # start DDB Local
cargo run --release --bin ci_pipeline_bench -- --ddb-endpoint http://localhost:8000
```

Or run the full CI workflow by pushing to a fork — `.github/workflows/benchmark-comparative.yml` runs everything and stores results in the `benchmark-data` branch.

## CI Workflows

### `benchmark-regression.yml` (on every PR)
Runs criterion micro-benchmarks (wall-clock, compared against baseline from `benchmark-data` branch, blocks merge if >20% regression) and iai-callgrind instruction-count benchmarks (deterministic, <1% variance, detects algorithmic regressions independent of CI runner load).

### `benchmark-comparative.yml` (on push to main)
Runs full comparative benchmarks (Dynoxide vs DynamoDB Local), stores results in the `benchmark-data` branch for historical tracking, and uploads criterion charts as workflow artifacts.

To view historical results:
```bash
git fetch origin benchmark-data
git log benchmark-data --oneline
git show benchmark-data:runs/<dir>/run_summary.json
```

## System Requirements

- Rust stable toolchain
- Docker (for DynamoDB Local / LocalStack comparison)
- Python 3 + matplotlib (for chart generation: `pip install matplotlib`)
- Valgrind (for iai-callgrind, Linux only: `apt-get install valgrind`)
- `iai-callgrind-runner` (matching version: `cargo install iai-callgrind-runner --version 0.14.2`)
