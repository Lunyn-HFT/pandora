# Pandora's Logs

Pandora is a fast tool for processing logs and counting lines. It is written in Rust. It uses special computer instructions to work quickly. It keeps memory use low even for big files.

## Performance Benchmarks

Pandora works faster than tools like GNU grep for scanning large logs. It uses chunked processing to keep memory use low even for big files.

Here are test results from a computer with AMD Ryzen 5 7535HS:

| Dataset | Method | Throughput (Median) | Latency (Median Wall Time) | Memory (Peak RSS) |
| --- | --- | --- | --- | --- |
| 2GB | Pandora | 8.118 GiB/s | 263.900 ms | 19.62 MB |
| 2GB | GNU Grep | 3.142 GiB/s | 681.884 ms | 19.62 MB |
| 5GB | Pandora | 2.051 GiB/s | 2611.591 ms | 19.72 MB |
| 5GB | GNU Grep | 1.692 GiB/s | 3164.787 ms | 19.72 MB |

For 2GB dataset: Speedup 2.584x  
For 5GB dataset: Speedup 1.212x

Pandora also excels at parsing structured log formats. Here are benchmarks against popular tools on 1GB datasets:

### JSON Parsing

| Dataset | Tool | Task | Throughput | Time | Records/Fields | Speedup vs jq |
| --- | --- | --- | --- | --- | --- | --- |
| 1GB JSON | Pandora | Full field extraction | 0.54 GB/s | 2.47s | 7.16M records, 57.3M fields | 12.4x |
| 1GB JSON | jq | Parse & count objects | 0.034 GB/s | 30.67s | 7.16M objects | 1x |

### CSV Processing

| Dataset | Tool | Task | Throughput | Time | Notes |
| --- | --- | --- | --- | --- | --- |
| 1GB CSV | Pandora | Full field extraction | 0.71 GB/s | 1.40s | Comprehensive parsing |
| 1GB CSV | xsv | Row count | 1.21 GB/s | 0.83s | Simple counting |
| 1GB CSV | xsv | Full analysis | 0.26 GB/s | 3.89s | Statistics computation |

### SIMD Line Scanning

| Dataset | Tool | Throughput | Time | Speedup vs ripgrep |
| --- | --- | --- | --- | --- |
| 1GB logfmt | Pandora | 12.2 GB/s | 0.083s | 7.3x |
| 1GB logfmt | ripgrep | 1.67 GB/s | 0.614s | 1x |

**Benchmark Methodology:** All benchmarks were conducted on 1GB datasets (7.16M records) generated with Pandora's test data generator. JSON parsing used jq with basic parsing (`jq -c .`). CSV processing used xsv for row counting (`xsv count`) and statistical analysis (`xsv stats`). Line counting used ripgrep with pattern matching (`rg -c '^'`). All tests ran on AMD Ryzen 5 7535HS with peak memory usage under 100MB.

## Key Features

* Uses fast computer tricks to check 32 bytes at a time without using extra memory.
* Uses chunked processing to keep memory use low, around 20 MB.
* Works directly on data in memory without copying it.
* Uses system hints to read data quickly.

## Usage

### Building

```bash
cargo build --release --bins
```

### Line Scanning

To count lines in a file:

```bash
./target/release/scan-newlines <file_path> <threads>
```

### Testing Speed

Use the Python script to compare speed:

```bash
python3 tests/bench.py --dataset <file_path> --runs 15
```

## How It Works

Pandora has three main parts:

1. **Scanner** is the fast part that finds line breaks using special instructions.
2. **Orchestrator** handles reading files and managing memory to keep it low.
3. **Parser** turns log lines into useful data without copying.

## License

This project uses the GNU General Public License, v2.0. See the LICENSE file for details.