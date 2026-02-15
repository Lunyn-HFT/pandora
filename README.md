# Pandora's Logs

Pandora is a fast tool for processing logs and counting lines. It is written in Rust. It uses special computer instructions to work quickly. It keeps memory use low even for big files.

## Performance

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