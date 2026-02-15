## Description

Brief description of the changes made. (e.g., "Added AVX-512 support for SIMD scanning" or "Fixed memory leak in orchestrator")

## Type of Change

- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update
- [ ] Refactoring (no functional changes)
- [ ] Performance improvement
- [ ] Other (please describe)

## Testing

- [ ] All existing tests pass (`cargo test`)
- [ ] New tests added (if applicable)
- [ ] Manual testing performed (e.g., benchmark with `python3 tests/bench.py`)
- [ ] Tested on different modes (mmap, streaming)

## Performance Impact

- [ ] No performance impact
- [ ] Improves performance (describe: e.g., "10% faster throughput")
- [ ] May degrade performance (describe: e.g., "Slight increase in memory for large files")
- Throughput/Latency changes: [e.g., measured with bench.py]

## Checklist

- [ ] Code follows project style guidelines (`cargo fmt`, `cargo clippy`)
- [ ] Commit messages are clear and descriptive
- [ ] Documentation updated (README, code comments)
- [ ] SIMD code tested on supported CPUs (AVX2/AVX-512)
- [ ] Memory usage verified (no leaks, bounded RSS)