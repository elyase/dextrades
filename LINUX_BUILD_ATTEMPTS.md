# Linux Build Troubleshooting Report

## Summary
This document chronicles the attempts to resolve Linux wheel building issues with the dextrades Python package that uses Rust (alloy-rs) with PyO3/maturin. The project successfully built Linux wheels initially but encountered persistent Docker compilation failures with the blst cryptographic library during later attempts.

## Working Baseline
- **Last Working Commit**: `6ba3117b0ab79570ab7a581487a4ac5aef95c992`
- **Status**: Successfully built and published Linux + Windows + macOS wheels to PyPI
- **Configuration**: Used `manylinux: "off"` with native Ubuntu runner compilation

## Root Cause
The core issue was **blst cryptographic library compilation failures** in Docker-based manylinux environments. The `c-kzg` crate (dependency of alloy-rs) transitively depends on `blst`, which has known compilation issues in Docker containers used by manylinux wheel builds.

## Attempted Solutions & Failures

### 1. Different manylinux Versions
**Attempts**: 
- `manylinux: "2_28"` (newer version for better crypto support)
- `manylinux: "2014"` (minimum for Rust 1.64+)
- `manylinux: "auto"` (let maturin decide)

**Results**: All failed with same Docker compilation errors
**Why it failed**: blst library has fundamental issues compiling in Docker environments regardless of manylinux version

### 2. Zig Cross-Compilation
**Attempts**:
- Manual zig installation from ziglang.org
- Using `--zig` flag with maturin
- pip install ziglang approach

**Results**: 
- Manual installation failed (wget/tar issues)
- zig flag caused maturin errors
**Why it failed**: Zig integration with maturin is still experimental and has compatibility issues with complex crypto libraries

### 3. Dependency Feature Configuration
**Attempts**:
- Added direct `c-kzg` dependency with `no-threads` and `portable` features
- Used `[patch.crates-io]` section to override c-kzg features

**Results**: Build succeeded locally but still failed in CI Docker environments
**Why it failed**: Feature flags don't resolve the fundamental Docker compilation environment issues

### 4. Following Successful Projects
**Research**: Analyzed paradigmxyz/cryo project which successfully uses alloy-rs
**Attempts**:
- Copied their exact maturin-action configuration
- Used their `sccache: 'true'` parameter approach
- Tried downgrading alloy version to match theirs (reverted immediately)

**Results**: Their configuration also failed in our environment
**Why it failed**: Different project structure, dependencies, or they may use different alloy features that avoid the problematic code paths

### 5. Alternative Build Systems
**Attempts**:
- Considered cibuildwheel instead of maturin-action
- Explored native GitHub Actions compilation

**Results**: Not fully implemented due to complexity
**Why it failed**: Would require significant workflow restructuring without guaranteeing success

### 6. Disabling Linux Builds (Rejected)
**Attempt**: Temporarily disabled Linux builds to ship macOS/Windows only
**Result**: User correctly rejected this as "cheating" rather than fixing the real issue
**Why it failed**: Not a real solution, abandons Linux users

## Technical Details

### Error Pattern
All Docker-based builds failed with variations of:
```
The process '/usr/bin/docker' failed with exit code 1
```

Specifically related to blst library compilation in the manylinux Docker container environment.

### Dependency Chain
```
alloy-rs → c-kzg → blst
```
The blst library (BLS signature cryptography) has known issues with Docker-based compilation.

### Working Configuration (Baseline)
```yaml
- name: Build with maturin (native Linux, no Docker)
  uses: PyO3/maturin-action@v1
  with:
    rust-toolchain: stable
    target: ${{ matrix.target }}
    manylinux: "off"  # Disable manylinux Docker to build natively
    command: build
    args: --release --out dist --find-interpreter
```

## Lessons Learned

1. **Native compilation works**: The `manylinux: "off"` approach successfully builds wheels
2. **Docker environments problematic**: All Docker-based manylinux approaches fail with cryptographic libraries
3. **Feature flags insufficient**: Dependency feature configuration doesn't resolve Docker compilation issues
4. **Project-specific solutions**: What works for other projects may not work due to different dependency configurations

## Recommendation

**Revert to working commit `6ba3117b0ab79570ab7a581487a4ac5aef95c992`** and maintain the `manylinux: "off"` approach. While this creates wheels with `linux_x86_64` tags (not manylinux-compliant), they work correctly and PyPI accepts them.

For broader Linux compatibility, users can install from source:
```bash
pip install dextrades --no-binary=dextrades
```

## Next Steps (If Pursued)

1. **Upstream fixes**: Monitor alloy-rs, c-kzg, and blst for Docker compilation improvements
2. **Alternative crypto libraries**: Investigate if alloy-rs can use different cryptographic backends
3. **Container alternatives**: Explore non-Docker based cross-compilation approaches
4. **Version pinning**: Lock to specific versions of problematic dependencies that are known to work

The native compilation approach remains the most reliable solution for this specific dependency chain.