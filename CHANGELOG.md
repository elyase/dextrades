# Changelog

All notable changes to this project will be documented in this file.

## [0.1.11] - 2025-08-19

### Added
- Support for Camp Network (chain ID: 325000) testnet
- New streamlined example script `demo_camp.py` showcasing Camp Network integration
- Proper token addresses for Camp Network stablecoins (MUSDC, MUSDT, DAI)
- Native wrapped token support for wCAMP

### Changed
- Improved warmup mechanism for better connection pool management
- Simplified and more pedagogical example code with reduced lines of code
- Better error handling for warmup failures (now non-fatal)

### Fixed
- Token metadata resolution for Camp Network assets
- USD pricing for Camp Network swaps using mock stablecoins

## [0.1.10] - Previous Release

Initial release with high-performance DEX swap streaming and analysis capabilities.