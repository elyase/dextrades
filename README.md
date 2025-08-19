<h1>
<p align="center">
  <br>DexTrades 🦄 
</p >
</h1>

<p align="center">
A Python library for streaming DEX trades from RPC nodes.
</p>

<p align="center">
  <a href="https://pypi.org/project/dextrades">
    <img src="https://img.shields.io/pypi/v/dextrades.svg?label=pypi&logo=PyPI&logoColor=white" alt="PyPI">
  </a>
  <a href="https://opensource.org/licenses/MIT">
    <img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT">
  </a>
</p>

## ✨ Features

* **Direct On-Chain Data**: Pulls event logs directly from a given RPC URL, requiring no indexers or third-party APIs. It currently decodes swaps from Uniswap V2 and V3.

* **Data Enrichment Pipeline**: The library can enrich raw log data with:
    * Token metadata (symbols, decimals) for readable amounts.
    * Block timestamps for each swap.
    * USD values calculated using Chainlink ETH/USD price feeds at the swap's block height.

* **Rust Core**: Built with a Rust backend (`PyO3`, `alloy`) for processing. It implements RPC provider racing, a circuit breaker, and automatic retries for connection resilience.

* **Friendly Python API**: Provides an `async` generator to stream trades. Enrichments are controlled via boolean flags. Supports streaming individual swaps or Apache Arrow batches.

##  📦 Installation


Using `uv` (recommended):
```bash
uv add dextrades
````

Or with `pip`:

```bash
pip install dextrades
```

## 💻 Usage

The `Client` manages connections to one or more RPC endpoints. The `stream_swaps` method returns an async iterator of swap events.

```python
import asyncio
import dextrades

urls = [
    "https://eth-pokt.nodies.app",
    "https://ethereum.publicnode.com",
]
with dextrades.Client(urls) as client:
    # Stream a small block range; normalized token amounts included
    async for swap in client.stream_swaps(
        ["uniswap_v2", "uniswap_v3"],
        17000003, 17000003,
        batch_size=1,
        enrich_timestamps=True,
        enrich_usd=True,
    ):
        print(
            swap.get("dex_protocol"),
            swap.get("token_sold_symbol"), swap.get("token_sold_amount"),
            "→",
            swap.get("token_bought_symbol"), swap.get("token_bought_amount"),
            "USD:", swap.get("value_usd"),
        )
```
### Example Output

```
time                 dex           bought           sold              value_usd  trader  hash  
-----------------------------------------------------------------------------------------------------------------------------
2023-04-08 01:58:47  Uniswap V2    0.0529  WETH     98.9990  USDC        $99.00  0x5eA7  0x37f7
2023-04-08 01:58:47  Uniswap V2    0.0398  XMON      0.0529  WETH        $98.63  0x5eA7  0x37f7
2023-04-08 01:58:47  Uniswap V2    0.0452  WETH      0.7000  QNT         $84.38  0x4a30  0x5428
2023-04-08 01:58:47  Uniswap V2    3.2402  WETH      2.9994  PAXG     $6,045.62  0xdBC2  0x8f46
```



## 📊 Available Fields

| Enricher | Fields Added | Description |
|----------|--------------|-------------|
| **Core** | `block_number`, `tx_hash`, `log_index`, `dex_protocol`, `pool_address` | Always present |
| **transaction** | `tx_from`, `tx_to`, `gas_used` | Transaction context |
| **timestamp** | `block_timestamp` | Block mining time |
| **token_metadata** | `token0_address`, `token1_address`, `token0_symbol`, `token1_symbol`, `token0_decimals`, `token1_decimals` | Token information |
| **swap** | `token_bought_address`, `token_sold_address`, `token_bought_symbol`, `token_sold_symbol`, `token_bought_amount`, `token_sold_amount`, `token_bought_amount_raw`, `token_sold_amount_raw` | Trade direction & amounts |
| **price_usd** | `value_usd`, `value_usd_method`, `chainlink_updated_at` | USD valuation (stablecoins + Chainlink ETH/USD) |


### Network overrides (generic per-chain pricing)

Provide network-specific overrides at client initialization to make USD pricing and warmup work on non-mainnet chains.

```python
overrides = {
    # Wrapped native token address (e.g., WETH, wCAMP)
    "native_wrapped": "0x4200000000000000000000000000000000000006",
    # Chainlink-style native/USD aggregator (optional)
    # "native_usd_aggregator": "0x...",
    # Stablecoin addresses to treat as USD stables on this network
    "stable_addresses": [
        # "0x...", "0x..."
    ],
    # Optional warmup tokens that exist on the current network (avoid noisy warmup logs)
    "warmup_tokens": []
}

with dextrades.Client(["<rpc-url>"], network_overrides=overrides) as client:
    async for swap in client.stream_swaps(["uniswap_v2", "uniswap_v3"], 100, 100, enrich_usd=True):
        ...
```

If no per-network aggregator is provided, USD pricing falls back to:
- Stablecoin passthrough (by address or common stable symbols)
- Native/USD via Chainlink only on Ethereum mainnet (default mainnet aggregator)

### Router filter (optional)

Filter to SmartRouter transactions only:

```python
async for swap in client.stream_swaps(["uniswap_v2","uniswap_v3"], 100, 100, routers=["0xRouter..."]):
    ...

# Also available for individual streaming:
client.stream_individual_swaps(["uniswap_v2","uniswap_v3"], 100, 100, routers=["0xRouter..."])
```



## 🗺️ Roadmap

- [x] Uniswap V2
- [x] Uniswap V3
- [x]  and deduplication
- [x] Enrichments: 
  - [x] token metadata
  - [x] trade direction
  - [x] timestamps
  - [x] USD values via Chainlink
  - [x] USD values via stablecoin passthrough 
- [x] RPC provider
  - [x] racing
  - [x] retries
  - [x] circuit breakers
  - [x] sharded `getLogs`
- [x] Python API
- [x] CLI
- [x] example and demo
- [x] benchmarks
- [ ] additional enrichments:
  - [ ] trader balance
  - [ ] Uniswap V3 Quoter fallback for non-WETH/stable tokens
- [ ] Chainlink Feed Registry (USD feeds) and multi-chain aggregator addresses
- [ ] CLI UX polish (enrichment flags, simple table mode)
- [ ] Light metrics: stage counters and provider health snapshot
- [ ] Additional DEX protocols
- [ ] Optional persistent caches and Parquet/Polars export helpers
