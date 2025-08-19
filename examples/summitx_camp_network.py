"""Stream DEX swaps from Camp Network and display as a formatted table."""

import asyncio
from datetime import datetime

import dextrades


def row(time, dex, bought_amt, bought_sym, sold_amt, sold_sym, usd, trader, hash):
    return (
        f"{time:<19}  {dex:<12}  {bought_amt:>14}  {bought_sym:<14}  "
        f"{sold_amt:>12}  {sold_sym:<14}  {usd:>12}  {trader:<6}  {hash:<6}"
    )


def fmt(v: float, w: int) -> str:
    return (
        f"{float(v):.4f}"[:w] if len(f"{float(v):.4f}") <= w else f"{float(v):.3e}"[:w]
    )


header = row("time", "dex", "bought", "", "sold", "", "value_usd", "trader", "hash")
print(header)
print("-" * len(header))


async def main():
    with dextrades.Client(
        ["https://rpc-campnetwork.xyz"],  # add more rpcs to speed up
        network_overrides={
            "native_wrapped": "0x1aE9c40eCd2DD6ad5858E5430A556d7aff28A44b",  # wCAMP
            "stable_addresses": [
                "0x71002dbf6cC7A885cE6563682932370c056aAca9",  # MUSDC
                "0xA745f7A59E70205e6040BdD3b33eD21DBD23FEB3",  # MUSDT
                "0x5d3011cCc6d3431D671c9e69EEddA9C5C654B97F",  # DAI
            ],
            "warmup_tokens": [],
        },
    ) as client:
        async for swap in client.stream_swaps(
            ["uniswap_v2", "uniswap_v3"],
            16365561,
            16365561,
            batch_size=1,
            enrich_timestamps=True,
            enrich_usd=True,
            routers=["0x197b7c9fC5c8AeA84Ab2909Bf94f24370539722D"],
        ):
            print(
                row(
                    datetime.fromtimestamp(swap["block_timestamp"]).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),  # timestamp
                    swap["dex_protocol"].replace("_", " ").title(),  # dex
                    fmt(swap["token_bought_amount"], 14),
                    swap["token_bought_symbol"],
                    fmt(swap["token_sold_amount"], 12),
                    swap["token_sold_symbol"],
                    (
                        f"${float(swap.get('value_usd', 0)):,.2f}"
                        if swap.get("value_usd")
                        else "-"
                    ),  # value_usd
                    swap["tx_from"][:6],  # trader
                    swap["tx_hash"][:6],  # hash
                )
            )


if __name__ == "__main__":
    asyncio.run(main())
