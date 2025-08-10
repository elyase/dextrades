"""Minimal, didactic demo: stream a single block and print a fixed-width table.

Columns:
  time, dex, token_sold, amount_sold, token_bought, amount_bought, value_usd, trader (first 6), hash (first 6)
Happy path only for block 17000003.
"""

import asyncio
from datetime import datetime
import dextrades


async def main():
    urls = [
        "https://eth-pokt.nodies.app",
        "https://ethereum.publicnode.com",
    ]
    with dextrades.Client(urls) as client:
        stream = client.stream_swaps(
            ["uniswap_v2", "uniswap_v3"],
            17000003,
            17000003,
            batch_size=1,
            enrich_timestamps=True,
            enrich_usd=True,
        )

        # Fixed column widths (keep it simple and consistent)
        W = {
            "time": 19,
            "dex": 12,
            "token": 14,
            "amt_sold": 12,
            "amt_bought": 14,
            "usd": 12,
            "short": 6,
        }

        def line(cells):
            return (
                f"{cells['time']:<{W['time']}}  "
                f"{cells['dex']:<{W['dex']}}  "
                f"{cells['bought_amt']:>{W['amt_bought']}}  "
                f"{cells['bought_token']:<{W['token']}}  "
                f"{cells['sold_amt']:>{W['amt_sold']}}  "
                f"{cells['sold_token']:<{W['token']}}  "
                f"{cells['value_usd']:>{W['usd']}}  "
                f"{cells['trader']:<{W['short']}}  "
                f"{cells['hash']:<{W['short']}}"
            )

        # Header and ruler
        header = line({
            "time": "time",
            "dex": "dex",
            "bought_amt": "bought",
            "bought_token": "",
            "sold_amt": "sold",
            "sold_token": "",
            "value_usd": "value_usd",
            "trader": "trader",
            "hash": "hash",
        })
        print(header)
        print("-" * len(header))

        def fmt_amt(v: float, width: int) -> str:
            v = float(v)
            s = f"{v:.4f}"
            if len(s) > width:
                s = f"{v:.3e}"
            return s[:width]

        shown = 0
        async for swap in stream:
            # Minimal extraction and formatting
            proto = {"uniswap_v2": "Uniswap V2", "uniswap_v3": "Uniswap V3"}[swap.get("dex_protocol").lower()]
            t = datetime.fromtimestamp(swap.get("block_timestamp")).strftime("%Y-%m-%d %H:%M:%S")
            s_sym = swap.get("token_sold_symbol")
            b_sym = swap.get("token_bought_symbol")
            s_amt = fmt_amt(swap.get('token_sold_amount'), W['amt_sold'])
            b_amt = fmt_amt(swap.get('token_bought_amount'), W['amt_bought'])
            usd = f"${float(swap.get('value_usd')):,.2f}"
            trader_short = (swap.get("tx_from") or "")[:6]
            hash_short = (swap.get("tx_hash") or "")[:6]

            print(line({
                "time": t,
                "dex": proto,
                "bought_amt": b_amt,
                "bought_token": b_sym,
                "sold_amt": s_amt,
                "sold_token": s_sym,
                "value_usd": usd,
                "trader": trader_short,
                "hash": hash_short,
            }))

            shown += 1
            if shown >= 12:
                break


if __name__ == "__main__":
    asyncio.run(main())
