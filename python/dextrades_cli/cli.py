import argparse
import asyncio
import sys
from typing import List


def _parse_list(value: str) -> List[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


async def run(args):
    import dextrades

    rpc_urls = _parse_list(args.rpc)
    protocols = _parse_list(args.protocols)

    # Use config builder to set runtime configuration
    builder = dextrades.PyConfigBuilder()
    builder.rpc_urls(rpc_urls)
    builder.max_concurrent_requests(args.max_concurrent_requests)
    builder.cache_size(args.cache_size)
    builder.providers_to_race(args.providers_to_race)
    builder.shard_logs(bool(args.shard_logs))
    if args.provider_strategy:
        builder.provider_strategy(args.provider_strategy)
    client = builder.build_client()

    # Stream swaps
    async for swap in client.stream_swaps(
        protocols,
        args.from_block,
        args.to_block,
        address=args.address,
        enrich_timestamps=args.enrich_timestamps,
        max_concurrent_chunks=args.max_concurrent_chunks,
        batches=args.batches,
    ):
        # Minimal console line
        block = swap.get("block_number")
        tx = (swap.get("tx_hash") or "")[0:12]
        proto = swap.get("dex_protocol")
        sold_sym = swap.get("token_sold_symbol")
        bought_sym = swap.get("token_bought_symbol")
        sold_amt = swap.get("token_sold_amount")
        bought_amt = swap.get("token_bought_amount")
        if sold_amt is not None and bought_amt is not None and sold_sym and bought_sym:
            info = f"sold {sold_amt:.4f} {sold_sym} -> bought {bought_amt:.4f} {bought_sym}"
        elif sold_sym and bought_sym:
            info = f"{sold_sym} <-> {bought_sym}"
        else:
            info = "(raw)"
        print(f"B{block} {tx} {proto}: {info}")


def main(argv=None):
    parser = argparse.ArgumentParser("dextrades")
    parser.add_argument("--rpc", default="https://eth-pokt.nodies.app,https://ethereum.publicnode.com,https://rpc.ankr.com/eth",
                        help="Comma-separated RPC endpoints")
    parser.add_argument("--protocols", default="uniswap_v2,uniswap_v3",
                        help="Comma-separated protocols")
    parser.add_argument("--from-block", dest="from_block", type=int, required=True)
    parser.add_argument("--to-block", dest="to_block", type=int, required=True)
    parser.add_argument("--address", default=None, help="Optional pool address filter")
    parser.add_argument("--enrich-timestamps", action="store_true")
    parser.add_argument("--max-concurrent-chunks", type=int, default=4)
    parser.add_argument("--providers-to-race", type=int, default=2)
    parser.add_argument("--provider-strategy", choices=["race", "shard"], default="race",
                        help="Provider coordination strategy across RPCs")
    parser.add_argument("--shard-logs", action="store_true",
                        help="Shard getLogs across block subranges for speed")
    parser.add_argument("--batches", action="store_true",
                        help="Stream Arrow RecordBatches instead of individual swaps")
    parser.add_argument("--max-concurrent-requests", type=int, default=10)
    parser.add_argument("--cache-size", type=int, default=1000)

    args = parser.parse_args(argv)
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
