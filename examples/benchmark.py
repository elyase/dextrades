import dextrades
import asyncio
import time
import csv
import os
from datetime import datetime
from typing import List, Dict

# Test configuration
BENCHMARK_BLOCK_RANGE = 100  # Number of blocks to test
BASE_BLOCK = 17000000  # Starting block (high activity period)

# RPC endpoints to test
RPC_CONFIGS = {
    "single_llamarpc": ["https://eth.llamarpc.com"],
    "single_publicnode": ["https://ethereum.publicnode.com"],
    "dual_rpc": ["https://eth.llamarpc.com", "https://ethereum.publicnode.com"],
    "triple_rpc": [
        "https://eth.llamarpc.com",
        "https://ethereum.publicnode.com",
        "https://rpc.ankr.com/eth",
    ],
    "quad_rpc": [
        "https://eth.llamarpc.com",
        "https://ethereum.publicnode.com",
        "https://rpc.ankr.com/eth",
        "https://ethereum.blockpi.network/v1/rpc/public",
    ],
}

# Concurrency configurations to test
CONCURRENCY_CONFIGS = {
    "sequential": {"max_concurrent_chunks": 1, "max_concurrent_requests": 5},
    "low_concurrency": {"max_concurrent_chunks": 2, "max_concurrent_requests": 10},
    "medium_concurrency": {"max_concurrent_chunks": 4, "max_concurrent_requests": 20},
    "high_concurrency": {"max_concurrent_chunks": 8, "max_concurrent_requests": 30},
    "extreme_concurrency": {"max_concurrent_chunks": 16, "max_concurrent_requests": 50},
}

# Protocol configurations
PROTOCOL_CONFIGS = {
    "uniswap_v2_only": ["uniswap_v2"],
    "uniswap_v3_only": ["uniswap_v3"],
    "multi_protocol": ["uniswap_v2", "uniswap_v3"],
}


class BenchmarkResult:
    def __init__(
        self, 
        config_name: str, 
        duration: float, 
        swaps_count: int, 
        blocks_processed: int,
        rpc_count: int = 1,
        providers_to_race: int = 1,
        provider_strategy: str = "race",
        shard_logs: bool = False,
        protocol_count: int = 1,
        concurrency_chunks: int = 1,
        concurrency_requests: int = 10
    ):
        self.config_name = config_name
        self.duration = duration
        self.swaps_count = swaps_count
        self.blocks_processed = blocks_processed
        self.swaps_per_second = swaps_count / duration if duration > 0 else 0
        self.blocks_per_second = blocks_processed / duration if duration > 0 else 0
        
        # Additional metadata for analysis
        self.rpc_count = rpc_count
        self.providers_to_race = providers_to_race
        self.provider_strategy = provider_strategy
        self.shard_logs = shard_logs
        self.protocol_count = protocol_count
        self.concurrency_chunks = concurrency_chunks
        self.concurrency_requests = concurrency_requests
        self.timestamp = datetime.now().isoformat()

    def __str__(self):
        return (
            f"{self.config_name}: {self.duration:.2f}s, "
            f"{self.swaps_count} swaps ({self.swaps_per_second:.1f}/s), "
            f"{self.blocks_processed} blocks ({self.blocks_per_second:.1f}/s)"
        )
    
    def to_dict(self):
        """Convert result to dictionary for CSV export"""
        return {
            'timestamp': self.timestamp,
            'config_name': self.config_name,
            'duration_s': round(self.duration, 3),
            'swaps_count': self.swaps_count,
            'blocks_processed': self.blocks_processed,
            'swaps_per_second': round(self.swaps_per_second, 2),
            'blocks_per_second': round(self.blocks_per_second, 2),
            'rpc_count': self.rpc_count,
            'providers_to_race': self.providers_to_race,
            'provider_strategy': self.provider_strategy,
            'shard_logs': self.shard_logs,
            'protocol_count': self.protocol_count,
            'concurrency_chunks': self.concurrency_chunks,
            'concurrency_requests': self.concurrency_requests,
        }


def export_results_to_csv(results: List[BenchmarkResult], filename: str = None):
    """Export benchmark results to CSV file"""
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"dextrades_benchmark_{timestamp}.csv"
    
    if not results:
        print("No results to export")
        return
    
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = results[0].to_dict().keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for result in results:
            writer.writerow(result.to_dict())
    
    print(f"📊 Results exported to {filename}")


def export_results_to_markdown(results: List[BenchmarkResult], filename: str = None):
    """Export benchmark results to Markdown table"""
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"dextrades_benchmark_{timestamp}.md"
    
    if not results:
        print("No results to export")
        return
    
    with open(filename, 'w') as mdfile:
        mdfile.write("# Dextrades Benchmark Results\n\n")
        mdfile.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Summary table
        mdfile.write("## Performance Summary\n\n")
        mdfile.write("| Configuration | Duration (s) | Swaps/s | Blocks/s | RPCs | Strategy | Sharded |\n")
        mdfile.write("|---------------|--------------|---------|----------|------|----------|----------|\n")
        
        for result in results:
            mdfile.write(f"| {result.config_name} | {result.duration:.2f} | {result.swaps_per_second:.1f} | {result.blocks_per_second:.1f} | {result.rpc_count} | {result.provider_strategy} | {'Yes' if result.shard_logs else 'No'} |\n")
        
        # Detailed results
        mdfile.write("\n## Detailed Results\n\n")
        for result in results:
            mdfile.write(f"### {result.config_name}\n\n")
            mdfile.write(f"- **Duration**: {result.duration:.3f} seconds\n")
            mdfile.write(f"- **Total Swaps**: {result.swaps_count}\n")
            mdfile.write(f"- **Blocks Processed**: {result.blocks_processed}\n")
            mdfile.write(f"- **Swaps/Second**: {result.swaps_per_second:.2f}\n")
            mdfile.write(f"- **Blocks/Second**: {result.blocks_per_second:.2f}\n")
            mdfile.write(f"- **RPC Count**: {result.rpc_count}\n")
            mdfile.write(f"- **Providers to Race**: {result.providers_to_race}\n")
            mdfile.write(f"- **Provider Strategy**: {result.provider_strategy}\n")
            mdfile.write(f"- **Sharded Logs**: {'Yes' if result.shard_logs else 'No'}\n")
            mdfile.write(f"- **Protocols**: {result.protocol_count}\n")
            mdfile.write(f"- **Concurrency Chunks**: {result.concurrency_chunks}\n")
            mdfile.write(f"- **Concurrency Requests**: {result.concurrency_requests}\n")
            mdfile.write("\n")
    
    print(f"📝 Results exported to {filename}")


async def run_benchmark(
    rpc_urls: List[str],
    protocols: List[str],
    concurrency_config: Dict[str, int],
    block_range: int = BENCHMARK_BLOCK_RANGE,
    batch_size: int = 10,
    providers_to_race: int | None = None,
    provider_strategy: str = "race",
    shard_logs: bool = False,
) -> BenchmarkResult:
    """Run a single benchmark configuration"""

    ptr = providers_to_race or min(2, len(rpc_urls))
    client = dextrades.Client(
        rpc_urls,
        max_concurrent_requests=concurrency_config["max_concurrent_requests"],
        cache_size=2000,  # Larger cache for benchmarks
        providers_to_race=ptr,
        shard_logs=shard_logs,
        provider_strategy=provider_strategy,
    )

    from_block = BASE_BLOCK
    to_block = from_block + block_range - 1

    print(
        f"  Testing: {len(rpc_urls)} RPCs, {len(protocols)} protocols, "
        f"chunks={concurrency_config['max_concurrent_chunks']}, "
        f"requests={concurrency_config['max_concurrent_requests']}"
    )
    print(f"  Block range: {from_block} to {to_block} ({block_range} blocks)")

    start_time = time.time()
    total_swaps = 0

    try:
        stream = client.stream_swaps(
            protocols,
            from_block,
            to_block,
            address=None,  # All pools
            batch_size=batch_size,
            enrich_timestamps=False,  # Skip timestamps for speed
            max_concurrent_chunks=concurrency_config["max_concurrent_chunks"],
            batches=True  # Enable batch mode for performance
        )

        async for batch in stream:
            if batch.num_rows > 0:
                total_swaps += batch.num_rows

    except Exception as e:
        print(f"  ❌ Error: {e}")
        return BenchmarkResult("ERROR", 0, 0, 0)

    duration = time.time() - start_time
    
    config_name = f"{len(rpc_urls)}RPCs_{len(protocols)}protos_c{concurrency_config['max_concurrent_chunks']}"
    if shard_logs:
        config_name += f"_{provider_strategy}_{ptr}race"
    
    return BenchmarkResult(
        config_name,
        duration,
        total_swaps,
        block_range,
        rpc_count=len(rpc_urls),
        providers_to_race=ptr,
        provider_strategy=provider_strategy,
        shard_logs=shard_logs,
        protocol_count=len(protocols),
        concurrency_chunks=concurrency_config['max_concurrent_chunks'],
        concurrency_requests=concurrency_config['max_concurrent_requests']
    )


async def run_rpc_scaling_benchmark():
    """Test how performance scales with number of RPC endpoints"""
    print("🚀 RPC Scaling Benchmark")
    print("=" * 60)

    results = []
    protocols = PROTOCOL_CONFIGS["multi_protocol"]
    concurrency = CONCURRENCY_CONFIGS["medium_concurrency"]

    for rpc_name, rpc_urls in RPC_CONFIGS.items():
        print(f"\n📡 Testing {rpc_name} ({len(rpc_urls)} RPCs)")
        # Sweep providers_to_race and provider strategies
        for ptr in range(1, min(4, len(rpc_urls)) + 1):
            # Test race strategy
            result_race = await run_benchmark(
                rpc_urls,
                protocols,
                concurrency,
                block_range=50,  # Smaller range for RPC scaling test
                providers_to_race=ptr,
                provider_strategy="race",
                shard_logs=True,
            )
            results.append(result_race)
            print(f"  ✅ race={ptr}, strategy=race: {result_race}")
            
            # Test shard strategy (only if we have multiple RPCs)
            if len(rpc_urls) > 1 and ptr > 1:
                result_shard = await run_benchmark(
                    rpc_urls,
                    protocols,
                    concurrency,
                    block_range=50,
                    providers_to_race=ptr,
                    provider_strategy="shard",
                    shard_logs=True,
                )
                results.append(result_shard)
                print(f"  ✅ race={ptr}, strategy=shard: {result_shard}")

    print("\n📊 RPC Scaling Results:")
    print(
        f"{'Config':<20} {'Duration':<10} {'Swaps':<8} {'Swaps/s':<10} {'Blocks/s':<10}"
    )
    print("-" * 60)
    for result in results:
        print(
            f"{result.config_name:<20} {result.duration:<10.2f} {result.swaps_count:<8} "
            f"{result.swaps_per_second:<10.1f} {result.blocks_per_second:<10.1f}"
        )


async def run_concurrency_benchmark():
    """Test how performance scales with concurrency levels"""
    print("\n\n⚡ Concurrency Scaling Benchmark")
    print("=" * 60)

    results = []
    rpc_urls = RPC_CONFIGS["dual_rpc"]
    protocols = PROTOCOL_CONFIGS["multi_protocol"]

    for concurrency_name, concurrency_config in CONCURRENCY_CONFIGS.items():
        print(f"\n🔄 Testing {concurrency_name}")

        result = await run_benchmark(
            rpc_urls,
            protocols,
            concurrency_config,
            block_range=30,  # Smaller range for concurrency test
        )
        results.append(result)
        print(f"  ✅ {result}")

    print("\n📊 Concurrency Scaling Results:")
    print(
        f"{'Config':<20} {'Duration':<10} {'Swaps':<8} {'Swaps/s':<10} {'Speedup':<10}"
    )
    print("-" * 60)

    baseline_speed = results[0].swaps_per_second if results else 1
    for result in results:
        speedup = result.swaps_per_second / baseline_speed if baseline_speed > 0 else 0
        print(
            f"{result.config_name.split('_c')[0]:<20} {result.duration:<10.2f} {result.swaps_count:<8} "
            f"{result.swaps_per_second:<10.1f} {speedup:<10.1f}x"
        )


async def run_protocol_benchmark():
    """Test performance differences between protocols"""
    print("\n\n🔬 Protocol Comparison Benchmark")
    print("=" * 60)

    results = []
    rpc_urls = RPC_CONFIGS["dual_rpc"]
    concurrency = CONCURRENCY_CONFIGS["medium_concurrency"]

    for protocol_name, protocols in PROTOCOL_CONFIGS.items():
        print(f"\n🧪 Testing {protocol_name}")

        result = await run_benchmark(rpc_urls, protocols, concurrency, block_range=30)
        results.append(result)
        print(f"  ✅ {result}")

    print("\n📊 Protocol Comparison Results:")
    print(
        f"{'Protocol':<20} {'Duration':<10} {'Swaps':<8} {'Swaps/s':<10} {'Efficiency':<12}"
    )
    print("-" * 70)
    for result in results:
        efficiency = result.swaps_count / result.duration if result.duration > 0 else 0
        print(
            f"{result.config_name.split('_c')[0]:<20} {result.duration:<10.2f} {result.swaps_count:<8} "
            f"{result.swaps_per_second:<10.1f} {efficiency:<12.1f}"
        )


async def run_batch_size_benchmark():
    """Test optimal batch size for different configurations"""
    print("\n\n📦 Batch Size Optimization Benchmark")
    print("=" * 60)

    batch_sizes = [5, 10, 20, 50, 100]
    results = []
    rpc_urls = RPC_CONFIGS["dual_rpc"]
    protocols = PROTOCOL_CONFIGS["multi_protocol"]
    concurrency = CONCURRENCY_CONFIGS["medium_concurrency"]

    for batch_size in batch_sizes:
        print(f"\n📏 Testing batch_size={batch_size}")

        result = await run_benchmark(
            rpc_urls, protocols, concurrency, block_range=50, batch_size=batch_size
        )
        results.append((batch_size, result))
        print(f"  ✅ {result}")

    print("\n📊 Batch Size Optimization Results:")
    print(
        f"{'Batch Size':<12} {'Duration':<10} {'Swaps':<8} {'Swaps/s':<10} {'Optimal':<8}"
    )
    print("-" * 60)

    best_throughput = max(result.swaps_per_second for _, result in results)
    for batch_size, result in results:
        is_optimal = "⭐" if result.swaps_per_second == best_throughput else ""
        print(
            f"{batch_size:<12} {result.duration:<10.2f} {result.swaps_count:<8} "
            f"{result.swaps_per_second:<10.1f} {is_optimal:<8}"
        )


async def run_comprehensive_benchmark():
    """Run the ultimate performance test with best configuration"""
    print("\n\n🏆 Comprehensive Performance Benchmark")
    print("=" * 60)

    # Use best configuration
    rpc_urls = RPC_CONFIGS["quad_rpc"]
    protocols = PROTOCOL_CONFIGS["multi_protocol"]
    concurrency = CONCURRENCY_CONFIGS["high_concurrency"]

    print("\n🎯 Ultimate Configuration Test:")
    print(f"   RPCs: {len(rpc_urls)} endpoints")
    print(f"   Protocols: {protocols}")
    print(f"   Concurrency: {concurrency}")
    print(f"   Block range: {BENCHMARK_BLOCK_RANGE} blocks")

    # Test both strategies for comparison
    result_race = await run_benchmark(
        rpc_urls,
        protocols,
        concurrency,
        block_range=BENCHMARK_BLOCK_RANGE,
        batch_size=20,  # Optimal batch size
        providers_to_race=min(4, len(rpc_urls)),
        provider_strategy="race",
        shard_logs=True,
    )

    result_shard = await run_benchmark(
        rpc_urls,
        protocols,
        concurrency,
        block_range=BENCHMARK_BLOCK_RANGE,
        batch_size=20,  # Optimal batch size
        providers_to_race=min(4, len(rpc_urls)),
        provider_strategy="shard",
        shard_logs=True,
    )

    print("\n🏆 ULTIMATE PERFORMANCE COMPARISON:")
    print(f"   Race Strategy: {result_race.swaps_per_second:.1f} swaps/s, {result_race.duration:.2f}s")
    print(f"   Shard Strategy: {result_shard.swaps_per_second:.1f} swaps/s, {result_shard.duration:.2f}s")
    
    best_result = result_shard if result_shard.swaps_per_second > result_race.swaps_per_second else result_race
    print(f"   🎯 Winner: {best_result.provider_strategy.upper()} strategy ({best_result.swaps_per_second:.1f} swaps/s)")
    
    return [result_race, result_shard]


async def run_provider_strategy_comparison():
    """Compare provider strategies across different configurations"""
    print("\n\n⚔️ Provider Strategy Comparison")
    print("=" * 60)
    
    all_results = []
    protocols = PROTOCOL_CONFIGS["multi_protocol"]
    concurrency = CONCURRENCY_CONFIGS["medium_concurrency"]
    
    for rpc_name, rpc_urls in RPC_CONFIGS.items():
        if len(rpc_urls) < 2:  # Skip single RPC configs
            continue
            
        print(f"\n📊 Testing {rpc_name} ({len(rpc_urls)} RPCs)")
        
        for providers_to_race in [2, min(4, len(rpc_urls))]:
            # Test race strategy
            result_race = await run_benchmark(
                rpc_urls,
                protocols,
                concurrency,
                block_range=30,
                providers_to_race=providers_to_race,
                provider_strategy="race",
                shard_logs=True,
            )
            all_results.append(result_race)
            
            # Test shard strategy
            result_shard = await run_benchmark(
                rpc_urls,
                protocols,
                concurrency,
                block_range=30,
                providers_to_race=providers_to_race,
                provider_strategy="shard",
                shard_logs=True,
            )
            all_results.append(result_shard)
            
            speedup = result_shard.swaps_per_second / result_race.swaps_per_second if result_race.swaps_per_second > 0 else 1
            print(f"  🏁 Providers={providers_to_race}: Race={result_race.swaps_per_second:.1f}/s, Shard={result_shard.swaps_per_second:.1f}/s (Speedup: {speedup:.2f}x)")
    
    return all_results


async def main():
    """Run all benchmarks"""
    print("🔥 DEXTRADES PERFORMANCE BENCHMARK SUITE")
    print("=" * 80)
    print(f"Block range: {BASE_BLOCK} to {BASE_BLOCK + BENCHMARK_BLOCK_RANGE}")
    print(f"Total blocks to process: {BENCHMARK_BLOCK_RANGE}")
    print()

    try:
        all_results = []
        
        # Run all benchmark suites
        await run_rpc_scaling_benchmark()
        await run_concurrency_benchmark()
        await run_protocol_benchmark()
        await run_batch_size_benchmark()
        
        # New comprehensive benchmarks with result collection
        comprehensive_results = await run_comprehensive_benchmark()
        all_results.extend(comprehensive_results)
        
        strategy_results = await run_provider_strategy_comparison()
        all_results.extend(strategy_results)

        # Export results to files
        if all_results:
            print("\n📊 Exporting benchmark results...")
            export_results_to_csv(all_results)
            export_results_to_markdown(all_results)

        print("\n\n🎉 BENCHMARK COMPLETE!")
        print("=" * 60)
        print("📈 Key Insights:")
        print("   • More RPC endpoints = better throughput")
        print("   • Provider-assigned shards can provide better scaling than racing")
        print("   • Optimal concurrency depends on network conditions")
        print("   • V2+V3 multi-protocol provides most comprehensive data")
        print("   • Batch size 20-50 typically optimal")
        print("   • 4+ RPCs with sharding strategy = maximum performance")

    except KeyboardInterrupt:
        print("\n\n⏹️  Benchmark interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Benchmark failed: {e}")


if __name__ == "__main__":
    asyncio.run(main())
