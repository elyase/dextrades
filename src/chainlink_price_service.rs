use alloy::primitives::Address;
use alloy::providers::ProviderBuilder;
use alloy::sol; 
use eyre::Result;
use log::{debug, warn};
use moka::sync::Cache;

const ONE_DAY_SECS: i64 = 24 * 60 * 60;

sol! {
    #[allow(missing_docs)]
    #[sol(rpc)]
    interface AggregatorV3Interface {
        function latestRoundData() external view returns (
            uint80 roundId,
            int256 answer,
            uint256 startedAt,
            uint256 updatedAt,
            uint80 answeredInRound
        );
        function decimals() external view returns (uint8);
    }
}

/// Minimal Chainlink-like price service for native/USD with per-block caching.
/// Uses native Alloy providers and .block(block.into()) for historical correctness.
#[derive(Clone)]
pub struct ChainlinkPriceService {
    rpc_urls: Vec<String>,
    aggregator: Address,
    // Cache block -> (price, updated_at)
    price_cache: Cache<u64, (f64, u64)>,
    // Cache decimals
    decimals_cache: Cache<String, u8>,
}

impl ChainlinkPriceService {
    pub fn new(rpc_urls: Vec<String>, aggregator: Address, cache_capacity: u64) -> Self {
        Self { rpc_urls, aggregator, price_cache: Cache::builder().max_capacity(cache_capacity).build(), decimals_cache: Cache::builder().max_capacity(4).build() }
    }

    async fn get_decimals_any(&self) -> Result<u8> {
        let cache_key = format!("decimals:{}", self.aggregator);
        if let Some(d) = self.decimals_cache.get(cache_key.as_str()) { return Ok(d); }
        let url = self
            .rpc_urls
            .get(0)
            .ok_or_else(|| eyre::eyre!("No RPC URLs configured for ChainlinkPriceService"))?
            .parse()?;
        let provider = ProviderBuilder::new().connect_http(url);
        let agg = AggregatorV3Interface::new(self.aggregator, provider);
        let d: u8 = agg.decimals().call().await?;
        self.decimals_cache.insert(cache_key, d);
        Ok(d)
    }

    /// Returns Some((price_usd, updated_at)) for ETH at the given block, or None if stale.
    pub async fn eth_usd_at_block(
        &self,
        block_number: u64,
        block_timestamp: Option<i64>,
    ) -> Result<Option<(f64, u64)>> {
        if let Some(v) = self.price_cache.get(&block_number) {
            return Ok(Some(v));
        }

        // Build a provider for this call
        let url = self
            .rpc_urls
            .get(0)
            .ok_or_else(|| eyre::eyre!("No RPC URLs configured for ChainlinkPriceService"))?
            .parse()?;
        let provider = ProviderBuilder::new().connect_http(url);
        let agg = AggregatorV3Interface::new(self.aggregator, provider);

        // Fetch decimals (cached)
        let decimals = self.get_decimals_any().await? as u32;

        // Read latestRoundData at the historical block
        let rd = agg
            .latestRoundData()
            .block(block_number.into())
            .call()
            .await?;

        let answer = rd.answer;
        let updated_at = rd.updatedAt; // U256
        let updated: u64 = updated_at.try_into().unwrap_or(0);

        // Staleness check (24h) if we have the block timestamp
        if let Some(ts) = block_timestamp {
            if (updated as i64) + ONE_DAY_SECS < ts {
                warn!("Chainlink ETH/USD stale at block {} (updated_at {} < ts {})", block_number, updated, ts);
                return Ok(None);
            }
        }

        // Convert price
        let ans_i128: i128 = answer.to_string().parse().unwrap_or(0);
        if ans_i128 <= 0 {
            return Ok(None);
        }
        let scale = 10f64.powi(decimals as i32);
        let price = (ans_i128 as f64) / scale;

        self.price_cache.insert(block_number, (price, updated));
        debug!("Aggregator {} price at block {} = {} (updated_at {})", self.aggregator, block_number, price, updated);
        Ok(Some((price, updated)))
    }
}
