use super::SwapEnricher;
use crate::schema::SwapEvent;
use crate::service::DextradesService;
use async_trait::async_trait;
use eyre::Result;
use serde_json;

// Reuse Chainlink price service
use crate::chainlink_price_service::ChainlinkPriceService;

#[derive(Default)]
pub struct PriceUsdEnricher;

#[async_trait]
impl SwapEnricher for PriceUsdEnricher {
    fn name(&self) -> &'static str { "price_usd" }

    fn required_fields(&self) -> Vec<&'static str> {
        vec![
            // Needs token metadata and trade direction amounts
            "token0_address",
            "token1_address",
            "token0_decimals",
            "token1_decimals",
            "token_bought_address",
            "token_sold_address",
            "token_bought_amount",
            "token_sold_amount",
        ]
    }

    fn provided_fields(&self) -> Vec<&'static str> {
        vec!["value_usd", "value_usd_method", "chainlink_updated_at"]
    }

    async fn enrich(&self, events: &mut [SwapEvent], service: &DextradesService) -> Result<()> {
        if events.is_empty() { return Ok(()); }

        // Stablecoins and WETH on mainnet
        const WETH: &str = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";
        const USDC: &str = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";
        const USDT: &str = "0xdAC17F958D2ee523a2206206994597C13D831ec7";
        const DAI:  &str = "0x6B175474E89094C44Da98b954EedeAC495271d0F";

        let cfg_urls = service.config().default_rpc_urls.clone();
        let price_svc = ChainlinkPriceService::new(cfg_urls, 1024);

        // Get timestamps per unique block for staleness checks
        use std::collections::HashMap;
        let mut block_ts: HashMap<u64, i64> = HashMap::new();
        for ev in events.iter() {
            if !block_ts.contains_key(&ev.block_number) {
                if let Ok(Some(ts)) = service.get_block_timestamp(ev.block_number).await {
                    block_ts.insert(ev.block_number, ts);
                }
            }
        }

        for ev in events.iter_mut() {
            // Passthrough if a USD stable is present
            let sold_addr = ev.get_enriched_string("token_sold_address");
            let bought_addr = ev.get_enriched_string("token_bought_address");
            let sold_amt = ev.token_sold_amount;
            let bought_amt = ev.token_bought_amount;

            let is_stable = |addr: &Option<String>| -> bool {
                if let Some(a) = addr {
                    let a_low = a.to_lowercase();
                    a_low == USDC.to_lowercase() || a_low == USDT.to_lowercase() || a_low == DAI.to_lowercase()
                } else { false }
            };

            if is_stable(&sold_addr) {
                if let Some(v) = sold_amt {
                    ev.add_enriched_field("value_usd".to_string(), serde_json::json!(v));
                    ev.add_enriched_field("value_usd_method".to_string(), serde_json::json!("stable_passthrough"));
                    ev.value_usd = Some(v);
                    ev.value_usd_method = Some("stable_passthrough".to_string());
                    continue;
                }
            }
            if is_stable(&bought_addr) {
                if let Some(v) = bought_amt {
                    ev.add_enriched_field("value_usd".to_string(), serde_json::json!(v));
                    ev.add_enriched_field("value_usd_method".to_string(), serde_json::json!("stable_passthrough"));
                    ev.value_usd = Some(v);
                    ev.value_usd_method = Some("stable_passthrough".to_string());
                    continue;
                }
            }

            // WETH path via Chainlink
            let is_weth = |addr: &Option<String>| -> bool {
                if let Some(a) = addr { a.eq_ignore_ascii_case(WETH) } else { false }
            };

            if is_weth(&sold_addr) || is_weth(&bought_addr) {
                let ts = block_ts.get(&ev.block_number).copied();
                if let Ok(Some((eth_usd, updated_at))) = price_svc.eth_usd_at_block(ev.block_number, ts).await {
                    // Determine WETH amount side
                    let mut usd = None;
                    if is_weth(&sold_addr) { usd = sold_amt.map(|a| a * eth_usd); }
                    if usd.is_none() && is_weth(&bought_addr) { usd = bought_amt.map(|a| a * eth_usd); }
                    if let Some(v) = usd {
                        ev.add_enriched_field("value_usd".to_string(), serde_json::json!(v));
                        ev.add_enriched_field("value_usd_method".to_string(), serde_json::json!("weth_chainlink"));
                        ev.add_enriched_field("chainlink_updated_at".to_string(), serde_json::json!(updated_at));
                        ev.value_usd = Some(v);
                        ev.value_usd_method = Some("weth_chainlink".to_string());
                    }
                }
            }
        }

        Ok(())
    }
}
