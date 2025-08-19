use super::SwapEnricher;
use crate::schema::SwapEvent;
use crate::service::DextradesService;
use async_trait::async_trait;
use eyre::Result;
use serde_json;

// Reuse Chainlink price service
use crate::chainlink_price_service::ChainlinkPriceService;
use crate::config::NetworkOverrides;

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

        // Stablecoins and WETH (mainnet addresses only);
        // on non-mainnet chains we fall back to symbol-based detection unless overrides are provided.
        const WETH_MAINNET: &str = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";
        const USDC_MAINNET: &str = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";
        const USDT_MAINNET: &str = "0xdAC17F958D2ee523a2206206994597C13D831ec7";
        const DAI_MAINNET:  &str = "0x6B175474E89094C44Da98b954EedeAC495271d0F";

        let cfg_urls = service.config().default_rpc_urls.clone();
        let overrides: Option<&NetworkOverrides> = service.config().network_overrides.as_ref();
        // Build optional aggregator-based price service if we have an aggregator
        let price_svc = if let Some(ov) = overrides {
            if let Some(ref agg) = ov.native_usd_aggregator {
                if let Ok(addr) = crate::error::parse_address(agg) {
                    Some(ChainlinkPriceService::new(cfg_urls.clone(), addr, 1024))
                } else { None }
            } else { None }
        } else {
            // Default to mainnet aggregator when on mainnet; otherwise none
            match service.get_chain_id().await {
                Ok(1) => {
                    if let Ok(addr) = crate::error::parse_address("0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419") {
                        Some(ChainlinkPriceService::new(cfg_urls.clone(), addr, 1024))
                    } else { None }
                },
                _ => None,
            }
        };

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
            let sold_sym = ev.get_enriched_string("token_sold_symbol");
            let bought_sym = ev.get_enriched_string("token_bought_symbol");
            let sold_amt = ev.token_sold_amount;
            let bought_amt = ev.token_bought_amount;

            // Detect common USD stables by symbol across chains
            let is_stable_symbol = |sym: &Option<String>| -> bool {
                if let Some(s) = sym {
                    let l = s.to_ascii_lowercase();
                    // Explicit list first
                    if matches!(l.as_str(),
                        "usdc" | "usdce" | "usdc.e" |
                        "usdt" | "usdt.e" |
                        "dai" |
                        "busd" |
                        "tusd" |
                        "usdd" |
                        "fdusd"
                    ) { return true; }
                    // Heuristic: symbols that contain common USD stable identifiers
                    if l.contains("usdc") || l.contains("usdt") || l.contains("usd") { return true; }
                    false
                } else { false }
            };

            // Address-based detection for Ethereum mainnet stables only, or overrides
            let stable_override_set: std::collections::HashSet<String> = overrides
                .map(|ov| ov.stable_addresses.iter().map(|s| s.to_ascii_lowercase()).collect())
                .unwrap_or_else(|| std::collections::HashSet::new());
            let is_config_stable_addr = |addr: &Option<String>| -> bool {
                if let Some(a) = addr { stable_override_set.contains(&a.to_ascii_lowercase()) } else { false }
            };
            let is_mainnet_stable_addr = |addr: &Option<String>| -> bool {
                if let Some(a) = addr {
                    let a_low = a.to_ascii_lowercase();
                    a_low == USDC_MAINNET.to_ascii_lowercase() || a_low == USDT_MAINNET.to_ascii_lowercase() || a_low == DAI_MAINNET.to_ascii_lowercase()
                } else { false }
            };

            if is_config_stable_addr(&sold_addr) || is_mainnet_stable_addr(&sold_addr) || is_stable_symbol(&sold_sym) {
                if let Some(v) = sold_amt {
                    ev.add_enriched_field("value_usd".to_string(), serde_json::json!(v));
                    let method = if is_config_stable_addr(&sold_addr) || is_mainnet_stable_addr(&sold_addr) { "stable_passthrough" } else { "stable_symbol_passthrough" };
                    ev.add_enriched_field("value_usd_method".to_string(), serde_json::json!(method));
                    ev.value_usd = Some(v);
                    ev.value_usd_method = Some(method.to_string());
                    continue;
                }
            }
            if is_config_stable_addr(&bought_addr) || is_mainnet_stable_addr(&bought_addr) || is_stable_symbol(&bought_sym) {
                if let Some(v) = bought_amt {
                    ev.add_enriched_field("value_usd".to_string(), serde_json::json!(v));
                    let method = if is_config_stable_addr(&bought_addr) || is_mainnet_stable_addr(&bought_addr) { "stable_passthrough" } else { "stable_symbol_passthrough" };
                    ev.add_enriched_field("value_usd_method".to_string(), serde_json::json!(method));
                    ev.value_usd = Some(v);
                    ev.value_usd_method = Some(method.to_string());
                    continue;
                }
            }

            // WETH path via Chainlink
            // Native wrapped detection: prefer override; fallback to mainnet WETH when on mainnet
            let native_wrapped = overrides.and_then(|ov| ov.native_wrapped.clone());
            let is_native_wrapped = |addr: &Option<String>| -> bool {
                if let Some(a) = addr {
                    if let Some(ref w) = native_wrapped { if a.eq_ignore_ascii_case(w) { return true; } }
                    a.eq_ignore_ascii_case(WETH_MAINNET)
                } else { false }
            };

            if price_svc.is_some() && (is_native_wrapped(&sold_addr) || is_native_wrapped(&bought_addr)) {
                let ts = block_ts.get(&ev.block_number).copied();
                if let Ok(Some((eth_usd, updated_at))) = price_svc.as_ref().unwrap().eth_usd_at_block(ev.block_number, ts).await {
                    // Determine WETH amount side
                    let mut usd = None;
                    if is_native_wrapped(&sold_addr) { usd = sold_amt.map(|a| a * eth_usd); }
                    if usd.is_none() && is_native_wrapped(&bought_addr) { usd = bought_amt.map(|a| a * eth_usd); }
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
