use crate::enrichment::SwapEnricher;
use crate::error::parse_address;
use crate::schema::SwapEvent;
use crate::service::DextradesService;
use alloy::primitives::{Address, U256};
use async_trait::async_trait;
use eyre::Result;
use serde_json;

#[derive(Default)]
pub struct TraderContextEnricher;

// Minimal ERC20 interface for balanceOf
alloy::sol! {
    interface IERC20 {
        function balanceOf(address) external view returns (uint256);
    }
}

fn chain_name_from_id(chain_id: u64) -> &'static str {
    match chain_id {
        1 => "ethereum",
        8453 => "base",
        _ => "unknown",
    }
}

fn weth_address_for_chain(chain_id: u64) -> Option<Address> {
    match chain_id {
        1 => parse_address("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").ok(), // Mainnet WETH
        8453 => parse_address("0x4200000000000000000000000000000000000006").ok(), // Base WETH
        _ => None,
    }
}

#[derive(Clone)]
struct RouterLabel {
    frontend: &'static str,
    router: &'static str,
    contract_name: &'static str,
    kind: &'static str,
    description: &'static str,
}

// Hardcoded router labels (subset provided by user); keyed by lowercase address
fn router_labels(chain: &str) -> &'static std::collections::HashMap<&'static str, RouterLabel> {
    use once_cell::sync::Lazy;
    static ETH_LABELS: Lazy<std::collections::HashMap<&'static str, RouterLabel>> = Lazy::new(|| {
        let mut m = std::collections::HashMap::new();
        // 1inch routers
        m.insert("0x111111125434b319222cdbf8c261674adb56f3ae", RouterLabel { frontend: "1inch Integrators", router: "1inch Router", contract_name: "AggregationRouterV2", kind: "Aggregator", description: "" });
        m.insert("0x1111111254eeb25477b68fb85ed929f73a960582", RouterLabel { frontend: "1inch Integrators", router: "1inch Router", contract_name: "AggregationRouterV5", kind: "Aggregator", description: "1inch wallet + legacy aggressive option & API" });
        m.insert("0x1111111254fb6c44bac0bed2854e76f90643097d", RouterLabel { frontend: "1inch Integrators", router: "1inch Router", contract_name: "AggregationRouterV4", kind: "Aggregator", description: "external project frontends eg oasis" });
        m.insert("0x11111112542d85b3ef69ae05771c2dccff4faa26", RouterLabel { frontend: "1inch Integrators", router: "1inch Router", contract_name: "AggregationRouterV3", kind: "Aggregator", description: "" });
        m.insert("0x111111125421ca6dc452d289314280a0f8842a65", RouterLabel { frontend: "1inch Integrators", router: "1inch Router", contract_name: "AggregationRouterV6", kind: "Aggregator", description: "" });
        // Uniswap
        m.insert("0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45", RouterLabel { frontend: "Unknown: Uniswap V3 Router 2", router: "Uniswap V3 Router 2", contract_name: "Router 02", kind: "DEX", description: "unknown external project frontends" });
        m.insert("0xe592427a0aece92de3edee1f18e0157c05861564", RouterLabel { frontend: "Unknown: Uniswap V3 Router 1", router: "Uniswap V3 Router 1", contract_name: "Router", kind: "DEX", description: "unknown external project frontends" });
        m.insert("0x7a250d5630b4cf539739df2c5dacb4c659f2488f", RouterLabel { frontend: "Unknown: Uniswap V2 Router", router: "Uniswap V2 Router", contract_name: "Router 02", kind: "DEX", description: "advanced long tail token projects" });
        m.insert("0xef1c6e67703c7bd7107eed8303fbe6ec2554bf6b", RouterLabel { frontend: "Unknown: Uniswap Old Universal Router", router: "Uniswap Old Universal Router", contract_name: "Old Universal Router", kind: "DEX", description: "swap widget used by external projects" });
        m.insert("0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad", RouterLabel { frontend: "Uniswap Website & Wallet: Default", router: "Uniswap Universal Router", contract_name: "Universal Router", kind: "DEX", description: "uniswap wallet + dapp frontend" });
        m.insert("0x66a9893cc07d91d95644aedd05d03f95e1dba8af", RouterLabel { frontend: "Uniswap Website & Wallet: Default", router: "Uniswap Universal Router", contract_name: "Uniswap V4: Universal Router", kind: "DEX", description: "uniswap wallet + dapp frontend" });
        // 0x / Matcha
        m.insert("0xdef1c0ded9bec7f1a1670819833240f027b25eff", RouterLabel { frontend: "0x API Integrators", router: "0x API Router", contract_name: "0x: Exchange Proxy", kind: "Aggregator", description: "" });
        // MetaMask
        m.insert("0x881d40237659c251811cec9c364ef91dc08d300c", RouterLabel { frontend: "MetaMask Swaps", router: "MetaMask: Swap Router", contract_name: "MetaMask Router", kind: "Meta-aggregator", description: "" });
        // Coinbase
        m.insert("0xe66b31678d6c16e9ebf358268a790b763c133750", RouterLabel { frontend: "Coinbase Wallet", router: "Coinbase Swaps Router", contract_name: "0x: Coinbase Wallet Proxy", kind: "Wallet", description: "" });
        // PancakeSwap (eth)
        m.insert("0x13f4ea83d0bd40e75c8222255bc855a974568dd4", RouterLabel { frontend: "PancakeSwap Frontend", router: "PancakeSwap Router", contract_name: "SmartRouter", kind: "DEX", description: "" });
        m
    });
    static BASE_LABELS: Lazy<std::collections::HashMap<&'static str, RouterLabel>> = Lazy::new(|| {
        let mut m = std::collections::HashMap::new();
        // Uniswap Base routers
        m.insert("0x4752ba5dbc23f44d87826276bf6fd6b1c372ad24", RouterLabel { frontend: "Unknown: Uniswap V2 Router", router: "Uniswap V2 Router", contract_name: "Uniswap: V2 Router02", kind: "DEX", description: "" });
        m.insert("0x2626664c2603336e57b271c5c0b26f421741e481", RouterLabel { frontend: "Unknown: Uniswap V3 Router", router: "Uniswap V3 Router", contract_name: "Uniswap: V3 Router02", kind: "DEX", description: "" });
        m.insert("0x6ff5693b99212da76ad316178a184ab56d299b43", RouterLabel { frontend: "Uniswap Website & Wallet: Default", router: "Uniswap Universal Router", contract_name: "Uniswap V4: Universal Router", kind: "DEX", description: "uniswap wallet + dapp frontend" });
        // Aerodrome
        m.insert("0x827922686190790b37229fd06084350e74485b72", RouterLabel { frontend: "Aerodrome Frontend", router: "Aerodrome Router", contract_name: "Aerodrome: Universal Router", kind: "DEX", description: "" });
        m.insert("0xcf77a3ba9a5ca399b7c97c74d54e5b1beb874e43", RouterLabel { frontend: "Aerodrome Frontend", router: "Aerodrome Router", contract_name: "Aerodrome: Universal Router", kind: "DEX", description: "" });
        m.insert("0xbe6d8f0d05cc4be24d5167a3ef062215be6d18a5", RouterLabel { frontend: "Aerodrome Frontend", router: "Aerodrome Router", contract_name: "Aerodrome: Universal Router", kind: "DEX", description: "" });
        m.insert("0x6cb442acf35158d5eda88fe602221b67b400be3e", RouterLabel { frontend: "Aerodrome Frontend", router: "Aerodrome Router", contract_name: "Aerodrome: Universal Router", kind: "DEX", description: "" });
        // Odos on Base
        m.insert("0x19ceead7105607cd444f5ad10dd51356436095a1", RouterLabel { frontend: "Odos Frontend", router: "Odos Router", contract_name: "Odos: Router V2", kind: "Aggregator", description: "" });
        // MetaMask
        m.insert("0x9dda6ef3d919c9bc8885d5560999a3640431e8e6", RouterLabel { frontend: "MetaMask Swaps", router: "MetaMask: Swap Router", contract_name: "MetaMask Router", kind: "Meta-aggregator", description: "" });
        m
    });
    match chain {
        "base" => &BASE_LABELS,
        _ => &ETH_LABELS,
    }
}

#[async_trait]
impl SwapEnricher for TraderContextEnricher {
    fn name(&self) -> &'static str { "trader_context" }

    fn required_fields(&self) -> Vec<&'static str> {
        vec!["tx_from", "tx_to", "block_number", "dex_protocol"]
    }

    fn provided_fields(&self) -> Vec<&'static str> {
        vec![
            "trader_nonce",
            "trader_is_contract",
            "trader_native_balance_raw",
            "trader_native_balance",
            "trader_weth_balance_raw",
            "trader_weth_balance",
            "trader_token0_balance_raw",
            "trader_token0_balance",
            "trader_token1_balance_raw",
            "trader_token1_balance",
            "router_frontend",
            "router_name",
            "router_contract_name",
            "router_type",
            "router_description",
        ]
    }

    async fn enrich(&self, events: &mut [SwapEvent], service: &DextradesService) -> Result<()> {
        if events.is_empty() { return Ok(()); }

        let chain_id = service.get_chain_id().await.unwrap_or(1);
        let chain_name = chain_name_from_id(chain_id);
        let router_map = router_labels(chain_name);
        let weth_addr_opt = weth_address_for_chain(chain_id);

        for event in events.iter_mut() {
            let block = event.block_number;

            // Router labels based on tx_to (quick, no RPC)
            if let Some(ref to) = event.tx_to {
                let addr_norm = to.to_ascii_lowercase();
                if let Some(label) = router_map.get(addr_norm.as_str()) {
                    event.add_enriched_field("router_frontend".to_string(), serde_json::Value::String(label.frontend.to_string()));
                    event.add_enriched_field("router_name".to_string(), serde_json::Value::String(label.router.to_string()));
                    event.add_enriched_field("router_contract_name".to_string(), serde_json::Value::String(label.contract_name.to_string()));
                    event.add_enriched_field("router_type".to_string(), serde_json::Value::String(label.kind.to_string()));
                    event.add_enriched_field("router_description".to_string(), serde_json::Value::String(label.description.to_string()));

                    event.router_frontend = Some(label.frontend.to_string());
                    event.router_name = Some(label.router.to_string());
                    event.router_contract_name = Some(label.contract_name.to_string());
                    event.router_type = Some(label.kind.to_string());
                    event.router_description = Some(label.description.to_string());
                }
            }

            // Trader account context
            if let Some(ref from) = event.tx_from {
                if let Ok(addr) = parse_address(from) {
                    // Nonce
                    if let Ok(nonce) = service.rpc_service().get_transaction_count_at(addr, block).await {
                        event.add_enriched_field("trader_nonce".to_string(), serde_json::Value::Number(serde_json::Number::from(nonce)));
                        event.trader_nonce = Some(nonce);
                    }
                    // Native balance (18 decimals)
                    if let Ok(balance) = service.rpc_service().get_balance_at(addr, block).await {
                        let raw = balance.to_string();
                        let dec = format_units_f64(balance, 18);
                        event.add_enriched_field("trader_native_balance_raw".to_string(), serde_json::Value::String(raw.clone()));
                        event.add_enriched_field("trader_native_balance".to_string(), serde_json::Value::Number(serde_json::Number::from_f64(dec).unwrap_or_else(|| serde_json::Number::from(0))));
                        event.trader_native_balance_raw = Some(raw);
                        event.trader_native_balance = Some(dec);
                    }
                    // EOA vs contract via code length
                    if let Ok(code) = service.rpc_service().get_code_at(addr, block).await {
                        let is_contract = !code.is_empty();
                        event.add_enriched_field("trader_is_contract".to_string(), serde_json::Value::Bool(is_contract));
                        event.trader_is_contract = Some(is_contract);
                    }

                    // WETH balance if mapping known
                    if let Some(weth_addr) = weth_addr_opt {
                        if let Ok(raw) = erc20_balance_of(service, weth_addr, addr).await {
                            let dec = format_units_f64(raw, 18);
                            event.add_enriched_field("trader_weth_balance_raw".to_string(), serde_json::Value::String(raw.to_string()));
                            event.add_enriched_field("trader_weth_balance".to_string(), serde_json::Value::Number(serde_json::Number::from_f64(dec).unwrap_or_else(|| serde_json::Number::from(0))));
                            event.trader_weth_balance_raw = Some(raw.to_string());
                            event.trader_weth_balance = Some(dec);
                        }
                    }

                    // Token0/token1 balances if addresses available on event
                    if let Some(ref t0) = event.token0_address {
                        if let (Ok(token_addr), Some(decimals)) = (parse_address(t0), event.token0_decimals) {
                            if let Ok(raw) = erc20_balance_of(service, token_addr, addr).await {
                                let dec = format_units_f64(raw, decimals);
                                event.add_enriched_field("trader_token0_balance_raw".to_string(), serde_json::Value::String(raw.to_string()));
                                event.add_enriched_field("trader_token0_balance".to_string(), serde_json::Value::Number(serde_json::Number::from_f64(dec).unwrap_or_else(|| serde_json::Number::from(0))));
                                event.trader_token0_balance_raw = Some(raw.to_string());
                                event.trader_token0_balance = Some(dec);
                            }
                        }
                    }
                    if let Some(ref t1) = event.token1_address {
                        if let (Ok(token_addr), Some(decimals)) = (parse_address(t1), event.token1_decimals) {
                            if let Ok(raw) = erc20_balance_of(service, token_addr, addr).await {
                                let dec = format_units_f64(raw, decimals);
                                event.add_enriched_field("trader_token1_balance_raw".to_string(), serde_json::Value::String(raw.to_string()));
                                event.add_enriched_field("trader_token1_balance".to_string(), serde_json::Value::Number(serde_json::Number::from_f64(dec).unwrap_or_else(|| serde_json::Number::from(0))));
                                event.trader_token1_balance_raw = Some(raw.to_string());
                                event.trader_token1_balance = Some(dec);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

async fn erc20_balance_of(service: &DextradesService, token: Address, owner: Address) -> Result<U256> {
    use alloy::sol_types::SolCall;
    let data = IERC20::balanceOfCall::new((owner,)).abi_encode();
    let bytes = service.rpc_service().call(token, data).await?;
    let bal = IERC20::balanceOfCall::abi_decode_returns(&bytes)?;
    Ok(bal)
}

fn format_units_f64(value: U256, decimals: u8) -> f64 {
    use alloy::primitives::utils::format_units;
    if let Ok(s) = format_units(value, decimals) { s.parse::<f64>().unwrap_or(0.0) } else { 0.0 }
}

