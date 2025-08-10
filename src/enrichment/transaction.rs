use crate::enrichment::SwapEnricher;
use crate::schema::SwapEvent;
use crate::service::DextradesService;
use async_trait::async_trait;
use eyre::Result;
use serde_json;

/// Enricher that adds transaction context fields
pub struct TransactionEnricher;

#[async_trait]
impl SwapEnricher for TransactionEnricher {
    fn name(&self) -> &'static str {
        "transaction"
    }

    fn required_fields(&self) -> Vec<&'static str> {
        vec!["tx_hash"]
    }

    fn provided_fields(&self) -> Vec<&'static str> {
        vec!["tx_from", "tx_to", "gas_used"]
    }

    async fn enrich(&self, events: &mut [SwapEvent], service: &DextradesService) -> Result<()> {
        for event in events {
            // Get transaction details
            match service.get_tx_details(event.tx_hash.clone()).await {
                Ok(tx_details) => {
                    // Add to enriched fields
                    if let Some(tx_from) = tx_details.tx_from {
                        event.add_enriched_field(
                            "tx_from".to_string(),
                            serde_json::Value::String(tx_from.clone()),
                        );
                        // Also populate legacy field for backward compatibility
                        event.tx_from = Some(tx_from);
                    }

                    if let Some(tx_to) = tx_details.tx_to {
                        event.add_enriched_field(
                            "tx_to".to_string(),
                            serde_json::Value::String(tx_to.clone()),
                        );
                        // Also populate legacy field for backward compatibility
                        event.tx_to = Some(tx_to);
                    }

                    if let Some(gas_used) = tx_details.gas_used {
                        event.add_enriched_field(
                            "gas_used".to_string(),
                            serde_json::Value::Number(serde_json::Number::from(gas_used)),
                        );
                        // Also populate legacy field for backward compatibility
                        event.gas_used = Some(gas_used);
                    }
                }
                Err(e) => {
                    log::warn!(
                        "Failed to get transaction details for {}: {}",
                        event.tx_hash,
                        e
                    );
                    continue;
                }
            }
        }

        Ok(())
    }
}
