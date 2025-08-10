use crate::schema::SwapEvent;
use crate::service::DextradesService;
use async_trait::async_trait;
use eyre::Result;

/// Trait for enriching swap events with additional data
#[async_trait]
pub trait SwapEnricher: Send + Sync {
    /// Name of this enricher (for configuration and debugging)
    fn name(&self) -> &'static str;

    /// Fields that must be present before this enricher can run
    fn required_fields(&self) -> Vec<&'static str>;

    /// Fields that this enricher will add to events
    fn provided_fields(&self) -> Vec<&'static str>;

    /// Enrich the given events with additional data
    async fn enrich(&self, events: &mut [SwapEvent], service: &DextradesService) -> Result<()>;
}

/// Pipeline for orchestrating multiple enrichers
pub struct EnrichmentPipeline {
    enrichers: Vec<Box<dyn SwapEnricher>>,
}

impl EnrichmentPipeline {
    /// Create a new empty pipeline
    pub fn new() -> Self {
        Self {
            enrichers: Vec::new(),
        }
    }

    /// Add an enricher to the pipeline
    pub fn add_enricher(mut self, enricher: Box<dyn SwapEnricher>) -> Self {
        self.enrichers.push(enricher);
        self
    }

    /// Create pipeline from a list of enricher names
    pub fn from_names(names: Vec<&str>) -> Result<Self> {
        let mut pipeline = Self::new();

        for name in names {
            let enricher = create_enricher(name)?;
            pipeline = pipeline.add_enricher(enricher);
        }

        Ok(pipeline)
    }

    /// Run all enrichers in dependency order
    pub async fn enrich_all(
        &self,
        events: &mut [SwapEvent],
        service: &DextradesService,
    ) -> Result<()> {
        // For now, run enrichers in the order they were added
        // TODO: Implement dependency resolution for optimal ordering

        for enricher in &self.enrichers {
            log::info!("🔧 [EnrichmentPipeline] Running enricher: {} for {} events", enricher.name(), events.len());

            // Check if required fields are available
            if !self.check_required_fields(events, enricher.required_fields()) {
                log::warn!(
                    "Skipping enricher {} - missing required fields",
                    enricher.name()
                );
                continue;
            }

            // Optional: expose provided fields for visibility to avoid dead_code warnings
            let _provided = enricher.provided_fields();

            enricher.enrich(events, service).await?;
        }

        Ok(())
    }

    /// Check if all required fields are present in events
    fn check_required_fields(&self, events: &[SwapEvent], required: Vec<&'static str>) -> bool {
        if events.is_empty() || required.is_empty() {
            return true;
        }

        // Check first event as a representative sample
        let event = &events[0];

        for field in required {
            match field {
                // Core fields are always available
                "block_number" | "tx_hash" | "log_index" | "dex_protocol" | "pool_address" => {
                    continue
                }
                // Check enriched fields
                _ => {
                    if !event.enriched_fields.contains_key(field) {
                        return false;
                    }
                }
            }
        }

        true
    }
}

/// Factory function to create enrichers by name
fn create_enricher(name: &str) -> Result<Box<dyn SwapEnricher>> {
    match name {
        "transaction" => Ok(Box::new(
            crate::enrichment::transaction::TransactionEnricher,
        )),
        "timestamp" => Ok(Box::new(crate::enrichment::timestamp::TimestampEnricher)),
        "token_metadata" => Ok(Box::new(
            crate::enrichment::token_metadata::TokenMetadataEnricher,
        )),
        "trade_direction" => Ok(Box::new(
            crate::enrichment::trade_direction::TradeDirectionEnricher,
        )),
        "price_usd" => Ok(Box::new(
            crate::enrichment::price_usd::PriceUsdEnricher::default(),
        )),
        _ => Err(eyre::eyre!("Unknown enricher: {}", name)),
    }
}

/// Preset enrichment configurations
pub struct EnrichmentPresets;

impl EnrichmentPresets {
    /// Minimal enrichments - just token metadata and trade direction
    pub fn minimal() -> Result<EnrichmentPipeline> {
        EnrichmentPipeline::from_names(vec![
            "transaction", // Include transaction context for backward compatibility
            "token_metadata",
            "trade_direction",
        ])
    }

    /// Standard enrichments - includes transaction context and timestamps
    pub fn standard() -> Result<EnrichmentPipeline> {
        EnrichmentPipeline::from_names(vec![
            "transaction",
            "timestamp",
            "token_metadata",
            "trade_direction",
        ])
    }

    /// Comprehensive enrichments - all available enrichers
    #[allow(dead_code)]
    pub fn comprehensive() -> Result<EnrichmentPipeline> {
        EnrichmentPipeline::from_names(vec![
            "transaction",
            "timestamp",
            "token_metadata",
            "trade_direction",
        ])
    }
}

// Re-export enricher modules
pub mod timestamp;
pub mod token_metadata;
pub mod trade_direction;
pub mod transaction;
pub mod price_usd;
