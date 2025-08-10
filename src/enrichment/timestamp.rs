use crate::enrichment::SwapEnricher;
use crate::schema::SwapEvent;
use crate::service::DextradesService;
use async_trait::async_trait;
use eyre::Result;
use serde_json;
use std::collections::{HashMap, HashSet};

/// Enricher that adds block timestamp fields
pub struct TimestampEnricher;

#[async_trait]
impl SwapEnricher for TimestampEnricher {
    fn name(&self) -> &'static str {
        "timestamp"
    }

    fn required_fields(&self) -> Vec<&'static str> {
        vec!["block_number"]
    }

    fn provided_fields(&self) -> Vec<&'static str> {
        vec!["block_timestamp"]
    }

    async fn enrich(&self, events: &mut [SwapEvent], service: &DextradesService) -> Result<()> {
        let start_time = std::time::Instant::now();
        
        // Collect unique block numbers to batch fetch timestamps
        let unique_blocks: HashSet<u64> = events.iter().map(|e| e.block_number).collect();
        
        log::info!("🕒 Timestamp enricher starting: {} events, {} unique blocks", events.len(), unique_blocks.len());

        // Parallel fetch timestamps for all unique blocks
        let timestamp_futures: Vec<_> = unique_blocks.iter()
            .map(|&block_number| async move {
                let result = service.get_block_timestamp(block_number).await;
                (block_number, result)
            })
            .collect();

        // Execute all timestamp fetches concurrently
        log::info!("🚀 Starting {} parallel timestamp fetches", unique_blocks.len());
        let parallel_start = std::time::Instant::now();
        let timestamp_results = futures::future::join_all(timestamp_futures).await;
        let parallel_duration = parallel_start.elapsed();
        log::info!("✅ Parallel timestamp fetches completed in {:?}", parallel_duration);
        
        // Process results
        let mut timestamps = HashMap::new();
        for (block_number, result) in timestamp_results {
            match result {
                Ok(Some(timestamp)) => {
                    timestamps.insert(block_number, timestamp);
                }
                Ok(None) => {
                    log::warn!("No timestamp found for block {}", block_number);
                }
                Err(e) => {
                    log::warn!("Failed to get timestamp for block {}: {}", block_number, e);
                }
            }
        }

        // Apply timestamps to events
        let event_count = events.len();
        for event in events {
            if let Some(&timestamp) = timestamps.get(&event.block_number) {
                event.add_enriched_field(
                    "block_timestamp".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(timestamp)),
                );
                // Also populate legacy field for backward compatibility
                event.block_timestamp = Some(timestamp);
            }
        }
        
        let total_duration = start_time.elapsed();
        log::info!("✅ Timestamp enrichment completed in {:?} for {} events", total_duration, event_count);

        Ok(())
    }
}
