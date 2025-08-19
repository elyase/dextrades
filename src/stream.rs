/// Create and manage streaming tasks for real-time swap event processing
///
/// This module provides functionality for:
/// - Real-time streaming of swap events from multiple DEX protocols
/// - Configurable batch processing with automatic enrichment
/// - Error handling and recovery for streaming operations
/// - Python integration for data analysis workflows
///
/// # Architecture
///
/// The streaming system uses:
/// - Tokio tasks for concurrent block processing
/// - MPSC channels for efficient data passing
/// - Protocol registry for multi-protocol support
/// - Enrichment pipelines for data enhancement
///
/// # Example
///
/// ```python
/// import dextrades
///
/// config = dextrades.DextradesConfig()
/// service = dextrades.DextradesService(config)
///
/// # Stream events from multiple protocols
/// async for batch in service.stream_swaps_live(
///     protocols=["uniswap_v2", "uniswap_v3"],
///     enrichments=["token_metadata", "trade_direction"]
/// ):
///     print(f"Received {len(batch)} swap events")
/// ```
// src/stream.rs
use arrow::pyarrow::ToPyArrow;
use arrow::record_batch::RecordBatch;
use eyre::Report;
use futures::stream::{FuturesUnordered, StreamExt};
use log::{debug, info, warn};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

use crate::config::DextradesConfig;
use crate::error::parse_address;
use crate::event_pipeline::{EventPipeline, PipelineConfig};
use crate::schema::SwapEvent;
use crate::service::DextradesService;
use std::collections::VecDeque;

/// Lightweight typed wrapper over a Python dict representing a swap
#[pyclass]
pub struct PySwap {
    inner: PyObject,
}

    #[pymethods]
    impl PySwap {
    #[new]
    fn new(obj: PyObject) -> Self { Self { inner: obj } }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let d = self.inner.bind(py);
        Ok(format!("PySwap({})", d.repr()?.to_string()))
    }

    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<PyObject> {
        let d = self.inner.bind(py);
        let v = d.get_item(key)?;
        Ok(v.unbind().into())
    }

    fn __getattr__(&self, py: Python<'_>, name: &str) -> PyResult<PyObject> {
        let d = self.inner.bind(py);
        let v = d.get_item(name)?;
        Ok(v.unbind().into())
    }

    #[pyo3(signature=(key, default=None))]
    fn get(&self, py: Python<'_>, key: &str, default: Option<PyObject>) -> PyResult<PyObject> {
        let d = self.inner.bind(py);
        match d.get_item(key) {
            Ok(v) => Ok(v.unbind().into()),
            Err(_) => Ok(default.unwrap_or_else(|| py.None())),
        }
    }

    fn keys(&self, py: Python<'_>) -> PyResult<PyObject> {
        let d = self.inner.bind(py);
        let keys = d.call_method0("keys")?;
        Ok(keys.unbind().into())
    }

    fn as_dict(&self, py: Python<'_>) -> PyResult<PyObject> { Ok(self.inner.clone_ref(py)) }

    // Typed getters for common fields
    fn block_number(&self, py: Python<'_>) -> PyResult<u64> {
        let d = self.inner.bind(py);
        Ok(d.get_item("block_number")?.extract()?)
    }

    fn tx_hash(&self, py: Python<'_>) -> PyResult<String> {
        let d = self.inner.bind(py);
        Ok(d.get_item("tx_hash")?.extract()?)
    }

    fn log_index(&self, py: Python<'_>) -> PyResult<u64> {
        let d = self.inner.bind(py);
        Ok(d.get_item("log_index")?.extract()?)
    }

    fn dex_protocol(&self, py: Python<'_>) -> PyResult<String> {
        let d = self.inner.bind(py);
        Ok(d.get_item("dex_protocol")?.extract()?)
    }

    fn pool_address(&self, py: Python<'_>) -> PyResult<String> {
        let d = self.inner.bind(py);
        Ok(d.get_item("pool_address")?.extract()?)
    }

    fn block_timestamp(&self, py: Python<'_>) -> PyResult<Option<i64>> {
        let d = self.inner.bind(py);
        Ok(d.get_item("block_timestamp")?.extract().unwrap_or(None))
    }

    fn tx_from(&self, py: Python<'_>) -> PyResult<Option<String>> {
        let d = self.inner.bind(py);
        Ok(d.get_item("tx_from")?.extract().unwrap_or(None))
    }

    fn tx_to(&self, py: Python<'_>) -> PyResult<Option<String>> {
        let d = self.inner.bind(py);
        Ok(d.get_item("tx_to")?.extract().unwrap_or(None))
    }

    fn token_sold_symbol(&self, py: Python<'_>) -> PyResult<Option<String>> {
        let d = self.inner.bind(py);
        Ok(d.get_item("token_sold_symbol")?.extract().unwrap_or(None))
    }

    fn token_bought_symbol(&self, py: Python<'_>) -> PyResult<Option<String>> {
        let d = self.inner.bind(py);
        Ok(d.get_item("token_bought_symbol")?.extract().unwrap_or(None))
    }

    fn token_sold_amount(&self, py: Python<'_>) -> PyResult<Option<f64>> {
        let d = self.inner.bind(py);
        Ok(d.get_item("token_sold_amount")?.extract().unwrap_or(None))
    }

    fn token_bought_amount(&self, py: Python<'_>) -> PyResult<Option<f64>> {
        let d = self.inner.bind(py);
        Ok(d.get_item("token_bought_amount")?.extract().unwrap_or(None))
    }
}

/// A Python-exposed asynchronous stream of Arrow RecordBatches.
#[pyclass]
pub struct DextradesArrowStream {
    inner: Arc<Mutex<mpsc::Receiver<Result<RecordBatch, Report>>>>,
    pipeline: Option<Arc<EventPipeline>>,
    service: Option<Arc<DextradesService>>,
}

impl DextradesArrowStream {
    /// Create a new stream from a Tokio MPSC receiver.
    pub fn new(
        receiver: mpsc::Receiver<Result<RecordBatch, Report>>,
        pipeline: Option<Arc<EventPipeline>>,
        service: Option<Arc<DextradesService>>,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(receiver)),
            pipeline,
            service,
        }
    }
}

#[pymethods]
impl DextradesArrowStream {
    /// Asynchronously receive the next RecordBatch. Returns None when stream is exhausted.
    fn recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);

        future_into_py(py, async move {
            let mut rx = inner.lock().await;
            match rx.recv().await {
                Some(Ok(batch)) => Python::with_gil(|py| {
                    record_batch_to_pyarrow(py, batch).map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(format!(
                            "FFI conversion error: {}",
                            e
                        ))
                    })
                }),
                Some(Err(e)) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Error in stream: {}",
                    e
                ))),
                None => Ok(Python::with_gil(|py| py.None())),
            }
        })
    }

    /// Close the stream, dropping the receiver.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        
        future_into_py(py, async move {
            inner.lock().await.close();
            Ok(Python::with_gil(|py| py.None()))
        })
    }

    /// Get current statistics (service + pipeline)
    fn get_stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pipeline = self.pipeline.clone();
        let service = self.service.clone();

        future_into_py::<_, PyObject>(py, async move {
            let pstats = if let Some(p) = pipeline { Some(p.get_stats().await) } else { None };
            let sstats = service.map(|s| s.get_stats_snapshot());
            Python::with_gil(|py| {
                let dict = PyDict::new(py);
                if let Some(s) = sstats {
                    dict.set_item("events_extracted", s.events_extracted)?;
                    dict.set_item("events_enriched", s.events_enriched)?;
                    dict.set_item("batches_emitted", s.batches_emitted)?;
                    dict.set_item("rows_emitted", s.rows_emitted)?;
                    dict.set_item("enrichment_errors", s.enrichment_errors)?;
                }
                if let Some(p) = pstats {
                    dict.set_item("pipeline_seen_events", p.seen_events_count)?;
                    dict.set_item("pipeline_buffer_size", p.reorder_buffer_size)?;
                    dict.set_item("pipeline_highest_block", p.highest_seen_block)?;
                }
                Ok(dict.unbind().into())
            })
        })
    }

    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);

        future_into_py(py, async move {
            let mut rx = inner.lock().await;
            match rx.recv().await {
                Some(Ok(batch)) => Python::with_gil(|py| {
                    record_batch_to_pyarrow(py, batch).map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(format!(
                            "FFI conversion error: {}",
                            e
                        ))
                    })
                }),
                Some(Err(e)) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Error in stream: {}",
                    e
                ))),
                None => Err(pyo3::exceptions::PyStopAsyncIteration::new_err(())),
            }
        })
    }

    
}

/// A Python-exposed asynchronous stream that yields individual swaps instead of batches
#[pyclass]
pub struct DextradesSwapStream {
    inner: Arc<Mutex<mpsc::Receiver<Result<RecordBatch, Report>>>>,
    current_batch: Arc<Mutex<VecDeque<PyObject>>>,
    #[allow(dead_code)]
    pipeline: Option<Arc<EventPipeline>>,
    #[allow(dead_code)]
    service: Option<Arc<DextradesService>>,
}

impl DextradesSwapStream {
    /// Create a new swap stream from an Arrow stream
    pub fn new(arrow_stream: DextradesArrowStream) -> Self {
        Self {
            inner: arrow_stream.inner,
            current_batch: Arc::new(Mutex::new(VecDeque::new())),
            pipeline: arrow_stream.pipeline,
            service: arrow_stream.service,
        }
    }
    
    /// Convert a swap dictionary to a typed Python wrapper
    fn dict_to_swap_namedtuple(py: Python, swap_dict: PyObject) -> PyResult<PyObject> {
        let wrapper = PySwap { inner: swap_dict };
        // Avoid deprecated IntoPy::into_py by converting Py<T> into PyObject directly
        Ok(Py::new(py, wrapper)?.into())
    }
}

#[pymethods]
impl DextradesSwapStream {
    /// Close the stream, dropping the underlying receiver
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        
        future_into_py(py, async move {
            let mut rx = inner.lock().await;
            rx.close();
            Ok(Python::with_gil(|py| py.None()))
        })
    }

    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let current_batch = Arc::clone(&self.current_batch);
        
        future_into_py(py, async move {
            loop {
                // Check if we have any swaps in the current batch
                {
                    let mut batch_guard = current_batch.lock().await;
                    if let Some(swap) = batch_guard.pop_front() {
                        return Ok(swap);
                    }
                }
                
                // Need to get the next batch from the underlying receiver
                let mut rx = inner.lock().await;
                match rx.recv().await {
                    Some(Ok(batch)) => {
                        drop(rx);
                        
                        // Convert Arrow batch to Python and process all swaps
                        let batch_result = Python::with_gil(|py| {
                            let batch_obj = record_batch_to_pyarrow(py, batch)
                                .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("FFI conversion error: {}", e)))?;
                            let batch = batch_obj.bind(py);
                            
                            // Convert Arrow batch to Python list
                            let pylist = batch.call_method0("to_pylist")?;
                            Ok(pylist.unbind())
                        });
                        
                        match batch_result {
                            Ok(pylist) => {
                                // Convert list of dictionaries to list of namedtuples and queue them
                                let swaps = Python::with_gil(|py| {
                                    let list = pylist.bind(py);
                                    let len = list.len()?;
                                    if len == 0 {
                                        return Ok::<Vec<PyObject>, PyErr>(Vec::new());
                                    }
                                    
                                    let mut swaps = Vec::new();
                                    for i in 0..len {
                                        let swap_dict = list.get_item(i)?;
                                        let swap_namedtuple = Self::dict_to_swap_namedtuple(py, swap_dict.unbind())?;
                                        swaps.push(swap_namedtuple);
                                    }
                                    Ok(swaps)
                                })?;
                                
                                // Queue all swaps except the first one, or continue if empty
                                if !swaps.is_empty() {
                                    let mut swaps_iter = swaps.into_iter();
                                    let first_swap = swaps_iter.next().unwrap();
                                    
                                    // Queue the remaining swaps
                                    let mut batch_guard = current_batch.lock().await;
                                    for swap in swaps_iter {
                                        batch_guard.push_back(swap);
                                    }
                                    drop(batch_guard);
                                    
                                    // Return the first swap immediately
                                    return Ok(first_swap);
                                }
                                // If swaps is empty, continue to next batch
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    Some(Err(e)) => {
                        drop(rx);
                        return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "Error in stream: {}",
                            e
                        )));
                    }
                    None => {
                        drop(rx);
                        return Err(pyo3::exceptions::PyStopAsyncIteration::new_err(()));
                    }
                }
            }
        })
    }
}

/// Convert a Rust RecordBatch into a PyArrow object using the to_pyarrow method.
fn record_batch_to_pyarrow(py: Python, batch: RecordBatch) -> Result<PyObject, Report> {
    // With Arrow 55+ and the pyarrow feature, we can use the to_pyarrow method directly
    // This handles all the FFI details for us, including memory management
    Ok(batch.to_pyarrow(py)?)
}

/// Spawn an asynchronous stream of Dextrades swap RecordBatches.
/// This version fetches real logs from the blockchain using provided RPC URLs.
#[pyfunction]
pub fn stream_dex_swaps(
    _py: Python,
    protocols: Vec<String>, // Changed to support multiple protocols
    from_block: u64,
    to_block: u64,
    address: Option<String>, // None = all pools, Some(addr) = specific pool
    batch_size: Option<u64>,
    rpc_urls: Vec<String>,
    enrich_timestamps: Option<bool>, // Optional timestamp enrichment
    enrich_usd: Option<bool>,        // Optional USD pricing enrichment
    max_concurrent_chunks: Option<usize>, // NEW: Control chunk-level concurrency
    routers: Option<Vec<String>>,    // Optional router whitelist (tx.to addresses)
) -> PyResult<DextradesArrowStream> {
    if rpc_urls.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "At least one RPC URL must be provided",
        ));
    }

    if protocols.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "At least one protocol must be provided",
        ));
    }

    let rt = pyo3_async_runtimes::tokio::get_runtime();
    let (tx, rx) = mpsc::channel::<Result<RecordBatch, Report>>(10);

    info!("[Stream Task] Using real RPC with {} URLs", rpc_urls.len());

    // Clone values for later
    let protocols_clone = protocols.clone();
    let address_clone = address.clone();
    let enrich_timestamps = enrich_timestamps.unwrap_or(false);

    // Build configuration synchronously
    let mut config = if let Some(batch_size_param) = batch_size {
        DextradesConfig::builder()
            .rpc_urls(rpc_urls.clone())
            .streaming_batch_size(batch_size_param)
            .max_concurrent_batches(max_concurrent_chunks.unwrap_or(10))
            .build()
    } else {
        let mut cfg = DextradesConfig::balanced_streaming();
        cfg.default_rpc_urls = rpc_urls.clone();
        if let Some(chunks) = max_concurrent_chunks {
            cfg.max_concurrent_batches = chunks;
        }
        cfg
    };
    if let Some(r) = routers { config.router_whitelist = Some(r); }

    // Create service using the runtime (blocking only for init)
    let service = match rt.block_on(async { DextradesService::new(config).await }) {
        Ok(svc) => Arc::new(svc),
        Err(e) => {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Failed to create service: {}",
                e
            )));
        }
    };

    // CRITICAL FIX: Global session warmup to prevent cold start failures (non-fatal)
    let _ = rt.block_on(async { service.warmup_streaming_session().await });

    // Create enrichment pipeline (optionally add USD pricing enricher)
    let pipeline = {
        let base_result = if enrich_timestamps {
            crate::enrichment::EnrichmentPresets::standard()
        } else {
            crate::enrichment::EnrichmentPresets::minimal()
        };
        let mut base = match base_result {
            Ok(p) => p,
            Err(e) => {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to create enrichment pipeline: {}",
                    e
                )));
            }
        };
        if enrich_usd.unwrap_or(false) {
            base = base.add_enricher(Box::new(crate::enrichment::price_usd::PriceUsdEnricher::default()));
        }
        Arc::new(base)
    };

    // Event processing pipeline for deduplication and ordering
    let event_pipeline_config = PipelineConfig {
        reorder_window_blocks: 0,
        max_reorder_delay: std::time::Duration::from_millis(100),
        pruning_interval: std::time::Duration::from_secs(300),
    };
    let event_pipeline = match rt.block_on(async { EventPipeline::new(event_pipeline_config, Arc::clone(&service)).await }) {
        Ok(p) => Arc::new(p),
        Err(e) => {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Failed to create event pipeline: {}",
                e
            )));
        }
    };

    // Get effective values from service config
    let batch_size = service.config().effective_batch_size();
    let max_concurrent_chunks = service.config().effective_max_concurrent_batches();

    // Start async task to process the stream using pre-built context
    let service_for_task = Arc::clone(&service);
    let pipeline_for_task = Arc::clone(&pipeline);
    let event_pipeline_for_task = Arc::clone(&event_pipeline);
    let tx_for_task = tx.clone();
    let future = async move {
        info!(
            "🚀 [Stream Config] Using {} mode: batch_size={}, max_concurrent_batches={}",
            match service_for_task.config().streaming_mode {
                crate::config::StreamingMode::Immediate => "Immediate",
                crate::config::StreamingMode::Balanced => "Balanced",
                crate::config::StreamingMode::Throughput => "Throughput",
                crate::config::StreamingMode::Custom => "Custom",
            },
            batch_size,
            max_concurrent_chunks
        );

        // Parse the pool address if provided
        let pool_filter = if let Some(ref addr_str) = address_clone {
            match parse_address(addr_str) {
                Ok(addr) => Some(addr),
                Err(e) => {
                    let err = format!("Invalid pool address: {}", e);
                    info!("[Stream Task] {}", err);
                    let _ = tx.send(Err(eyre::eyre!(err))).await;
                    return;
                }
            }
        } else {
            None
        };

        // Warm up logs to prime provider capabilities and caches
        if let Some(proto) = protocols_clone.get(0) {
            let event_signature: Option<alloy::primitives::B256> = match proto.as_str() {
                "uniswap_v2" => Some(crate::protocols::uniswap_v2::get_swap_event_signature()),
                "uniswap_v3" => Some(crate::protocols::uniswap_v3::get_swap_event_signature()),
                _ => None,
            };
            if let Some(sig) = event_signature {
                let mut filter = alloy::rpc::types::Filter::new()
                    .from_block(from_block)
                    .to_block(from_block)
                    .event_signature(sig);
                if let Some(addr) = pool_filter {
                    filter = filter.address(addr);
                }
                match service_for_task.rpc_service().get_logs(&filter).await {
                    Ok(_) => info!("✅ [Stream Task] Log warmup succeeded"),
                    Err(e) => info!("⚠️ [Stream Task] Log warmup non-fatal: {}", e),
                }
            }
        }

        // Process data in batches
        let num_batches = ((to_block - from_block) / batch_size) + 1;

        // Start streaming batches
        let pool_desc = if let Some(ref addr) = address_clone {
            format!("pool {}", addr)
        } else {
            "all pools".to_string()
        };

        info!(
            "🚀 [Stream Task] Starting stream for protocols {:?} {}",
            protocols_clone, pool_desc
        );
        info!(
            "📆 [Stream Task] Blocks: {} to {}, batch size: {}, max concurrent chunks: {}, total batches: {}",
            from_block, to_block, batch_size, max_concurrent_chunks, num_batches
        );

        // Process chunks concurrently using FuturesUnordered for better control
        let mut pending_chunks = FuturesUnordered::new();
        let mut next_batch_idx = 0;
        let mut completed_chunks = 0;

        // Helper function to create chunk processing futures
        let create_chunk_future =
            |batch_idx: u64,
             service: Arc<DextradesService>,
             pipeline: Arc<crate::enrichment::EnrichmentPipeline>,
             event_pipeline: Arc<EventPipeline>,
             protocols: Vec<String>,
             pool_filter: Option<alloy::primitives::Address>| {
                async move {
                    let batch_start = from_block + (batch_idx * batch_size);
                    let batch_end = std::cmp::min(to_block, batch_start + batch_size - 1);

                    let _batch_start_time = std::time::Instant::now();
                    info!(
                        "⏱️ [Stream Task] Processing batch {}/{}: blocks {} to {}",
                        batch_idx + 1, num_batches, batch_start, batch_end
                    );

                    let mut all_extracted_events = Vec::new();

                    // Process each protocol
                    for protocol in &protocols {
                        // Get event signature and extract function for this protocol
                        let (event_signature, extract_fn): (alloy::primitives::B256, fn(&[alloy::rpc::types::Log]) -> Vec<SwapEvent>) = match protocol.as_str() {
                            "uniswap_v2" => (crate::protocols::uniswap_v2::get_swap_event_signature(), crate::protocols::uniswap_v2::extract_swaps),
                            "uniswap_v3" => (crate::protocols::uniswap_v3::get_swap_event_signature(), crate::protocols::uniswap_v3::extract_swaps),
                            _ => {
                                eprintln!("[Stream Task] Unknown protocol: {}", protocol);
                                continue;
                            }
                        };

                        // Fetch logs using the RPC service
                        let mut filter = alloy::rpc::types::Filter::new()
                            .from_block(batch_start)
                            .to_block(batch_end)
                            .event_signature(event_signature);

                        // Add pool filter if specified
                        if let Some(pool_addr) = pool_filter {
                            filter = filter.address(pool_addr);
                        }

                        let logs = if service.config().shard_logs {
                            let result = match service.config().provider_strategy {
                                crate::config::ProviderStrategy::Race => {
                                    // Use racing shards (existing behavior)
                                    service.rpc_service().get_logs_sharded(
                                        event_signature,
                                        pool_filter,
                                        batch_start,
                                        batch_end,
                                        service.config().providers_to_race,
                                    ).await
                                }
                                crate::config::ProviderStrategy::Shard => {
                                    // Use provider-assigned shards (new linear scaling)
                                    service.rpc_service().get_logs_provider_assigned_shards(
                                        event_signature,
                                        pool_filter,
                                        batch_start,
                                        batch_end,
                                        service.config().providers_to_race,
                                    ).await
                                }
                            };
                            match result {
                                Ok(raw_logs) => {
                                    info!("✅ [{}] Got {} sharded logs for blocks {} to {}", protocol, raw_logs.len(), batch_start, batch_end);
                                    extract_fn(&raw_logs)
                                }
                                Err(e) => {
                                    let error_str = e.to_string();
                                    if error_str.contains("32701") || error_str.contains("Please specify an address") {
                                        debug!("⚠️ [{}] Provider restriction - using fallback (expected behavior)", protocol);
                                    } else {
                                        warn!("❌ [{}] RPC error: {}", protocol, e);
                                    }
                                    Vec::new()
                                }
                            }
                        } else {
                            match service.rpc_service().get_logs(&filter).await {
                                Ok(raw_logs) => {
                                    info!("✅ [{}] Got {} raw logs for blocks {} to {}", protocol, raw_logs.len(), batch_start, batch_end);
                                    // Extract swaps using protocol-specific function
                                    extract_fn(&raw_logs)
                                }
                                Err(e) => {
                                    let error_str = e.to_string();
                                    if error_str.contains("32701") || error_str.contains("Please specify an address") {
                                        debug!("⚠️ [{}] Provider restriction - using fallback (expected behavior)", protocol);
                                    } else {
                                        warn!("❌ [{}] RPC error: {}", protocol, e);
                                    }
                                Vec::new()
                                }
                            }
                        };

                        info!("🔄 [{}] Extracted {} swap events", protocol, logs.len());
                        all_extracted_events.extend(logs);
                    }

                    if !all_extracted_events.is_empty() {
                        // Record extracted events
                        service.record_extracted(all_extracted_events.len() as u64);
                        // Process events through the multi-stage pipeline
                        let processed_events = event_pipeline.process_events(all_extracted_events).await;
                        
                        if !processed_events.is_empty() {
                            // Apply enrichment pipeline to ordered, deduplicated events
                            let mut enriched_events = processed_events;

                            let enrichment_start = std::time::Instant::now();
                            match pipeline.enrich_all(&mut enriched_events, &service).await {
                                Ok(()) => {
                                    let enrichment_duration = enrichment_start.elapsed();
                                    info!(
                                        "✨ [Stream Task] Successfully enriched {} events in {:?}",
                                        enriched_events.len(), enrichment_duration
                                    );
                                    // Record enrichment success count
                                    service.record_enriched(enriched_events.len() as u64);
                                }
                                Err(e) => {
                                    eprintln!("[Stream Task] Error during enrichment: {}", e);
                                    service.record_enrichment_error();
                                    // Continue with partially enriched events
                                }
                            }

                            // Apply filtering if configured
                            let filtered_events = apply_swap_filters(&enriched_events, &service.config());
                            if filtered_events.len() != enriched_events.len() {
                                debug!(
                                    "[Stream Task] Filtered {} events to {} after applying filters",
                                    enriched_events.len(),
                                    filtered_events.len()
                                );
                            }
                            let enriched_events = filtered_events;

                            // Create a record batch from the enriched event data
                            match create_batch_from_swap_events(&enriched_events) {
                                Ok(batch) => Some((batch_idx, Ok(batch))),
                                Err(e) => {
                                    eprintln!("[Stream Task] Error creating batch from events: {}", e);
                                    Some((batch_idx, Err(e)))
                                }
                            }
                        } else {
                            None // No events ready for release yet (waiting in reorder buffer)
                        }
                    } else {
                        None // No events in this chunk
                    }
                }
            };

        // Start initial chunks
        while next_batch_idx < num_batches && pending_chunks.len() < max_concurrent_chunks {
            let future = create_chunk_future(
                next_batch_idx,
                Arc::clone(&service_for_task),
                Arc::clone(&pipeline_for_task),
                Arc::clone(&event_pipeline_for_task),
                protocols_clone.clone(),
                pool_filter,
            );
            pending_chunks.push(Box::pin(future));
            next_batch_idx += 1;
        }

        // Process chunks as they complete
        while let Some(chunk_result) = pending_chunks.next().await {
            if let Some((batch_idx, result)) = chunk_result {
                match result {
                    Ok(batch) => {
                        // Capture rows before sending to avoid move borrow issue
                        let rows_to_record = batch.num_rows() as u64;
                        // Send the batch to the Python side
                        if tx_for_task.send(Ok(batch)).await.is_err() {
                            info!("[Stream Task] Receiver dropped, stopping stream");
                            break;
                        }
                        // Record batch emission
                        service_for_task.record_batch_emitted(rows_to_record);
                        debug!("[Stream Task] Completed chunk {}", batch_idx);
                    }
                    Err(e) => {
                        eprintln!("[Stream Task] Error in chunk {}: {}", batch_idx, e);
                        if tx_for_task.send(Err(e)).await.is_err() {
                            info!("[Stream Task] Receiver dropped, stopping stream");
                            break;
                        }
                    }
                }
            }

            completed_chunks += 1;

            // Start next chunk if available
            if next_batch_idx < num_batches {
                let future = create_chunk_future(
                    next_batch_idx,
                    Arc::clone(&service_for_task),
                    Arc::clone(&pipeline_for_task),
                    Arc::clone(&event_pipeline_for_task),
                    protocols_clone.clone(),
                    pool_filter,
                );
                pending_chunks.push(Box::pin(future));
                next_batch_idx += 1;
            }
        }

        // Flush any remaining events from the reorder buffer
        let remaining_events = event_pipeline_for_task.flush_remaining_events().await;
        if !remaining_events.is_empty() {
            // Apply enrichment to remaining events
            let mut enriched_remaining = remaining_events;
            let enrichment_start = std::time::Instant::now();
            match pipeline_for_task.enrich_all(&mut enriched_remaining, &service_for_task).await {
                Ok(()) => {
                    let enrichment_duration = enrichment_start.elapsed();
                    info!(
                        "✨ [Stream Task] Successfully enriched {} remaining events in {:?}",
                        enriched_remaining.len(), enrichment_duration
                    );
                }
                Err(e) => {
                    eprintln!("[Stream Task] Error enriching remaining events: {}", e);
                }
            }

            // Apply filtering and send remaining events
            let filtered_remaining = apply_swap_filters(&enriched_remaining, &service_for_task.config());
            if !filtered_remaining.is_empty() {
                match create_batch_from_swap_events(&filtered_remaining) {
                    Ok(batch) => {
                        if tx_for_task.send(Ok(batch)).await.is_err() {
                            info!("[Stream Task] Receiver dropped during flush");
                        } else {
                            info!("[Stream Task] Flushed {} remaining events", filtered_remaining.len());
                        }
                    }
                    Err(e) => {
                        eprintln!("[Stream Task] Error creating batch from remaining events: {}", e);
                    }
                }
            }
        }

        info!(
            "[Stream Task] Stream complete, processed {} chunks",
            completed_chunks
        );
    };

    // Spawn the streaming task
    rt.spawn(future);

    Ok(DextradesArrowStream::new(rx, Some(event_pipeline), Some(service)))
}

/// Create a RecordBatch from SwapEvent data
pub fn create_batch_from_swap_events(swap_events: &[SwapEvent]) -> Result<RecordBatch, Report> {
    use arrow::array::{Float64Array, Int64Array, StringArray, UInt64Array, UInt8Array};

    let schema = Arc::new(crate::schema::swap_event_schema());
    let num_rows = swap_events.len();

    if num_rows == 0 {
        // Return empty batch with correct schema
        return RecordBatch::try_new(
            schema,
            vec![
                // Core fields
                Arc::new(UInt64Array::from(Vec::<u64>::new())), // block_number
                Arc::new(Int64Array::from(Vec::<Option<i64>>::new())), // block_timestamp
                Arc::new(StringArray::from(Vec::<String>::new())), // tx_hash
                Arc::new(UInt64Array::from(Vec::<u64>::new())), // log_index
                Arc::new(StringArray::from(Vec::<String>::new())), // dex_protocol
                Arc::new(StringArray::from(Vec::<String>::new())), // pool_address
                // Participants
                Arc::new(StringArray::from(Vec::<Option<String>>::new())), // taker
                Arc::new(StringArray::from(Vec::<Option<String>>::new())), // recipient
                // Token metadata
                Arc::new(StringArray::from(Vec::<Option<String>>::new())), // token0_address
                Arc::new(StringArray::from(Vec::<Option<String>>::new())), // token1_address
                Arc::new(StringArray::from(Vec::<Option<String>>::new())), // token0_symbol
                Arc::new(StringArray::from(Vec::<Option<String>>::new())), // token1_symbol
                Arc::new(UInt8Array::from(Vec::<Option<u8>>::new())),      // token0_decimals
                Arc::new(UInt8Array::from(Vec::<Option<u8>>::new())),      // token1_decimals
                // Trade direction
                Arc::new(StringArray::from(Vec::<Option<String>>::new())), // token_bought_address
                Arc::new(StringArray::from(Vec::<Option<String>>::new())), // token_sold_address
                Arc::new(StringArray::from(Vec::<Option<String>>::new())), // token_bought_symbol
                Arc::new(StringArray::from(Vec::<Option<String>>::new())), // token_sold_symbol
                Arc::new(StringArray::from(Vec::<Option<String>>::new())), // token_bought_amount_raw
                Arc::new(StringArray::from(Vec::<Option<String>>::new())), // token_sold_amount_raw
                Arc::new(Float64Array::from(Vec::<Option<f64>>::new())),   // token_bought_amount
                Arc::new(Float64Array::from(Vec::<Option<f64>>::new())),   // token_sold_amount
                // Transaction context
                Arc::new(StringArray::from(Vec::<Option<String>>::new())), // tx_from
                Arc::new(StringArray::from(Vec::<Option<String>>::new())), // tx_to
                Arc::new(UInt64Array::from(Vec::<Option<u64>>::new())),    // gas_used
                // Price analysis
                Arc::new(Float64Array::from(Vec::<Option<f64>>::new())), // price_weth_per_token
            ],
        )
        .map_err(|e| eyre::eyre!("Failed to create empty batch: {}", e));
    }

    // Extract data from SwapEvent structs
    let mut block_numbers = Vec::with_capacity(num_rows);
    let mut block_timestamps = Vec::with_capacity(num_rows);
    let mut tx_hashes = Vec::with_capacity(num_rows);
    let mut log_indices = Vec::with_capacity(num_rows);
    let mut dex_protocols = Vec::with_capacity(num_rows);
    let mut pool_addresses = Vec::with_capacity(num_rows);
    let mut takers = Vec::with_capacity(num_rows);
    let mut recipients = Vec::with_capacity(num_rows);

    // Token metadata arrays
    let mut token0_addresses = Vec::with_capacity(num_rows);
    let mut token1_addresses = Vec::with_capacity(num_rows);
    let mut token0_symbols = Vec::with_capacity(num_rows);
    let mut token1_symbols = Vec::with_capacity(num_rows);
    let mut token0_decimals = Vec::with_capacity(num_rows);
    let mut token1_decimals = Vec::with_capacity(num_rows);

    // Trade direction arrays
    let mut token_bought_addresses = Vec::with_capacity(num_rows);
    let mut token_sold_addresses = Vec::with_capacity(num_rows);
    let mut token_bought_symbols = Vec::with_capacity(num_rows);
    let mut token_sold_symbols = Vec::with_capacity(num_rows);
    let mut token_bought_amount_raws = Vec::with_capacity(num_rows);
    let mut token_sold_amount_raws = Vec::with_capacity(num_rows);
    let mut token_bought_amounts = Vec::with_capacity(num_rows);
    let mut token_sold_amounts = Vec::with_capacity(num_rows);

    // Transaction context arrays
    let mut tx_froms = Vec::with_capacity(num_rows);
    let mut tx_tos = Vec::with_capacity(num_rows);
    let mut gas_useds = Vec::with_capacity(num_rows);

    // Price analysis arrays
    let mut price_weth_per_tokens = Vec::with_capacity(num_rows);
    let mut value_usds = Vec::with_capacity(num_rows);
    let mut value_usd_methods = Vec::with_capacity(num_rows);

    for event in swap_events {
        // Core fields
        block_numbers.push(event.block_number);
        block_timestamps.push(event.block_timestamp);
        tx_hashes.push(event.tx_hash.clone());
        log_indices.push(event.log_index);
        dex_protocols.push(event.dex_protocol.clone());
        pool_addresses.push(event.pool_address.clone());

        // Participants
        takers.push(event.taker.clone());
        recipients.push(event.recipient.clone());

        // Token metadata
        token0_addresses.push(event.token0_address.clone());
        token1_addresses.push(event.token1_address.clone());
        token0_symbols.push(event.token0_symbol.clone());
        token1_symbols.push(event.token1_symbol.clone());
        token0_decimals.push(event.token0_decimals);
        token1_decimals.push(event.token1_decimals);

        // Trade direction
        token_bought_addresses.push(event.token_bought_address.clone());
        token_sold_addresses.push(event.token_sold_address.clone());
        token_bought_symbols.push(event.token_bought_symbol.clone());
        token_sold_symbols.push(event.token_sold_symbol.clone());
        token_bought_amount_raws.push(event.token_bought_amount_raw.clone());
        token_sold_amount_raws.push(event.token_sold_amount_raw.clone());
        token_bought_amounts.push(event.token_bought_amount);
        token_sold_amounts.push(event.token_sold_amount);

        // Transaction context
        tx_froms.push(event.tx_from.clone());
        tx_tos.push(event.tx_to.clone());
        gas_useds.push(event.gas_used);

        // Price analysis
        price_weth_per_tokens.push(event.price_weth_per_token);
        value_usds.push(event.value_usd);
        value_usd_methods.push(event.value_usd_method.clone());
    }

    // Create the RecordBatch
    let batch = RecordBatch::try_new(
        schema,
        vec![
            // Core fields
            Arc::new(UInt64Array::from(block_numbers)),
            Arc::new(Int64Array::from(block_timestamps)),
            Arc::new(StringArray::from(tx_hashes)),
            Arc::new(UInt64Array::from(log_indices)),
            Arc::new(StringArray::from(dex_protocols)),
            Arc::new(StringArray::from(pool_addresses)),
            // Participants
            Arc::new(StringArray::from(takers)),
            Arc::new(StringArray::from(recipients)),
            // Token metadata
            Arc::new(StringArray::from(token0_addresses)),
            Arc::new(StringArray::from(token1_addresses)),
            Arc::new(StringArray::from(token0_symbols)),
            Arc::new(StringArray::from(token1_symbols)),
            Arc::new(UInt8Array::from(token0_decimals)),
            Arc::new(UInt8Array::from(token1_decimals)),
            // Trade direction
            Arc::new(StringArray::from(token_bought_addresses)),
            Arc::new(StringArray::from(token_sold_addresses)),
            Arc::new(StringArray::from(token_bought_symbols)),
            Arc::new(StringArray::from(token_sold_symbols)),
            Arc::new(StringArray::from(token_bought_amount_raws)),
            Arc::new(StringArray::from(token_sold_amount_raws)),
            Arc::new(Float64Array::from(token_bought_amounts)),
            Arc::new(Float64Array::from(token_sold_amounts)),
            // Transaction context
            Arc::new(StringArray::from(tx_froms)),
            Arc::new(StringArray::from(tx_tos)),
            Arc::new(UInt64Array::from(gas_useds)),
            // Price analysis
            Arc::new(Float64Array::from(price_weth_per_tokens)),
            // USD analysis
            Arc::new(Float64Array::from(value_usds)),
            Arc::new(StringArray::from(value_usd_methods)),
        ],
    )?;

    Ok(batch)
}

// (No unused enrichment helpers; enrichment handled in pipelines directly.)

/// Apply configured filters to swap events
pub fn apply_swap_filters(events: &[SwapEvent], config: &DextradesConfig) -> Vec<SwapEvent> {
    events
        .iter()
        .filter(|event| {
            // Router whitelist filter (requires transaction enricher)
            if let Some(ref routers) = config.router_whitelist {
                match &event.tx_to {
                    Some(to) => {
                        let pass = routers.iter().any(|r| r.eq_ignore_ascii_case(to));
                        if !pass { return false; }
                    }
                    None => return false,
                }
            }

            // Volume filter - skip if token amounts are not available
            if let Some(_min_volume) = config.min_volume_filter {
                // For now, we can only filter on token amounts since we don't have USD values
                // This would be enhanced with price oracle integration
                if let (Some(bought_amount), Some(sold_amount)) = (event.token_bought_amount, event.token_sold_amount) {
                    // Simple heuristic: if both amounts are very small, likely not a significant trade
                    if bought_amount < 0.001 && sold_amount < 0.001 {
                        return false;
                    }
                }
            }

            // Token whitelist filter
            if let Some(ref whitelist) = config.token_whitelist {
                let mut found_whitelisted = false;
                
                if let Some(ref token0) = event.token0_address {
                    if whitelist.iter().any(|addr| addr.eq_ignore_ascii_case(token0)) {
                        found_whitelisted = true;
                    }
                }
                
                if let Some(ref token1) = event.token1_address {
                    if whitelist.iter().any(|addr| addr.eq_ignore_ascii_case(token1)) {
                        found_whitelisted = true;
                    }
                }

                if !found_whitelisted {
                    return false;
                }
            }

            // Token blacklist filter
            if let Some(ref blacklist) = config.token_blacklist {
                if let Some(ref token0) = event.token0_address {
                    if blacklist.iter().any(|addr| addr.eq_ignore_ascii_case(token0)) {
                        return false;
                    }
                }
                
                if let Some(ref token1) = event.token1_address {
                    if blacklist.iter().any(|addr| addr.eq_ignore_ascii_case(token1)) {
                        return false;
                    }
                }
            }

            true
        })
        .cloned()
        .collect()
}
