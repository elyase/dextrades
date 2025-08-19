use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3::create_exception;

// Initialize the logger
fn init_logger() {
    let _ = env_logger::try_init();
}


// Re-export main modules
mod circuit_breaker;
mod config;
mod enrichment;
mod error;
mod event_pipeline;
mod individual_stream;
mod protocols;
mod rpc_rate_limiter;
// mod rpc_service; // removed legacy service in favor of RpcOrchestrator
mod rpc_orchestrator;
mod rpc_retry_service;
mod schema;
mod service;
mod stream;
mod token_metadata_service;
mod types;
mod chainlink_price_service;

use config::{DextradesConfig, ConfigBuilder};
use crate::config::NetworkOverrides;
use service::DextradesService;
use stream::*;
use std::sync::Arc;

// Create custom exception classes
create_exception!(dextrades, DextradesError, pyo3::exceptions::PyException);
create_exception!(dextrades, InvalidAddressError, DextradesError);
create_exception!(dextrades, RpcError, DextradesError);
create_exception!(dextrades, ProtocolError, DextradesError);
create_exception!(dextrades, ConfigError, DextradesError);
create_exception!(dextrades, ParsingError, DextradesError);
create_exception!(dextrades, StreamError, DextradesError);

/// Python wrapper for ConfigBuilder
#[pyclass]
pub struct PyConfigBuilder {
    inner: ConfigBuilder,
}

#[pymethods]
impl PyConfigBuilder {
    /// Create a new ConfigBuilder
    #[new]
    fn new() -> Self {
        Self {
            inner: ConfigBuilder::new(),
        }
    }
    
    /// Set RPC URLs
    fn rpc_urls(&mut self, urls: Vec<String>) -> PyResult<()> {
        self.inner = self.inner.clone().rpc_urls(urls);
        Ok(())
    }
    
    /// Set maximum concurrent requests
    fn max_concurrent_requests(&mut self, max: usize) -> PyResult<()> {
        self.inner = self.inner.clone().max_concurrent_requests(max);
        Ok(())
    }
    
    /// Set cache size
    fn cache_size(&mut self, size: u64) -> PyResult<()> {
        self.inner = self.inner.clone().cache_size(size);
        Ok(())
    }
    
    /// Set batch size
    fn batch_size(&mut self, size: u64) -> PyResult<()> {
        self.inner = self.inner.clone().batch_size(size);
        Ok(())
    }
    
    /// Configure circuit breaker
    fn circuit_breaker(&mut self, failure_threshold: usize, recovery_timeout_ms: u64, success_threshold: usize) -> PyResult<()> {
        self.inner = self.inner.clone().circuit_breaker(
            failure_threshold,
            std::time::Duration::from_millis(recovery_timeout_ms),
            success_threshold,
        );
        Ok(())
    }
    
    /// Enable adaptive batch sizing
    // Removed for minimal core
    
    /// Set volume filter
    fn volume_filter(&mut self, min_volume: f64) -> PyResult<()> {
        self.inner = self.inner.clone().volume_filter(min_volume);
        Ok(())
    }
    
    /// Set token whitelist
    fn token_whitelist(&mut self, whitelist: Vec<String>) -> PyResult<()> {
        self.inner = self.inner.clone().token_whitelist(whitelist);
        Ok(())
    }
    
    /// Set token blacklist
    fn token_blacklist(&mut self, blacklist: Vec<String>) -> PyResult<()> {
        self.inner = self.inner.clone().token_blacklist(blacklist);
        Ok(())
    }
    
    /// Set number of providers to race per request
    fn providers_to_race(&mut self, count: usize) -> PyResult<()> {
        self.inner = self.inner.clone().providers_to_race(count);
        Ok(())
    }

    /// Enable or disable sharding of log requests
    fn shard_logs(&mut self, enable: bool) -> PyResult<()> {
        self.inner = self.inner.clone().shard_logs(enable);
        Ok(())
    }

    /// Set provider strategy: "race" or "shard"
    fn provider_strategy(&mut self, strategy: &str) -> PyResult<()> {
        let strategy_enum = match strategy {
            "race" => crate::config::ProviderStrategy::Race,
            "shard" => crate::config::ProviderStrategy::Shard,
            _ => return Err(pyo3::exceptions::PyValueError::new_err(
                "provider_strategy must be 'race' or 'shard'"
            )),
        };
        self.inner = self.inner.clone().provider_strategy(strategy_enum);
        Ok(())
    }

    // Note: network_overrides are currently only configurable via Client(..., network_overrides={...})
    
    /// Build the configuration and create a client
    fn build_client(&self) -> PyResult<Client> {
        let config = self.inner.clone().build();
        let rt = pyo3_async_runtimes::tokio::get_runtime();
        let rpc_urls = config.default_rpc_urls.clone();

        let service = rt
            .block_on(async {
                DextradesService::new(config)
                .await
            })
            .map_err(|e: error::DextradesError| -> PyErr { e.to_py_err() })?;

        Ok(Client {
            inner: Arc::new(service),
            rpc_urls,
        })
    }
}

/// Python wrapper for AlloyRpcService
#[pyclass]
pub struct Client {
    inner: Arc<DextradesService>,
    rpc_urls: Vec<String>,
}

#[pymethods]
impl Client {
    /// Create a new client with the given RPC URLs and configuration
    #[new]
    #[pyo3(signature = (rpc_urls, max_concurrent_requests=10, cache_size=1000, providers_to_race=2, shard_logs=false, provider_strategy="race", network_overrides=None))]
    fn new(
        rpc_urls: Vec<String>,
        max_concurrent_requests: usize,
        cache_size: u64,
        providers_to_race: usize,
        shard_logs: bool,
        provider_strategy: &str,
        network_overrides: Option<PyObject>,
    ) -> PyResult<Self> {
        init_logger(); // Initialize the logger
        let rt = pyo3_async_runtimes::tokio::get_runtime();
        let rpc_urls_clone = rpc_urls.clone();

        let provider_strategy_enum = match provider_strategy {
            "race" => crate::config::ProviderStrategy::Race,
            "shard" => crate::config::ProviderStrategy::Shard,
            _ => return Err(pyo3::exceptions::PyValueError::new_err(
                "provider_strategy must be 'race' or 'shard'"
            )),
        };

        // Build config with optional overrides
        let mut cfg = DextradesConfig { default_rpc_urls: rpc_urls_clone, max_concurrent_requests, cache_size, providers_to_race, shard_logs, provider_strategy: provider_strategy_enum, ..Default::default() };
        if let Some(nw) = network_overrides {
            Python::with_gil(|py| -> PyResult<()> {
                let any = nw.bind(py);
                if let Ok(dict) = any.downcast::<pyo3::types::PyDict>() {
                    let mut ov = NetworkOverrides::default();
                    if let Ok(Some(val)) = dict.get_item("native_wrapped") { if let Ok(s) = val.extract::<String>() { ov.native_wrapped = Some(s); } }
                    if let Ok(Some(val)) = dict.get_item("native_usd_aggregator") { if let Ok(s) = val.extract::<String>() { ov.native_usd_aggregator = Some(s); } }
                    if let Ok(Some(val)) = dict.get_item("stable_addresses") { if let Ok(v) = val.extract::<Vec<String>>() { ov.stable_addresses = v; } }
                    if let Ok(Some(val)) = dict.get_item("warmup_tokens") { if let Ok(v) = val.extract::<Vec<String>>() { ov.warmup_tokens = v; } }
                    cfg.network_overrides = Some(ov);
                }
                Ok(())
            })?;
        }

        let service = rt
            .block_on(async { DextradesService::new(cfg).await })
            .map_err(|e: error::DextradesError| -> PyErr { e.to_py_err() })?;

        Ok(Self {
            inner: Arc::new(service),
            rpc_urls,
        })
    }

    fn get_chain_id<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let service = Arc::clone(&self.inner);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            service.get_chain_id().await.map_err(|e: error::DextradesError| -> PyErr { e.to_py_err() })
        })
    }

    fn get_block_number<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let service = Arc::clone(&self.inner);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            service.get_block_number().await.map_err(|e: error::DextradesError| -> PyErr { e.to_py_err() })
        })
    }

    /// Get RPC metrics snapshot as a Python dict
    fn get_rpc_metrics<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let service = Arc::clone(&self.inner);

        pyo3_async_runtimes::tokio::future_into_py::<_, PyObject>(py, async move {
            let metrics = service.get_rpc_metrics().await;
            Python::with_gil(|py| {
                let dict = pyo3::types::PyDict::new(py);
                dict.set_item("total_requests", metrics.total_requests)?;
                dict.set_item("rate_limit_hits", metrics.rate_limit_hits)?;
                dict.set_item("active_requests", metrics.active_requests)?;
                dict.set_item("requests_per_second", metrics.requests_per_second)?;

                // enricher breakdown as dict
                let eb = pyo3::types::PyDict::new(py);
                for (k, v) in metrics.enricher_breakdown.into_iter() {
                    eb.set_item(k, v)?;
                }
                dict.set_item("enricher_breakdown", eb)?;

                dict.set_item(
                    "measurement_duration_secs",
                    metrics.measurement_duration.as_secs_f64(),
                )?;

                Ok(dict.unbind().into())
            })
        })
    }

    /// Get service-level pipeline stats as a Python dict
    fn get_stats(&self, py: Python) -> PyResult<PyObject> {
        let service = Arc::clone(&self.inner);
        let s = service.get_stats_snapshot();
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("events_extracted", s.events_extracted)?;
        dict.set_item("events_enriched", s.events_enriched)?;
        dict.set_item("batches_emitted", s.batches_emitted)?;
        dict.set_item("rows_emitted", s.rows_emitted)?;
        dict.set_item("enrichment_errors", s.enrichment_errors)?;
        Ok(dict.unbind().into())
    }

    fn get_token_metadata<'a>(&self, py: Python<'a>, address: &str) -> PyResult<Bound<'a, PyAny>> {
        let service = Arc::clone(&self.inner);
        let address = address.to_string();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            service
                .get_token_metadata(&address)
                .await
                .map(|opt| opt.map(|info| info.as_tuple()))
                .map_err(|e: error::DextradesError| -> PyErr { e.to_py_err() })
        })
    }


    fn get_pool_tokens<'a>(&self, py: Python<'a>, address: &str) -> PyResult<Bound<'a, PyAny>> {
        let service = Arc::clone(&self.inner);
        let address = address.to_string();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            service.get_pool_tokens(&address).await.map_err(|e: error::DextradesError| -> PyErr { e.to_py_err() })
        })
    }

    fn get_logs<'a>(
        &self,
        py: Python<'a>,
        from_block: u64,
        to_block: u64,
        address: Option<&str>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let service = Arc::clone(&self.inner);
        let address = address.map(|s| s.to_string());

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            service
                .get_logs(from_block, to_block, address.as_deref())
                .await
                .map_err(|e: error::DextradesError| -> PyErr { e.to_py_err() })
        })
    }

    /// Stream DEX swaps - returns individual swaps by default, or Arrow batches for performance
    ///
    /// Args:
    ///     protocols: DEX protocol(s) - either a string like "uniswap_v2" or list like ["uniswap_v2", "uniswap_v3"]
    ///     from_block: Starting block number
    ///     to_block: Ending block number
    ///     address: Pool address to filter by (None for all pools)
    ///     batch_size: Number of blocks to process per batch
    ///     enrich_timestamps: Whether to enrich with block timestamps (slower)
    ///     max_concurrent_chunks: Maximum number of chunks to process concurrently (default: 3)
    ///     batches: Return Arrow batches if True, individual swaps if False (default: False)
    #[pyo3(signature = (protocols, from_block, to_block, address=None, batch_size=None, enrich_timestamps=None, enrich_usd=None, max_concurrent_chunks=None, routers=None, batches=false))]
    fn stream_swaps(
        &self,
        py: Python<'_>,
        protocols: PyObject, // Accept either string or list of strings
        from_block: u64,
        to_block: u64,
        address: Option<String>,
        batch_size: Option<u64>,
        enrich_timestamps: Option<bool>,
        enrich_usd: Option<bool>,
        max_concurrent_chunks: Option<usize>,
        routers: Option<Vec<String>>,
        batches: bool,
    ) -> PyResult<PyObject> {
        // Convert protocols to Vec<String>
        let protocols_vec: Vec<String> = if let Ok(protocol_str) = protocols.extract::<String>(py) {
            // Single protocol string
            vec![protocol_str]
        } else if let Ok(protocol_list) = protocols.extract::<Vec<String>>(py) {
            // List of protocols
            protocol_list
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "protocols must be a string or list of strings",
            ));
        };

        if batches {
            // Return Arrow batches for performance
            let arrow_stream = stream_dex_swaps(
                py,
                protocols_vec,
                from_block,
                to_block,
                address,
                batch_size,
                self.rpc_urls.clone(),
                enrich_timestamps,
                enrich_usd,
                max_concurrent_chunks,
                routers.clone(),
            )?;
            Ok(Py::new(py, arrow_stream)?.into())
        } else {
            // Return individual swaps (default)
            let arrow_stream = stream_dex_swaps(
                py,
                protocols_vec,
                from_block,
                to_block,
                address,
                batch_size,
                self.rpc_urls.clone(),
                enrich_timestamps,
                enrich_usd,
                max_concurrent_chunks,
                routers.clone(),
            )?;
            let swap_stream = DextradesSwapStream::new(arrow_stream);
            Ok(Py::new(py, swap_stream)?.into())
        }
    }

    /// Stream individual DEX swaps in real-time (true event-by-event streaming)
    ///
    /// This method provides true individual event streaming where each swap event
    /// is processed, enriched, and delivered immediately without waiting for
    /// block completion. This enables real-time applications with minimal latency.
    ///
    /// Args:
    ///     protocols: DEX protocol(s) - either a string like "uniswap_v2" or list like ["uniswap_v2", "uniswap_v3"]
    ///     from_block: Starting block number
    ///     to_block: Ending block number
    ///     address: Pool address to filter by (None for all pools)
    ///     enrich_timestamps: Whether to enrich with block timestamps (slower)
    #[pyo3(signature = (protocols, from_block, to_block, address=None, enrich_timestamps=None, enrich_usd=None, routers=None))]
    fn stream_individual_swaps(
        &self,
        py: Python<'_>,
        protocols: PyObject, // Accept either string or list of strings
        from_block: u64,
        to_block: u64,
        address: Option<String>,
        enrich_timestamps: Option<bool>,
        enrich_usd: Option<bool>,
        routers: Option<Vec<String>>,
    ) -> PyResult<PyObject> {
        // Convert protocols to Vec<String>
        let protocols_vec: Vec<String> = if let Ok(protocol_str) = protocols.extract::<String>(py) {
            // Single protocol string
            vec![protocol_str]
        } else if let Ok(protocol_list) = protocols.extract::<Vec<String>>(py) {
            // List of protocols
            protocol_list
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "protocols must be a string or list of strings",
            ));
        };

        // Use individual streaming function
        let arrow_stream = individual_stream::stream_individual_swaps(
            py,
            protocols_vec,
            from_block,
            to_block,
            address,
            self.rpc_urls.clone(),
            enrich_timestamps,
            enrich_usd,
            routers,
        )?;
        
        // Return individual swaps
        let swap_stream = DextradesSwapStream::new(arrow_stream);
        Ok(Py::new(py, swap_stream)?.into())
    }

    /// Context manager support - enter
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Context manager support - exit
    fn __exit__<'py>(
        &self,
        _py: Python<'py>,
        _exc_type: Option<&Bound<'py, PyAny>>,
        _exc_value: Option<&Bound<'py, PyAny>>,
        _traceback: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<bool> {
        // For now, we don't have explicit cleanup to do
        // This can be extended later if needed
        Ok(false) // Don't suppress exceptions
    }
}

/// Python module definition
#[pymodule]
fn dextrades(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(stream_dex_swaps, m)?)?;
    m.add_function(wrap_pyfunction!(individual_stream::stream_individual_swaps, m)?)?;

    m.add_class::<Client>()?;
    m.add_class::<PyConfigBuilder>()?;
    m.add_class::<DextradesArrowStream>()?;
    m.add_class::<DextradesSwapStream>()?;
    m.add_class::<stream::PySwap>()?;

    // Add custom exception classes
    m.add("DextradesError", _py.get_type::<DextradesError>())?;
    m.add("InvalidAddressError", _py.get_type::<InvalidAddressError>())?;
    m.add("RpcError", _py.get_type::<RpcError>())?;
    m.add("ProtocolError", _py.get_type::<ProtocolError>())?;
    m.add("ConfigError", _py.get_type::<ConfigError>())?;
    m.add("ParsingError", _py.get_type::<ParsingError>())?;
    m.add("StreamError", _py.get_type::<StreamError>())?;

    Ok(())
}
