//! Python logging framework integration
//!
//! This module provides comprehensive logging integration with Python's logging framework,
//! enabling structured logging, log levels, formatters, and handlers for DeltaLake operations.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use std::collections::HashMap;
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Logger configuration and management
#[pyclass]
#[derive(Debug, Clone)]
pub struct LoggerConfig {
    name: String,
    level: String,
    format: String,
    handlers: Vec<String>,
    propagate: bool,
}

#[pymethods]
impl LoggerConfig {
    #[new]
    #[pyo3(signature = (name="deltalakedb", level="INFO", format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", handlers=vec!["console".to_string()], propagate=true))]
    fn new(name: String, level: String, format: String, handlers: Vec<String>, propagate: bool) -> Self {
        Self {
            name,
            level,
            format,
            handlers,
            propagate,
        }
    }

    /// Get logger name
    #[getter]
    fn name(&self) -> String {
        self.name.clone()
    }

    /// Get log level
    #[getter]
    fn level(&self) -> String {
        self.level.clone()
    }

    /// Set log level
    #[setter]
    fn set_level(&mut self, level: String) {
        self.level = level;
    }

    /// Get format string
    #[getter]
    fn format(&self) -> String {
        self.format.clone()
    }

    /// Set format string
    #[setter]
    fn set_format(&mut self, format: String) {
        self.format = format;
    }

    /// Get handler list
    #[getter]
    fn handlers(&self) -> Vec<String> {
        self.handlers.clone()
    }

    /// Add handler
    fn add_handler(&mut self, handler: String) {
        if !self.handlers.contains(&handler) {
            self.handlers.push(handler);
        }
    }

    /// Remove handler
    fn remove_handler(&mut self, handler: String) {
        self.handlers.retain(|h| h != &handler);
    }

    /// Get propagate setting
    #[getter]
    fn propagate(&self) -> bool {
        self.propagate
    }

    /// Set propagate setting
    #[setter]
    fn set_propagate(&mut self, propagate: bool) {
        self.propagate = propagate;
    }

    /// Convert to dictionary
    fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("name", &self.name)?;
        dict.set_item("level", &self.level)?;
        dict.set_item("format", &self.format)?;
        dict.set_item("handlers", self.handlers.clone())?;
        dict.set_item("propagate", self.propagate)?;
        Ok(dict.to_object(py))
    }
}

/// Log entry with structured data
#[pyclass]
#[derive(Debug, Clone)]
pub struct LogEntry {
    timestamp: DateTime<Utc>,
    level: String,
    logger_name: String,
    message: String,
    module: Option<String>,
    function: Option<String>,
    line: Option<u32>,
    extra_data: HashMap<String, String>,
    transaction_id: Option<String>,
    table_name: Option<String>,
    operation: Option<String>,
    duration_ms: Option<f64>,
    error_code: Option<String>,
}

#[pymethods]
impl LogEntry {
    #[new]
    #[pyo3(signature = (level, logger_name, message, module=None, function=None, line=None, extra_data=None))]
    fn new(
        level: String,
        logger_name: String,
        message: String,
        module: Option<String>,
        function: Option<String>,
        line: Option<u32>,
        extra_data: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            timestamp: Utc::now(),
            level,
            logger_name,
            message,
            module,
            function,
            line,
            extra_data: extra_data.unwrap_or_default(),
            transaction_id: None,
            table_name: None,
            operation: None,
            duration_ms: None,
            error_code: None,
        }
    }

    /// Get timestamp
    #[getter]
    fn timestamp(&self) -> String {
        self.timestamp.to_rfc3339()
    }

    /// Get log level
    #[getter]
    fn level(&self) -> String {
        self.level.clone()
    }

    /// Get logger name
    #[getter]
    fn logger_name(&self) -> String {
        self.logger_name.clone()
    }

    /// Get message
    #[getter]
    fn message(&self) -> String {
        self.message.clone()
    }

    /// Get module
    #[getter]
    fn module(&self) -> Option<String> {
        self.module.clone()
    }

    /// Get function
    #[getter]
    fn function(&self) -> Option<String> {
        self.function.clone()
    }

    /// Get line number
    #[getter]
    fn line(&self) -> Option<u32> {
        self.line
    }

    /// Get extra data
    #[getter]
    fn extra_data(&self) -> HashMap<String, String> {
        self.extra_data.clone()
    }

    /// Add extra data
    fn add_extra_data(&mut self, key: String, value: String) {
        self.extra_data.insert(key, value);
    }

    /// Set transaction context
    fn set_transaction_context(&mut self, transaction_id: String, table_name: Option<String>, operation: Option<String>) {
        self.transaction_id = Some(transaction_id);
        self.table_name = table_name;
        self.operation = operation;
    }

    /// Set performance context
    fn set_performance_context(&mut self, duration_ms: f64) {
        self.duration_ms = Some(duration_ms);
    }

    /// Set error context
    fn set_error_context(&mut self, error_code: String) {
        self.error_code = Some(error_code);
    }

    /// Get transaction ID
    #[getter]
    fn transaction_id(&self) -> Option<String> {
        self.transaction_id.clone()
    }

    /// Get table name
    #[getter]
    fn table_name(&self) -> Option<String> {
        self.table_name.clone()
    }

    /// Get operation
    #[getter]
    fn operation(&self) -> Option<String> {
        self.operation.clone()
    }

    /// Get duration in milliseconds
    #[getter]
    fn duration_ms(&self) -> Option<f64> {
        self.duration_ms
    }

    /// Get error code
    #[getter]
    fn error_code(&self) -> Option<String> {
        self.error_code.clone()
    }

    /// Convert to dictionary
    fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("timestamp", self.timestamp.to_rfc3339())?;
        dict.set_item("level", &self.level)?;
        dict.set_item("logger_name", &self.logger_name)?;
        dict.set_item("message", &self.message)?;

        if let Some(module) = &self.module {
            dict.set_item("module", module)?;
        }
        if let Some(function) = &self.function {
            dict.set_item("function", function)?;
        }
        if let Some(line) = self.line {
            dict.set_item("line", line)?;
        }

        let extra_dict = PyDict::new(py);
        for (key, value) in &self.extra_data {
            extra_dict.set_item(key, value)?;
        }
        dict.set_item("extra_data", extra_dict)?;

        if let Some(transaction_id) = &self.transaction_id {
            dict.set_item("transaction_id", transaction_id)?;
        }
        if let Some(table_name) = &self.table_name {
            dict.set_item("table_name", table_name)?;
        }
        if let Some(operation) = &self.operation {
            dict.set_item("operation", operation)?;
        }
        if let Some(duration_ms) = self.duration_ms {
            dict.set_item("duration_ms", duration_ms)?;
        }
        if let Some(error_code) = &self.error_code {
            dict.set_item("error_code", error_code)?;
        }

        Ok(dict.to_object(py))
    }

    /// Convert to formatted log string
    fn to_formatted_string(&self, format: Option<String>) -> String {
        let format_str = format.unwrap_or_else(||
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s".to_string()
        );

        let mut result = format_str.clone();
        result = result.replace("%(asctime)s", &self.timestamp.to_rfc3339());
        result = result.replace("%(name)s", &self.logger_name);
        result = result.replace("%(levelname)s", &self.level);
        result = result.replace("%(message)s", &self.message);

        if let Some(module) = &self.module {
            result = result.replace("%(module)s", module);
        }
        if let Some(function) = &self.function {
            result = result.replace("%(funcName)s", function);
        }
        if let Some(line) = self.line {
            result = result.replace("%(lineno)s", &line.to_string());
        }

        result
    }
}

/// Logger with advanced features
#[pyclass]
#[derive(Debug)]
pub struct DeltaLogger {
    config: LoggerConfig,
    entries: Vec<LogEntry>,
    max_entries: usize,
    enable_file_logging: bool,
    enable_console_logging: bool,
    log_file_path: Option<String>,
}

#[pymethods]
impl DeltaLogger {
    #[new]
    #[pyo3(signature = (config=None, max_entries=10000, enable_file_logging=false, enable_console_logging=true, log_file_path=None))]
    fn new(
        config: Option<LoggerConfig>,
        max_entries: usize,
        enable_file_logging: bool,
        enable_console_logging: bool,
        log_file_path: Option<String>,
    ) -> Self {
        Self {
            config: config.unwrap_or_else(|| LoggerConfig::new(
                "deltalakedb".to_string(),
                "INFO".to_string(),
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s".to_string(),
                vec!["console".to_string()],
                true,
            )),
            entries: Vec::new(),
            max_entries,
            enable_file_logging,
            enable_console_logging,
            log_file_path,
        }
    }

    /// Get logger configuration
    #[getter]
    fn config(&self) -> LoggerConfig {
        self.config.clone()
    }

    /// Set logger configuration
    #[setter]
    fn set_config(&mut self, config: LoggerConfig) {
        self.config = config;
    }

    /// Get maximum number of entries
    #[getter]
    fn max_entries(&self) -> usize {
        self.max_entries
    }

    /// Set maximum number of entries
    #[setter]
    fn set_max_entries(&mut self, max_entries: usize) {
        self.max_entries = max_entries;
    }

    /// Enable file logging
    fn enable_file_logging(&mut self, file_path: String) -> PyResult<()> {
        self.enable_file_logging = true;
        self.log_file_path = Some(file_path);
        Ok(())
    }

    /// Disable file logging
    fn disable_file_logging(&mut self) {
        self.enable_file_logging = false;
        self.log_file_path = None;
    }

    /// Enable console logging
    fn enable_console_logging(&mut self) {
        self.enable_console_logging = true;
    }

    /// Disable console logging
    fn disable_console_logging(&mut self) {
        self.enable_console_logging = false;
    }

    /// Log a message
    fn log(&mut self, py: Python, level: String, message: String, module: Option<String>, function: Option<String>, line: Option<u32>, extra_data: Option<HashMap<String, String>>) -> PyResult<()> {
        // Create log entry
        let mut entry = LogEntry::new(level.clone(), self.config.name.clone(), message, module, function, line, extra_data);

        // Add to entries
        self.entries.push(entry.clone());

        // Trim entries if necessary
        if self.entries.len() > self.max_entries {
            self.entries.remove(0);
        }

        // Send to Python logging system
        if self.enable_console_logging || self.enable_file_logging {
            let logging = py.import("logging")?;
            let logger = logging.call_method1("getLogger", (&self.config.name,))?;

            let log_method = match level.to_uppercase().as_str() {
                "DEBUG" => "debug",
                "INFO" => "info",
                "WARNING" => "warning",
                "ERROR" => "error",
                "CRITICAL" => "critical",
                _ => "info",
            };

            logger.call_method1(log_method, (message,))?;
        }

        Ok(())
    }

    /// Log debug message
    fn debug(&mut self, py: Python, message: String, module: Option<String>, function: Option<String>, line: Option<u32>, extra_data: Option<HashMap<String, String>>) -> PyResult<()> {
        self.log(py, "DEBUG".to_string(), message, module, function, line, extra_data)
    }

    /// Log info message
    fn info(&mut self, py: Python, message: String, module: Option<String>, function: Option<String>, line: Option<u32>, extra_data: Option<HashMap<String, String>>) -> PyResult<()> {
        self.log(py, "INFO".to_string(), message, module, function, line, extra_data)
    }

    /// Log warning message
    fn warning(&mut self, py: Python, message: String, module: Option<String>, function: Option<String>, line: Option<u32>, extra_data: Option<HashMap<String, String>>) -> PyResult<()> {
        self.log(py, "WARNING".to_string(), message, module, function, line, extra_data)
    }

    /// Log error message
    fn error(&mut self, py: Python, message: String, module: Option<String>, function: Option<String>, line: Option<u32>, extra_data: Option<HashMap<String, String>>) -> PyResult<()> {
        self.log(py, "ERROR".to_string(), message, module, function, line, extra_data)
    }

    /// Log critical message
    fn critical(&mut self, py: Python, message: String, module: Option<String>, function: Option<String>, line: Option<u32>, extra_data: Option<HashMap<String, String>>) -> PyResult<()> {
        self.log(py, "CRITICAL".to_string(), message, module, function, line, extra_data)
    }

    /// Log database operation
    fn log_operation(&mut self, py: Python, operation: String, table_name: String, transaction_id: Option<String>, duration_ms: Option<f64>, success: bool, error_message: Option<String>) -> PyResult<()> {
        let level = if success { "INFO" } else { "ERROR" };
        let mut extra_data = HashMap::new();
        extra_data.insert("operation".to_string(), operation.clone());
        extra_data.insert("table_name".to_string(), table_name.clone());
        if let Some(tx_id) = &transaction_id {
            extra_data.insert("transaction_id".to_string(), tx_id.clone());
        }
        if let Some(duration) = duration_ms {
            extra_data.insert("duration_ms".to_string(), duration.to_string());
        }
        extra_data.insert("success".to_string(), success.to_string());

        let message = if success {
            format!("Operation '{}' completed successfully on table '{}' in {:.2}ms",
                   operation, table_name, duration_ms.unwrap_or(0.0))
        } else {
            format!("Operation '{}' failed on table '{}': {}",
                   operation, table_name, error_message.unwrap_or_else(|| "Unknown error".to_string()))
        };

        let mut entry = LogEntry::new(level.to_string(), self.config.name.clone(), message, None, None, None, Some(extra_data));
        entry.set_transaction_context(transaction_id.unwrap_or_else(|| Uuid::new_v4().to_string()), Some(table_name), Some(operation));
        if let Some(duration) = duration_ms {
            entry.set_performance_context(duration);
        }

        self.entries.push(entry.clone());

        // Send to Python logging system
        if self.enable_console_logging || self.enable_file_logging {
            let logging = py.import("logging")?;
            let logger = logging.call_method1("getLogger", (&self.config.name,))?;
            logger.call_method1(if success { "info" } else { "error" }, (entry.message.clone(),))?;
        }

        Ok(())
    }

    /// Log performance metrics
    fn log_performance(&mut self, py: Python, operation: String, metrics: HashMap<String, f64>) -> PyResult<()> {
        let mut extra_data = HashMap::new();
        for (key, value) in &metrics {
            extra_data.insert(key.clone(), value.to_string());
        }

        let message = format!("Performance metrics for '{}': {:?}", operation, metrics);

        let mut entry = LogEntry::new("INFO".to_string(), self.config.name.clone(), message, None, None, None, Some(extra_data));
        entry.set_transaction_context(Uuid::new_v4().to_string(), None, Some(operation));

        self.entries.push(entry.clone());

        // Send to Python logging system
        if self.enable_console_logging || self.enable_file_logging {
            let logging = py.import("logging")?;
            let logger = logging.call_method1("getLogger", (&self.config.name,))?;
            logger.call_method1("info", (entry.message.clone(),))?;
        }

        Ok(())
    }

    /// Get recent log entries
    fn get_recent_entries(&self, count: usize) -> Vec<LogEntry> {
        let start = if self.entries.len() > count {
            self.entries.len() - count
        } else {
            0
        };
        self.entries[start..].to_vec()
    }

    /// Get entries by level
    fn get_entries_by_level(&self, level: String) -> Vec<LogEntry> {
        self.entries.iter()
            .filter(|entry| entry.level.to_uppercase() == level.to_uppercase())
            .cloned()
            .collect()
    }

    /// Get entries by time range
    fn get_entries_by_time_range(&self, start_time: String, end_time: String) -> PyResult<Vec<LogEntry>> {
        let start_dt = DateTime::parse_from_rfc3339(&start_time)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid start time format: {}", e)))?;
        let end_dt = DateTime::parse_from_rfc3339(&end_time)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid end time format: {}", e)))?;

        Ok(self.entries.iter()
            .filter(|entry| entry.timestamp.timestamp() >= start_dt.timestamp() && entry.timestamp.timestamp() <= end_dt.timestamp())
            .cloned()
            .collect())
    }

    /// Clear all log entries
    fn clear_entries(&mut self) {
        self.entries.clear();
    }

    /// Get log statistics
    fn get_statistics(&self) -> HashMap<String, usize> {
        let mut stats = HashMap::new();
        stats.insert("total_entries".to_string(), self.entries.len());

        let mut level_counts = HashMap::new();
        for entry in &self.entries {
            let count = level_counts.entry(entry.level.clone()).or_insert(0);
            *count += 1;
        }

        for (level, count) in level_counts {
            stats.insert(format!("{}_count", level.to_lowercase()), count);
        }

        stats
    }

    /// Export logs to file
    fn export_logs(&self, py: Python, file_path: String, format: String) -> PyResult<()> {
        let builtins = py.import("builtins")?;
        let file = builtins.call_method1("open", (file_path, "w"))?;

        match format.to_lowercase().as_str() {
            "json" => {
                let json = py.import("json")?;
                let logs_list: PyResult<PyObject> = self.entries.iter()
                    .map(|entry| entry.to_dict(py))
                    .collect::<Result<Vec<_>, _>>()
                    .map(|entries| PyList::new(py, entries).to_object(py));

                json.call_method1("dump", (logs_list?, file))?;
            }
            "csv" => {
                // Write CSV header
                file.call_method1("write", ("timestamp,level,logger_name,message,module,function,line,transaction_id,table_name,operation,duration_ms,error_code\n",))?;

                // Write log entries
                for entry in &self.entries {
                    let line = format!("{},{},{},{},{},{},{},{},{},{},{},{}\n",
                        entry.timestamp.to_rfc3339(),
                        entry.level,
                        entry.logger_name,
                        entry.message.replace(",", "\\,"),
                        entry.module.as_deref().unwrap_or(""),
                        entry.function.as_deref().unwrap_or(""),
                        entry.line.unwrap_or(0),
                        entry.transaction_id.as_deref().unwrap_or(""),
                        entry.table_name.as_deref().unwrap_or(""),
                        entry.operation.as_deref().unwrap_or(""),
                        entry.duration_ms.unwrap_or(0.0),
                        entry.error_code.as_deref().unwrap_or("")
                    );
                    file.call_method1("write", (line,))?;
                }
            }
            _ => {
                // Default: plain text format
                for entry in &self.entries {
                    let formatted = entry.to_formatted_string(Some(self.config.format.clone()));
                    file.call_method1("write", (format!("{}\n", formatted),))?;
                }
            }
        }

        file.call_method0("close")?;
        Ok(())
    }
}

/// Logging utilities and factory functions
#[pyclass]
pub struct LoggingUtils;

#[pymethods]
impl LoggingUtils {
    /// Create logger with default configuration
    #[staticmethod]
    fn create_default_logger() -> DeltaLogger {
        DeltaLogger::new(None, 10000, false, true, None)
    }

    /// Create logger for database operations
    #[staticmethod]
    fn create_database_logger(log_level: Option<String>, log_file: Option<String>) -> DeltaLogger {
        let config = LoggerConfig::new(
            "deltalakedb.database".to_string(),
            log_level.unwrap_or_else(|| "INFO".to_string()),
            "%(asctime)s - %(name)s - %(levelname)s - [%(transaction_id)s] %(message)s".to_string(),
            vec!["console".to_string()],
            true,
        );

        DeltaLogger::new(
            Some(config),
            5000,
            log_file.is_some(),
            true,
            log_file,
        )
    }

    /// Create logger for transaction operations
    #[staticmethod]
    fn create_transaction_logger(log_level: Option<String>, log_file: Option<String>) -> DeltaLogger {
        let config = LoggerConfig::new(
            "deltalakedb.transaction".to_string(),
            log_level.unwrap_or_else(|| "DEBUG".to_string()),
            "%(asctime)s - %(name)s - %(levelname)s - [TX:%(transaction_id)s] %(message)s".to_string(),
            vec!["console".to_string()],
            true,
        );

        DeltaLogger::new(
            Some(config),
            10000,
            log_file.is_some(),
            true,
            log_file,
        )
    }

    /// Create logger for performance monitoring
    #[staticmethod]
    fn create_performance_logger(log_level: Option<String>, log_file: Option<String>) -> DeltaLogger {
        let config = LoggerConfig::new(
            "deltalakedb.performance".to_string(),
            log_level.unwrap_or_else(|| "INFO".to_string()),
            "%(asctime)s - %(name)s - %(levelname)s - [PERF] %(message)s".to_string(),
            vec!["console".to_string()],
            true,
        );

        DeltaLogger::new(
            Some(config),
            7500,
            log_file.is_some(),
            true,
            log_file,
        )
    }

    /// Create logger for error tracking
    #[staticmethod]
    fn create_error_logger(log_level: Option<String>, log_file: Option<String>) -> DeltaLogger {
        let config = LoggerConfig::new(
            "deltalakedb.errors".to_string(),
            log_level.unwrap_or_else(|| "ERROR".to_string()),
            "%(asctime)s - %(name)s - %(levelname)s - [ERROR:%(error_code)s] %(message)s".to_string(),
            vec!["console".to_string()],
            true,
        );

        DeltaLogger::new(
            Some(config),
            2500,
            log_file.is_some(),
            true,
            log_file,
        )
    }

    /// Setup Python logging configuration
    #[staticmethod]
    fn setup_python_logging(
        py: Python,
        level: Option<String>,
        format: Option<String>,
        log_file: Option<String>,
        enable_console: Option<bool>,
    ) -> PyResult<()> {
        let logging = py.import("logging")?;

        let log_level = level.unwrap_or_else(|| "INFO".to_string());
        let log_format = format.unwrap_or_else(||
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s".to_string()
        );

        // Configure basic logging
        let basic_config_kwargs = PyDict::new(py);
        basic_config_kwargs.set_item("level", logging.getattr(log_level.to_uppercase())?)?;
        basic_config_kwargs.set_item("format", log_format)?;
        basic_config_kwargs.set_item("force", true)?;

        let mut handlers = Vec::new();

        if enable_console.unwrap_or(true) {
            let stream_handler = logging.getattr("StreamHandler")?.call0()?;
            let formatter = logging.getattr("Formatter")?.call1((log_format,))?;
            stream_handler.call_method1("setFormatter", (formatter,))?;
            handlers.push(stream_handler);
        }

        if let Some(file_path) = log_file {
            let file_handler = logging.getattr("FileHandler")?.call1((file_path,))?;
            let formatter = logging.getattr("Formatter")?.call1((log_format,))?;
            file_handler.call_method1("setFormatter", (formatter,))?;
            handlers.push(file_handler);
        }

        basic_config_kwargs.set_item("handlers", handlers)?;

        logging.call_method("basicConfig", (), Some(basic_config_kwargs))?;

        Ok(())
    }

    /// Get current logging level
    #[staticmethod]
    fn get_logging_level(py: Python, logger_name: Option<String>) -> PyResult<String> {
        let logging = py.import("logging")?;
        let logger_name = logger_name.unwrap_or_else(|| "root".to_string());
        let logger = logging.call_method1("getLogger", (logger_name,))?;

        let level = logger.call_method0("getEffectiveLevel")?;
        let level_int = level.extract::<i32>()?;

        let level_str = match level_int {
            0..=9 => "NOTSET",
            10 => "DEBUG",
            20 => "INFO",
            30 => "WARNING",
            40 => "ERROR",
            50 => "CRITICAL",
            _ => "UNKNOWN",
        };

        Ok(level_str.to_string())
    }

    /// Set logging level
    #[staticmethod]
    fn set_logging_level(py: Python, level: String, logger_name: Option<String>) -> PyResult<()> {
        let logging = py.import("logging")?;
        let logger_name = logger_name.unwrap_or_else(|| "root".to_string());
        let logger = logging.call_method1("getLogger", (logger_name,))?;

        let level_obj = logging.getattr(level.to_uppercase())?;
        logger.call_method1("setLevel", (level_obj,))?;

        Ok(())
    }

    /// Test logging configuration
    #[staticmethod]
    fn test_logging(py: Python, logger_name: Option<String>) -> PyResult<()> {
        let logging = py.import("logging")?;
        let logger_name = logger_name.unwrap_or_else(|| "deltalakedb.test".to_string());
        let logger = logging.call_method1("getLogger", (logger_name,))?;

        logger.call_method1("debug", ("Test debug message",))?;
        logger.call_method1("info", ("Test info message",))?;
        logger.call_method1("warning", ("Test warning message",))?;
        logger.call_method1("error", ("Test error message",))?;
        logger.call_method1("critical", ("Test critical message",))?;

        println!("Test messages logged to: {}", logger_name);
        Ok(())
    }
}

/// Function to initialize logging module
pub fn initialize_logging_module(py: Python) -> PyResult<()> {
    // This function can be called to set up default logging configuration
    let logging_utils = LoggingUtils;
    logging_utils.setup_python_logging(
        py,
        Some("INFO".to_string()),
        Some("%(asctime)s - %(name)s - %(levelname)s - %(message)s".to_string()),
        None,
        Some(true),
    )?;

    Ok(())
}

/// Convenience functions for module-level exports
#[pyfunction]
pub fn setup_logging(
    py: Python,
    level: Option<String>,
    format: Option<String>,
    log_file: Option<String>,
    enable_console: Option<bool>,
) -> PyResult<()> {
    LoggingUtils::setup_python_logging(py, level, format, log_file, enable_console)
}

#[pyfunction]
pub fn create_logger(
    name: Option<String>,
    level: Option<String>,
    max_entries: Option<usize>,
    enable_file_logging: Option<bool>,
    log_file_path: Option<String>,
) -> DeltaLogger {
    let config = if let Some(logger_name) = name {
        Some(LoggerConfig::new(
            logger_name,
            level.unwrap_or_else(|| "INFO".to_string()),
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s".to_string(),
            vec!["console".to_string()],
            true,
        ))
    } else {
        None
    };

    DeltaLogger::new(
        config,
        max_entries.unwrap_or(10000),
        enable_file_logging.unwrap_or(false),
        true,
        log_file_path,
    )
}