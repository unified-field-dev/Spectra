//! Schema registry for compile-time registered Spectra event and metric metadata.

use std::collections::BTreeSet;
use std::sync::OnceLock;

use quark::inventory;

use crate::classification::FieldClassification;

/// Verbosity / sampling tier for a Spectra schema (more verbose = greater ordinal).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum SpectraLevel {
    /// Error-tier emits (always on).
    Error,
    /// Warning-tier emits (always on).
    Warn,
    /// Informational emits.
    #[default]
    Info,
    /// Debug-tier emits.
    Debug,
    /// Trace-tier emits.
    Trace,
}

impl SpectraLevel {
    /// Parse `SPECTRA_LEVEL` / DSL values (`error`, `warn`, `info`, `debug`, `trace`).
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "error" => Some(Self::Error),
            "warn" | "warning" => Some(Self::Warn),
            "info" => Some(Self::Info),
            "debug" => Some(Self::Debug),
            "trace" => Some(Self::Trace),
            _ => None,
        }
    }

    /// True for tiers that must never be level-gated or statistically sampled away.
    pub fn is_always_on(self) -> bool {
        matches!(self, Self::Error | Self::Warn)
    }
}

/// Event vs metric schema registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoggingKind {
    /// Structured event table schema.
    Event,
    /// Metric family schema.
    Metric,
}

/// Field metadata for registry / UI (macros populate this).
#[derive(Debug, Clone)]
pub struct SchemaFieldMetadata {
    /// Field name.
    pub name: String,
    /// Rust type name as registered.
    pub rust_type: String,
    /// GDPR-oriented classification metadata.
    pub classification: FieldClassification,
}

/// Runtime metadata for a Spectra schema (event table or metric family).
#[derive(Debug, Clone)]
pub struct SchemaMetadata {
    /// Table or metric name (registry key).
    pub table_or_metric: String,
    /// Logical store name from the schema DSL (`store:` field); used for registry grouping and routing.
    pub store: String,
    /// Schema version string.
    pub version: String,
    /// Optional human-readable description.
    pub description: Option<String>,
    /// Whether this schema is an event table or metric family.
    pub logging_kind: LoggingKind,
    /// Registered field metadata.
    pub fields: Vec<SchemaFieldMetadata>,
    /// Compile-time default verbosity tier (see [`SpectraLevel`]).
    pub default_level: SpectraLevel,
    /// `1.0` = always emit when level passes; `0.0` = never (except forced errors/warns).
    pub default_sample_rate: f64,
    /// Gauges only: minimum interval between emits (last-write-wins coalesce window).
    pub gauge_coalesce_ms: Option<u64>,
}

impl Default for SchemaMetadata {
    fn default() -> Self {
        Self {
            table_or_metric: String::new(),
            store: "default".to_string(),
            version: String::new(),
            description: None,
            logging_kind: LoggingKind::Metric,
            fields: Vec::new(),
            default_level: SpectraLevel::Info,
            default_sample_rate: 1.0,
            gauge_coalesce_ms: None,
        }
    }
}

impl SchemaMetadata {
    /// Returns the table or metric name.
    pub fn table_name(&self) -> &str {
        &self.table_or_metric
    }
}

impl quark::Registrable for SchemaMetadata {
    fn registry_key(&self) -> &str {
        &self.table_or_metric
    }
}

/// Function-pointer wrapper collected by `inventory`.
pub struct SchemaMetadataInit(pub fn() -> SchemaMetadata);

inventory::collect!(SchemaMetadataInit);

/// Registry of metric and event schema metadata linked into the current binary.
///
/// [`global`](Self::global) lazily discovers every `spectra_schema!` and `spectra_metric!`
/// registration submitted through `inventory`. Schema modules must be compiled into the binary
/// (linked with `mod`); declaring macros alone in unlinked files does not register them.
///
/// # Examples
///
/// ```
/// use spectra_core::SchemaRegistry;
///
/// let registry = SchemaRegistry::global();
/// for name in registry.list_schemas() {
///     let schema = registry.get_schema(name).expect("listed schema");
///     println!("{} uses store {}", schema.table_name(), schema.store);
/// }
///
/// // "default" is always available even when this binary declares no schemas.
/// assert!(registry.distinct_store_names().iter().any(|name| name == "default"));
/// ```
#[derive(Debug)]
pub struct SchemaRegistry {
    inner: quark::Registry<SchemaMetadata>,
}

impl SchemaRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self {
            inner: quark::Registry::new(),
        }
    }

    /// Populate from all `inventory::submit!`-ed [`SchemaMetadataInit`] items.
    pub fn auto_discover() -> Self {
        let mut registry = Self::new();
        for init in inventory::iter::<SchemaMetadataInit> {
            let metadata = (init.0)();
            registry.register(Box::leak(Box::new(metadata)));
        }
        registry
    }

    /// Installs a custom registry as the process-global instance (call once).
    ///
    /// Panics if called more than once. Prefer letting [`Self::global`] auto-discover
    /// schemas unless the host needs a custom registry.
    pub fn set_global(registry: SchemaRegistry) {
        // Process invariant: double install indicates a host boot bug.
        #[allow(clippy::expect_used)]
        GLOBAL_REGISTRY
            .set(registry)
            .expect("SchemaRegistry::set_global called more than once");
    }

    /// Returns the process-global registry, auto-discovering on first access.
    pub fn global() -> &'static Self {
        GLOBAL_REGISTRY.get_or_init(Self::auto_discover)
    }

    /// Registers a leaked schema metadata entry.
    pub fn register(&mut self, metadata: &'static SchemaMetadata) {
        self.inner.register(metadata);
    }

    /// Looks up schema metadata by table or metric name.
    pub fn get_schema(&self, table_or_metric: &str) -> Option<&'static SchemaMetadata> {
        self.inner.get(table_or_metric)
    }

    /// Lists all registered table and metric names.
    pub fn list_schemas(&self) -> Vec<&str> {
        self.inner.list()
    }

    /// Returns whether a schema is registered.
    pub fn has_schema(&self, table_or_metric: &str) -> bool {
        self.inner.get(table_or_metric).is_some()
    }

    /// Distinct `store` names from all registered schemas, always including `"default"`.
    pub fn distinct_store_names(&self) -> Vec<String> {
        let mut names: BTreeSet<String> = BTreeSet::new();
        names.insert("default".to_string());
        for name in self.list_schemas() {
            if let Some(meta) = self.get_schema(name) {
                if !meta.store.is_empty() {
                    names.insert(meta.store.clone());
                }
            }
        }
        names.into_iter().collect()
    }
}

static GLOBAL_REGISTRY: OnceLock<SchemaRegistry> = OnceLock::new();

/// Distinct Spectra store names from schema inventory (always includes `"default"`).
pub fn collect_distinct_spectra_store_names() -> Vec<String> {
    SchemaRegistry::global().distinct_store_names()
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl std::ops::Deref for SchemaRegistry {
    type Target = quark::Registry<SchemaMetadata>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_empty_when_no_submissions_in_test_crate() {
        let reg = SchemaRegistry::new();
        assert_eq!(reg.list_schemas(), Vec::<&str>::new());
    }
}
