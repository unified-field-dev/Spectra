//! ClickHouse-protocol HTTP and native TCP client wrapper.

use std::path::{Path, PathBuf};

use clickhouse::Client as HttpClient;
use spectra_core::{Error, Result};

use crate::remote_security::{RemoteTransportSecurity, RemoteUrlKind};

/// Redact `userinfo` (credentials) from URLs embedded in error or log text.
///
/// Replaces `scheme://user:pass@host` with `scheme://***@host`. Non-URL strings are
/// returned unchanged except that substrings matching that pattern are scrubbed.
///
/// # Examples
///
/// ```
/// use spectra_backend_remote_common::redact_url_credentials;
///
/// let redacted = redact_url_credentials("http://user:s3cret@localhost:8123/db");
/// assert!(!redacted.contains("s3cret"));
/// assert!(redacted.contains("***@"));
/// ```
#[must_use]
pub fn redact_url_credentials(input: &str) -> String {
    // Scrub `://user:password@` (password may contain URL-encoded chars except '@').
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if let Some(rel) = find_userinfo_start(&input[i..]) {
            let abs = i + rel;
            out.push_str(&input[i..abs]);
            // abs points at first char of userinfo; find '@'
            if let Some(at_rel) = input[abs..].find('@') {
                out.push_str("***@");
                i = abs + at_rel + 1;
                continue;
            }
        }
        out.push(input[i..].chars().next().unwrap_or('\0'));
        i += input[i..].chars().next().map(|c| c.len_utf8()).unwrap_or(1);
    }
    out
}

fn find_userinfo_start(s: &str) -> Option<usize> {
    let mut search_from = 0;
    while let Some(scheme_rel) = s[search_from..].find("://") {
        let after_scheme = search_from + scheme_rel + 3;
        let rest = &s[after_scheme..];
        if let Some(at) = rest.find('@') {
            let userinfo = &rest[..at];
            if userinfo.contains(':') && !userinfo.contains('/') {
                return Some(after_scheme);
            }
        }
        search_from = after_scheme;
    }
    None
}

pub(crate) fn map_remote(e: impl std::error::Error + Send + Sync + 'static) -> Error {
    let message = redact_url_credentials(&e.to_string());
    Error::storage_source(message, e)
}

/// Shared client for remote ClickHouse-compatible storage engines.
#[derive(Clone)]
pub struct RemoteClient {
    inner: ClientInner,
}

#[derive(Clone)]
#[allow(clippy::large_enum_variant)] // Http client is large; Native is the small arm
enum ClientInner {
    Http(HttpClient),
    Native(NativeEndpoint),
}

#[derive(Clone)]
struct NativeEndpoint {
    host: String,
    port: u16,
    cli: PathBuf,
    /// When true, pass `--secure` to `clickhouse-client` (native TLS).
    secure: bool,
}

/// Streaming insert handle (HTTP RowBinary or native SQL insert).
pub struct RemoteInsert<T> {
    inner: InsertInner<T>,
}

enum InsertInner<T> {
    Http(clickhouse::insert::Insert<T>),
    Native {
        endpoint: NativeEndpoint,
        table: &'static str,
        rows: Vec<T>,
    },
}

impl RemoteClient {
    /// Connect to a remote engine (`https://` / `http://`, or `tcp+tls://` / `tcp://`).
    ///
    /// Plaintext schemes (`http://`, `tcp://`) require [`RemoteTransportSecurity::AllowInsecurePlaintext`]
    /// via `SPECTRA_ALLOW_INSECURE_REMOTE=1`. Prefer `https://` or `tcp+tls://` in production.
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with_security(url, RemoteTransportSecurity::from_env()).await
    }

    /// Connect with an explicit [`RemoteTransportSecurity`] policy (tests and custom hosts).
    #[tracing::instrument(name = "spectra.remote.connect", skip_all)]
    pub async fn connect_with_security(
        url: &str,
        security: RemoteTransportSecurity,
    ) -> Result<Self> {
        security.check_url(url)?;
        match RemoteUrlKind::parse(url)? {
            (RemoteUrlKind::Native { secure }, addr) => {
                let (host, port) = parse_host_port(addr)?;
                let cli = resolve_clickhouse_client()?;
                Ok(Self {
                    inner: ClientInner::Native(NativeEndpoint {
                        host,
                        port,
                        cli,
                        secure,
                    }),
                })
            }
            (RemoteUrlKind::Http, http_url) => {
                // `clickhouse::Client::with_url` does not apply URL userinfo; set
                // credentials explicitly when present (MVE: http://user:pass@host:8123).
                let mut client = HttpClient::default();
                if let Ok(parsed) = url::Url::parse(http_url) {
                    let mut base = parsed.clone();
                    let _ = base.set_username("");
                    let _ = base.set_password(None);
                    client = client.with_url(base.as_str());
                    if !parsed.username().is_empty() {
                        client = client.with_user(parsed.username());
                    }
                    if let Some(password) = parsed.password() {
                        client = client.with_password(password);
                    }
                } else {
                    client = client.with_url(http_url);
                }
                Ok(Self {
                    inner: ClientInner::Http(client),
                })
            }
        }
    }

    /// Execute DDL or administrative SQL.
    #[tracing::instrument(name = "spectra.remote.execute", skip_all, fields(sql_len = sql.len()))]
    pub async fn execute(&self, sql: &str) -> Result<()> {
        match &self.inner {
            ClientInner::Http(client) => client.query(sql).execute().await.map_err(map_remote),
            ClientInner::Native(endpoint) => run_native_execute(endpoint, sql).await,
        }
    }

    /// Query three string columns (legacy helper for tests).
    #[tracing::instrument(name = "spectra.remote.query_strings", skip_all, fields(sql_len = sql.len()))]
    pub async fn query_strings(&self, sql: &str) -> Result<Vec<(String, String, String)>> {
        match &self.inner {
            ClientInner::Http(client) => {
                #[derive(clickhouse::Row, serde::Deserialize)]
                struct Row3 {
                    c0: String,
                    c1: String,
                    c2: String,
                }
                let rows = client
                    .query(sql)
                    .fetch_all::<Row3>()
                    .await
                    .map_err(map_remote)?;
                Ok(rows.into_iter().map(|r| (r.c0, r.c1, r.c2)).collect())
            }
            ClientInner::Native(endpoint) => {
                let lines = run_native_select(endpoint, sql).await?;
                Ok(lines
                    .into_iter()
                    .map(|cols| {
                        (
                            cols.first().cloned().unwrap_or_default(),
                            cols.get(1).cloned().unwrap_or_default(),
                            cols.get(2).cloned().unwrap_or_default(),
                        )
                    })
                    .collect())
            }
        }
    }

    /// Fetch metric rows `(value, labels_json, ts)`.
    #[tracing::instrument(name = "spectra.remote.query_metric_rows", skip_all, fields(sql_len = sql.len()))]
    pub async fn query_metric_rows(&self, sql: &str) -> Result<Vec<(f64, String, String)>> {
        match &self.inner {
            ClientInner::Http(client) => {
                #[derive(clickhouse::Row, serde::Deserialize)]
                struct MetricRow {
                    value: f64,
                    labels: String,
                    ts: String,
                }
                let rows = client
                    .query(sql)
                    .fetch_all::<MetricRow>()
                    .await
                    .map_err(map_remote)?;
                Ok(rows
                    .into_iter()
                    .map(|r| (r.value, r.labels, r.ts))
                    .collect())
            }
            ClientInner::Native(endpoint) => {
                let lines = run_native_select(endpoint, sql).await?;
                let mut out = Vec::new();
                for cols in lines {
                    if cols.len() < 3 {
                        continue;
                    }
                    let value = cols[0].parse::<f64>().map_err(map_remote)?;
                    out.push((value, cols[1].clone(), cols[2].clone()));
                }
                Ok(out)
            }
        }
    }

    /// Fetch event rows `(fields_json, ts)`.
    #[tracing::instrument(name = "spectra.remote.query_event_rows", skip_all, fields(sql_len = sql.len()))]
    pub async fn query_event_rows(&self, sql: &str) -> Result<Vec<(String, String)>> {
        match &self.inner {
            ClientInner::Http(client) => {
                #[derive(clickhouse::Row, serde::Deserialize)]
                struct EventRow {
                    fields: String,
                    ts: String,
                }
                let rows = client
                    .query(sql)
                    .fetch_all::<EventRow>()
                    .await
                    .map_err(map_remote)?;
                Ok(rows.into_iter().map(|r| (r.fields, r.ts)).collect())
            }
            ClientInner::Native(endpoint) => {
                let lines = run_native_select(endpoint, sql).await?;
                Ok(lines
                    .into_iter()
                    .filter_map(|cols| {
                        if cols.len() < 2 {
                            return None;
                        }
                        Some((cols[0].clone(), cols[1].clone()))
                    })
                    .collect())
            }
        }
    }

    /// Begin a streaming insert into `spectra_metrics`.
    pub async fn insert_metrics(&self) -> Result<RemoteInsert<MetricInsertRow>> {
        match &self.inner {
            ClientInner::Http(client) => Ok(RemoteInsert {
                inner: InsertInner::Http(
                    client.insert("spectra_metrics").await.map_err(map_remote)?,
                ),
            }),
            ClientInner::Native(endpoint) => Ok(RemoteInsert {
                inner: InsertInner::Native {
                    endpoint: endpoint.clone(),
                    table: "spectra_metrics",
                    rows: Vec::new(),
                },
            }),
        }
    }

    /// Begin a streaming insert into `spectra_events`.
    pub async fn insert_events(&self) -> Result<RemoteInsert<EventInsertRow>> {
        match &self.inner {
            ClientInner::Http(client) => Ok(RemoteInsert {
                inner: InsertInner::Http(
                    client.insert("spectra_events").await.map_err(map_remote)?,
                ),
            }),
            ClientInner::Native(endpoint) => Ok(RemoteInsert {
                inner: InsertInner::Native {
                    endpoint: endpoint.clone(),
                    table: "spectra_events",
                    rows: Vec::new(),
                },
            }),
        }
    }
}

#[allow(private_bounds)] // InsertSqlRow is an internal row helper, not part of the public API
impl<T> RemoteInsert<T>
where
    T: clickhouse::RowOwned + clickhouse::RowWrite + Clone + Send + Sync + 'static + InsertSqlRow,
{
    /// Append one row to the insert stream.
    pub async fn write(&mut self, row: &T) -> Result<()> {
        match &mut self.inner {
            InsertInner::Http(insert) => insert.write(row).await.map_err(map_remote),
            InsertInner::Native { rows, .. } => {
                rows.push(row.clone());
                Ok(())
            }
        }
    }

    /// Finish the insert stream.
    pub async fn end(self) -> Result<()> {
        match self.inner {
            InsertInner::Http(insert) => insert.end().await.map_err(map_remote),
            InsertInner::Native {
                endpoint,
                table,
                rows,
            } => {
                if rows.is_empty() {
                    return Ok(());
                }
                let values = rows
                    .iter()
                    .map(|row| row.insert_values_sql())
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!("INSERT INTO {table} VALUES {values}");
                run_native_execute(&endpoint, &sql).await
            }
        }
    }
}

/// Row shape for metric inserts.
#[derive(clickhouse::Row, serde::Serialize, Clone)]
pub struct MetricInsertRow {
    /// Metric name.
    pub name: String,
    /// `counter` or `gauge`.
    pub kind: String,
    /// Numeric value.
    pub value: f64,
    /// JSON-encoded labels.
    pub labels: String,
    /// RFC3339 timestamp string.
    pub ts: String,
    /// Optional correlation identifier.
    pub correlation_id: Option<String>,
}

/// Row shape for event inserts.
#[derive(clickhouse::Row, serde::Serialize, Clone)]
pub struct EventInsertRow {
    /// Logical event table name.
    pub table_name: String,
    /// JSON-encoded fields.
    pub fields: String,
    /// RFC3339 timestamp string.
    pub ts: String,
    /// Optional correlation identifier.
    pub correlation_id: Option<String>,
}

/// Format a UTC timestamp for remote storage.
pub fn datetime_to_ch_ts(ts: chrono::DateTime<chrono::Utc>) -> String {
    ts.to_rfc3339()
}

/// Parse an RFC3339 timestamp from remote storage.
pub fn parse_rfc3339_ts(s: &str) -> Result<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map_err(|e| Error::internal(format!("invalid remote timestamp: {e}")))
}

fn parse_host_port(addr: &str) -> Result<(String, u16)> {
    let (host, port) = if let Some((host, port)) = addr.rsplit_once(':') {
        (
            host.to_string(),
            port.parse::<u16>()
                .map_err(|e| Error::config(format!("invalid native port in remote URL: {e}")))?,
        )
    } else {
        (addr.to_string(), 9528)
    };
    Ok((host, port))
}

fn native_command(endpoint: &NativeEndpoint) -> tokio::process::Command {
    let mut cmd = tokio::process::Command::new(&endpoint.cli);
    cmd.arg("--host").arg(&endpoint.host);
    cmd.arg("--port").arg(endpoint.port.to_string());
    if endpoint.secure {
        cmd.arg("--secure");
    }
    cmd
}

fn sql_quote(s: &str) -> String {
    // Insert path is trusted in-process data; still strip NUL to avoid truncated literals.
    let s = s.replace('\0', "");
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

trait InsertSqlRow {
    fn insert_values_sql(&self) -> String;
}

impl InsertSqlRow for MetricInsertRow {
    fn insert_values_sql(&self) -> String {
        let correlation = match &self.correlation_id {
            Some(id) => sql_quote(id),
            None => "NULL".to_string(),
        };
        format!(
            "({name}, {kind}, {value}, {labels}, {ts}, {correlation})",
            name = sql_quote(&self.name),
            kind = sql_quote(&self.kind),
            value = self.value,
            labels = sql_quote(&self.labels),
            ts = sql_quote(&self.ts),
            correlation = correlation,
        )
    }
}

impl InsertSqlRow for EventInsertRow {
    fn insert_values_sql(&self) -> String {
        let correlation = match &self.correlation_id {
            Some(id) => sql_quote(id),
            None => "NULL".to_string(),
        };
        format!(
            "({table_name}, {fields}, {ts}, {correlation})",
            table_name = sql_quote(&self.table_name),
            fields = sql_quote(&self.fields),
            ts = sql_quote(&self.ts),
            correlation = correlation,
        )
    }
}

fn resolve_clickhouse_client() -> Result<PathBuf> {
    if let Ok(path) = std::env::var("SPECTRA_CLICKHOUSE_CLIENT_PATH") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Ok(path);
        }
    }
    if let Ok(home) = std::env::var("HOME") {
        let bundled = Path::new(&home).join("tensorbase-smoke/clickhouse-client");
        if bundled.is_file() {
            return Ok(bundled);
        }
    }
    if let Ok(path) = which_client("clickhouse-client") {
        return Ok(path);
    }
    Err(Error::config(
        "native tcp:// / tcp+tls:// URLs require clickhouse-client \
         (set SPECTRA_CLICKHOUSE_CLIENT_PATH or install in PATH)",
    ))
}

fn which_client(name: &str) -> Result<PathBuf> {
    let output = std::process::Command::new("which")
        .arg(name)
        .output()
        .map_err(Error::Io)?;
    if !output.status.success() {
        return Err(Error::config(format!("{name} not found")));
    }
    let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Ok(PathBuf::from(path))
}

async fn run_native_execute(endpoint: &NativeEndpoint, sql: &str) -> Result<()> {
    let output = native_command(endpoint)
        .arg("--query")
        .arg(sql)
        .output()
        .await
        .map_err(Error::Io)?;
    if !output.status.success() {
        return Err(Error::storage(format!(
            "clickhouse-client execute failed: {}",
            redact_url_credentials(&String::from_utf8_lossy(&output.stderr))
        )));
    }
    Ok(())
}

async fn run_native_select(endpoint: &NativeEndpoint, sql: &str) -> Result<Vec<Vec<String>>> {
    let output = native_command(endpoint)
        .arg("--query")
        .arg(sql)
        .output()
        .await
        .map_err(Error::Io)?;
    if !output.status.success() {
        return Err(Error::storage(format!(
            "clickhouse-client query failed: {}",
            redact_url_credentials(&String::from_utf8_lossy(&output.stderr))
        )));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| line.split('\t').map(str::to_string).collect())
        .collect())
}

#[cfg(test)]
mod redact_tests {
    use super::redact_url_credentials;

    #[test]
    fn redacts_password_from_http_url() {
        let out = redact_url_credentials("http://user:s3cret@localhost:8123/db");
        assert!(!out.contains("s3cret"));
        assert!(!out.contains("user:"));
        assert!(out.contains("***@localhost:8123"));
    }

    #[test]
    fn leaves_url_without_userinfo() {
        let url = "http://localhost:8123/";
        assert_eq!(redact_url_credentials(url), url);
    }

    #[test]
    fn redacts_inside_longer_error_message() {
        let out = redact_url_credentials(
            "storage error: failed to connect to https://admin:hunter2@db.example:8443/x",
        );
        assert!(!out.contains("hunter2"));
        assert!(out.contains("***@db.example"));
    }
}

#[cfg(test)]
mod connect_security_tests {
    use super::RemoteClient;
    use crate::remote_security::{RemoteTransportSecurity, ALLOW_INSECURE_REMOTE_ENV};

    #[tokio::test]
    async fn connect_accepts_https_under_require_tls() {
        // HTTPS is TLS-oriented; client construction does not dial until execute/query.
        RemoteClient::connect_with_security(
            "https://clickhouse.example:8443",
            RemoteTransportSecurity::RequireTls,
        )
        .await
        .expect("https connect constructs client");
    }

    #[tokio::test]
    async fn connect_rejects_http_under_require_tls() {
        let result = RemoteClient::connect_with_security(
            "http://127.0.0.1:8123",
            RemoteTransportSecurity::RequireTls,
        )
        .await;
        let err = match result {
            Ok(_) => panic!("plaintext rejected"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("plaintext"));
        assert!(err.to_string().contains(ALLOW_INSECURE_REMOTE_ENV));
    }

    #[tokio::test]
    async fn connect_allows_http_when_insecure_allowed() {
        RemoteClient::connect_with_security(
            "http://127.0.0.1:8123",
            RemoteTransportSecurity::AllowInsecurePlaintext,
        )
        .await
        .expect("http allowed with AllowInsecurePlaintext");
    }
}
