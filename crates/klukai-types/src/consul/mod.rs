use core::fmt;
use std::{
    collections::{BTreeMap, HashMap},
    fmt::Display,
    fs::OpenOptions,
    io::BufReader,
};

use bytes::Bytes;
use http_body_util::Empty;
use hyper::http::uri::InvalidUri;
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{client::legacy::Client as HyperClient, rt::TokioExecutor};
use rustls::RootCertStore;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_with::{NoneAsEmptyString, serde_as};

use rusqlite::types::{FromSql, FromSqlError, ValueRef};

pub mod config;
pub use config::Config;

pub type ConsulResult<T> = std::result::Result<T, Error>;

#[derive(Clone)]
pub struct Client {
    client: HyperClient<
        HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
        Empty<Bytes>,
    >,
    addr: String,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").field("addr", &self.addr).finish()
    }
}

impl Client {
    pub fn new(config: Config) -> ConsulResult<Self> {
        let scheme = if config.tls.is_some() {
            "https"
        } else {
            "http"
        };
        let ctor = if let Some(tls) = config.tls {
            // HTTPS path
            let mut root_store = RootCertStore::empty();
            let mut cacert_file = BufReader::new(
                OpenOptions::new()
                    .read(true)
                    .open(&tls.ca_file)
                    .map_err(Error::TlsSetup)?,
            );
            for cacert in rustls_pemfile::certs(&mut cacert_file) {
                let cacert = cacert.map_err(Error::TlsSetup)?;
                root_store.add(cacert)?;
            }

            let mut cert_file = BufReader::new(
                OpenOptions::new()
                    .read(true)
                    .open(&tls.cert_file)
                    .map_err(Error::TlsSetup)?,
            );
            let certs: Result<Vec<CertificateDer>, _> =
                rustls_pemfile::certs(&mut cert_file).collect();
            let certs = certs.map_err(Error::TlsSetup)?;

            let mut key_file = BufReader::new(
                OpenOptions::new()
                    .read(true)
                    .open(&tls.key_file)
                    .map_err(Error::TlsSetup)?,
            );
            let keys: Result<Vec<_>, _> =
                rustls_pemfile::pkcs8_private_keys(&mut key_file).collect();
            let mut keys = keys.map_err(Error::TlsSetup)?;
            let key = PrivateKeyDer::Pkcs8(keys.remove(0));

            let tls_config = rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_client_auth_cert(certs, key)
                .map_err(|e| {
                    Error::TlsSetup(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
                })?;

            HttpsConnectorBuilder::new()
                .with_tls_config(tls_config)
                .https_or_http()
                .enable_http2()
                .build()
        } else {
            // this is always gonna be HTTP, but we still need to build this, annoyingly.
            // TODO: build custom connector so we don't have to do this
            HttpsConnectorBuilder::new()
                .with_native_roots()?
                .https_or_http()
                .enable_http1()
                .build()
        };

        Ok(Self {
            client: HyperClient::builder(TokioExecutor::new()).build(ctor),
            addr: format!("{}://{}", scheme, config.address),
        })
    }

    pub async fn agent_services(&self) -> ConsulResult<HashMap<String, AgentService>> {
        self.request("/v1/agent/services").await
    }

    pub async fn agent_checks(&self) -> ConsulResult<HashMap<String, AgentCheck>> {
        self.request("/v1/agent/checks").await
    }

    async fn request<P: Display, T: DeserializeOwned>(&self, path: P) -> ConsulResult<T> {
        let res = match self
            .client
            .get(format!("{}{}", &self.addr, &path).parse()?)
            .await
        {
            Ok(res) => res,
            Err(e) => {
                return Err(e.into());
            }
        };

        if res.status() != hyper::StatusCode::OK {
            return Err(Error::BadStatusCode(res.status()));
        }

        use http_body_util::BodyExt;
        let bytes = res.collect().await?.to_bytes();

        Ok(serde_json::from_slice(&bytes)?)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Hyper(#[from] hyper::Error),
    #[error("bad status code: {0}")]
    BadStatusCode(hyper::StatusCode),
    #[error(transparent)]
    InvalidUri(#[from] InvalidUri),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error("tls setup: {0}")]
    TlsSetup(std::io::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    HyperUtil(#[from] hyper_util::client::legacy::Error),
    #[error(transparent)]
    Rustls(#[from] rustls::Error),
    #[error(transparent)]
    Webpki(#[from] webpki::Error),
}

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
#[serde(rename_all(deserialize = "PascalCase"))]
pub struct AgentService {
    #[serde(rename(deserialize = "ID"))]
    pub id: String,
    #[serde(rename(deserialize = "Service"))]
    pub name: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub meta: BTreeMap<String, String>,
    pub port: u16,
    pub address: String,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all(deserialize = "PascalCase"))]
pub struct AgentCheck {
    #[serde(rename(deserialize = "CheckID"))]
    pub id: String,
    pub name: String,
    pub status: ConsulCheckStatus,
    pub output: String,
    #[serde(rename(deserialize = "ServiceID"))]
    pub service_id: String,
    pub service_name: String,
    #[serde_as(as = "NoneAsEmptyString")]
    pub notes: Option<String>,
}

#[derive(Debug, Copy, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum ConsulCheckStatus {
    #[default]
    Passing,
    Warning,
    Critical,
}

impl ConsulCheckStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ConsulCheckStatus::Passing => "passing",
            ConsulCheckStatus::Warning => "warning",
            ConsulCheckStatus::Critical => "critical",
        }
    }
}

impl FromSql for ConsulCheckStatus {
    fn column_result(value: ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        match value {
            ValueRef::Text(s) => Ok(match String::from_utf8_lossy(s).as_ref() {
                "passing" => Self::Passing,
                "warning" => Self::Warning,
                "critical" => Self::Critical,
                _ => {
                    return Err(FromSqlError::InvalidType);
                }
            }),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl fmt::Display for ConsulCheckStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}
