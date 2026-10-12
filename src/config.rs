use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

/// Top-level configuration, loaded from a YAML file.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub mqtt: MqttConfig,
    #[serde(default)]
    pub discovery: DiscoveryConfig,
    #[serde(default)]
    pub socket: SocketConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MqttConfig {
    pub host: String,
    #[serde(default = "default_mqtt_port")]
    pub port: u16,
    #[serde(default = "default_client_id")]
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<String>,
    #[serde(default = "default_keep_alive")]
    pub keep_alive_secs: u64,
    /// Root of the topic tree, e.g. `pdk-board`.
    #[serde(default = "default_topic_prefix")]
    pub topic_prefix: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DiscoveryConfig {
    /// mDNS service type to browse for.
    #[serde(default = "default_service_type")]
    pub service_type: String,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            service_type: default_service_type(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SocketConfig {
    /// Terminator appended to each command written to the device socket.
    ///
    /// Escape sequences are interpreted (`\n`, `\r`, `\t`, `\0`, `\\`), so you
    /// can write `"\r\n"` for CRLF or `""` to append nothing. Defaults to `\n`.
    #[serde(default = "default_terminator")]
    pub command_terminator: String,

    /// Delimiter separating messages read back from the device socket. Each
    /// frame between delimiters is published as one MQTT response.
    ///
    /// Escape sequences are interpreted (`\n`, `\r`, `\t`, `\0`, `\\`). Use
    /// `"\r\n"` for CRLF or `""` to disable framing (publish each raw read
    /// chunk as it arrives). Defaults to `\n`.
    #[serde(default = "default_delimiter")]
    pub response_delimiter: String,
}

impl Default for SocketConfig {
    fn default() -> Self {
        Self {
            command_terminator: default_terminator(),
            response_delimiter: default_delimiter(),
        }
    }
}

impl SocketConfig {
    /// Decode the configured terminator into the raw bytes to append.
    pub fn terminator_bytes(&self) -> Vec<u8> {
        decode_escapes(&self.command_terminator)
    }

    /// Decode the configured response delimiter into the raw bytes to split on.
    pub fn delimiter_bytes(&self) -> Vec<u8> {
        decode_escapes(&self.response_delimiter)
    }
}

/// Decode backslash escapes (`\n`, `\r`, `\t`, `\0`, `\\`) into raw bytes.
/// Unknown escapes are passed through verbatim (backslash kept).
fn decode_escapes(s: &str) -> Vec<u8> {
    let mut out = Vec::new();
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            let mut buf = [0u8; 4];
            out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            continue;
        }
        match chars.next() {
            Some('n') => out.push(b'\n'),
            Some('r') => out.push(b'\r'),
            Some('t') => out.push(b'\t'),
            Some('0') => out.push(0),
            Some('\\') => out.push(b'\\'),
            // Unknown escape: keep the backslash and the following char as-is.
            Some(other) => {
                out.push(b'\\');
                let mut buf = [0u8; 4];
                out.extend_from_slice(other.encode_utf8(&mut buf).as_bytes());
            }
            None => out.push(b'\\'),
        }
    }
    out
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file {}", path.display()))?;
        let cfg: Config = serde_yaml::from_str(&raw)
            .with_context(|| format!("parsing YAML config {}", path.display()))?;
        Ok(cfg)
    }
}

fn default_mqtt_port() -> u16 {
    1883
}
fn default_client_id() -> String {
    "rust-mqtt-adapter".to_string()
}
fn default_keep_alive() -> u64 {
    30
}
fn default_topic_prefix() -> String {
    "pdk-board".to_string()
}
fn default_service_type() -> String {
    "_pdk-board._tcp.local.".to_string()
}
fn default_terminator() -> String {
    "\\n".to_string()
}
fn default_delimiter() -> String {
    "\\n".to_string()
}
