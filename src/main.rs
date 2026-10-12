mod config;

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr, SocketAddrV6};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use mdns_sd::{ServiceDaemon, ServiceEvent};
use rumqttc::{AsyncClient, Event, LastWill, MqttOptions, Packet, QoS};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch, Mutex};
use tracing::{debug, error, info, warn};

use crate::config::Config;

/// Channel buffer for commands queued toward a single device's socket.
const DEVICE_CMD_BUFFER: usize = 64;
/// Backoff between TCP reconnect attempts.
const RECONNECT_DELAY: Duration = Duration::from_secs(5);
/// Bounds for the exponential backoff between MQTT broker reconnect attempts.
const MQTT_BACKOFF_MIN: Duration = Duration::from_secs(1);
const MQTT_BACKOFF_MAX: Duration = Duration::from_secs(30);
/// How long to wait for the first broker connection before failing startup.
const MQTT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Registry mapping device MAC -> command sender feeding that device's socket task.
type Registry = Arc<Mutex<HashMap<String, mpsc::Sender<Vec<u8>>>>>;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.yaml".to_string());
    let cfg = Arc::new(Config::load(&config_path)?);
    info!(path = %config_path, "loaded configuration");

    // --- MQTT setup ---
    let adapter_status_topic = format!("{}/adapter/status", cfg.mqtt.topic_prefix);
    let mut opts = MqttOptions::new(
        cfg.mqtt.client_id.clone(),
        cfg.mqtt.host.clone(),
        cfg.mqtt.port,
    );
    opts.set_keep_alive(Duration::from_secs(cfg.mqtt.keep_alive_secs));
    opts.set_last_will(LastWill::new(
        &adapter_status_topic,
        "offline",
        QoS::AtLeastOnce,
        true,
    ));
    if let Some(user) = &cfg.mqtt.username {
        opts.set_credentials(user.clone(), cfg.mqtt.password.clone().unwrap_or_default());
    }
    let (client, mut eventloop) = AsyncClient::new(opts, 64);

    // Subscribe to the wildcard command topic; per-device routing is by MAC in the topic.
    let cmd_filter = format!("{}/+/cmd", cfg.mqtt.topic_prefix);

    // Fail fast: the broker must accept our connection before we start anything else.
    let broker = format!("{}:{}", cfg.mqtt.host, cfg.mqtt.port);
    tokio::time::timeout(MQTT_CONNECT_TIMEOUT, async {
        loop {
            if let Event::Incoming(Packet::ConnAck(_)) = eventloop.poll().await? {
                return Ok::<_, rumqttc::ConnectionError>(());
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out after {MQTT_CONNECT_TIMEOUT:?}"))
    .and_then(|r| r.map_err(Into::into))
    .with_context(|| format!("connecting to MQTT broker {broker}"))?;
    on_mqtt_connected(&client, &cmd_filter, &adapter_status_topic).await?;
    info!(%broker, "connected to MQTT broker");

    // Bumped on every (re)connect so device bridges can re-publish retained status.
    let (conn_tx, conn_rx) = watch::channel(0u64);

    let registry: Registry = Arc::new(Mutex::new(HashMap::new()));

    // --- mDNS discovery task ---
    {
        let cfg = cfg.clone();
        let client = client.clone();
        let registry = registry.clone();
        tokio::spawn(async move {
            if let Err(e) = run_discovery(cfg, client, registry, conn_rx).await {
                error!(error = %e, "discovery task exited");
            }
        });
    }

    // --- MQTT event loop: route incoming command publishes to device sockets ---
    let prefix = cfg.mqtt.topic_prefix.clone();
    let mut backoff = MQTT_BACKOFF_MIN;
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                // The broker may have dropped our session: always resubscribe.
                backoff = MQTT_BACKOFF_MIN;
                info!(%broker, "reconnected to MQTT broker");
                if let Err(e) = on_mqtt_connected(&client, &cmd_filter, &adapter_status_topic).await
                {
                    error!(error = %e, "failed to restore MQTT state after reconnect");
                }
                conn_tx.send_modify(|n| *n += 1);
            }
            Ok(Event::Incoming(Packet::Publish(p))) => {
                info!(topic = p.topic, "received an mqtt msg");
                if let Some(mac) = mac_from_cmd_topic(&prefix, &p.topic) {
                    info!(topic = p.topic, "routing command to topic");
                    route_command(&registry, mac, p.payload.to_vec()).await;
                } else {
                    debug!(topic = %p.topic, "ignoring publish on unrecognized topic");
                }
            }
            Ok(_) => {}
            Err(e) => {
                error!(error = %e, retry_in = ?backoff, "MQTT connection error; retrying");
                tokio::time::sleep(backoff).await;
                backoff = next_backoff(backoff);
            }
        }
    }
}

/// Double the backoff, capped at `MQTT_BACKOFF_MAX`.
fn next_backoff(current: Duration) -> Duration {
    (current * 2).min(MQTT_BACKOFF_MAX)
}

/// Per-connection MQTT state: (re)subscribe to commands and announce the adapter online.
async fn on_mqtt_connected(
    client: &AsyncClient,
    cmd_filter: &str,
    status_topic: &str,
) -> Result<()> {
    client
        .try_subscribe(cmd_filter, QoS::AtLeastOnce)
        .with_context(|| format!("subscribing to {cmd_filter}"))?;
    info!(topic = %cmd_filter, "subscribed to command topic");
    publish_status(client, status_topic, "online").await;
    Ok(())
}

/// Extract the MAC segment from a `{prefix}/{mac}/cmd` topic, if it matches.
fn mac_from_cmd_topic<'a>(prefix: &str, topic: &'a str) -> Option<&'a str> {
    let rest = topic.strip_prefix(prefix)?.strip_prefix('/')?;
    let mac = rest.strip_suffix("/cmd")?;
    if mac.is_empty() || mac.contains('/') {
        warn!("mac is empty");
        return None;
    }
    Some(mac)
}

/// Forward a command payload to the matching device's socket task.
async fn route_command(registry: &Registry, mac: &str, payload: Vec<u8>) {
    let (sender, known) = {
        let reg = registry.lock().await;
        (
            reg.get(mac).cloned(),
            reg.keys().cloned().collect::<Vec<_>>(),
        )
    };
    match sender {
        Some(tx) => {
            let bytes = payload.len();
            if let Err(e) = tx.send(payload).await {
                warn!(mac, error = %e, "device command channel closed");
            } else {
                info!(mac, bytes, "queued command to device socket");
            }
        }
        None => warn!(
            mac,
            known = ?known,
            "no device registered for this topic MAC; check the MAC segment matches a known device"
        ),
    }
}

/// Browse mDNS and spawn a per-device bridge task for each newly resolved board.
async fn run_discovery(
    cfg: Arc<Config>,
    client: AsyncClient,
    registry: Registry,
    conn_rx: watch::Receiver<u64>,
) -> Result<()> {
    let daemon = ServiceDaemon::new().context("creating mDNS daemon")?;
    let receiver = daemon
        .browse(&cfg.discovery.service_type)
        .with_context(|| format!("browsing for {}", cfg.discovery.service_type))?;
    info!(service = %cfg.discovery.service_type, "started mDNS browse");

    while let Ok(event) = receiver.recv_async().await {
        match event {
            ServiceEvent::ServiceResolved(info) => {
                let hostname = info.get_hostname();
                let mac = match extract_mac(hostname) {
                    Some(mac) => mac,
                    None => {
                        warn!(
                            hostname,
                            "could not extract MAC from hostname; skipping device"
                        );
                        continue;
                    }
                };

                let addr = match select_socket_addr(info.get_addresses(), info.get_port()) {
                    Some(addr) => addr,
                    None => {
                        warn!(mac, "resolved device has no usable address; skipping");
                        continue;
                    }
                };

                let mut reg = registry.lock().await;
                if reg.contains_key(&mac) {
                    debug!(mac, %addr, "device already tracked");
                    continue;
                }

                let (tx, rx) = mpsc::channel::<Vec<u8>>(DEVICE_CMD_BUFFER);
                reg.insert(mac.clone(), tx);
                drop(reg);

                info!(mac, %addr, "discovered new device; starting bridge");
                let client = client.clone();
                let cfg = cfg.clone();
                let registry = registry.clone();
                let conn_rx = conn_rx.clone();
                tokio::spawn(async move {
                    device_bridge(cfg, client, mac.clone(), addr, rx, conn_rx).await;
                    // Bridge exited permanently (command channel closed): drop registration.
                    registry.lock().await.remove(&mac);
                });
            }
            ServiceEvent::ServiceRemoved(_, fullname) => {
                info!(service = %fullname, "device announced removal");
            }
            other => debug!(?other, "mDNS event"),
        }
    }

    Ok(())
}

/// Bridge a single device: TCP <-> MQTT, with reconnect. Returns when the
/// command channel is closed (i.e. the adapter is shutting the device down).
async fn device_bridge(
    cfg: Arc<Config>,
    client: AsyncClient,
    mac: String,
    addr: SocketAddr,
    mut rx: mpsc::Receiver<Vec<u8>>,
    mut conn_rx: watch::Receiver<u64>,
) {
    let resp_topic = format!("{}/{}/resp", cfg.mqtt.topic_prefix, mac);
    let status_topic = format!("{}/{}/status", cfg.mqtt.topic_prefix, mac);
    let terminator = cfg.socket.terminator_bytes();
    let delimiter = cfg.socket.delimiter_bytes();

    loop {
        match TcpStream::connect(addr).await {
            Ok(stream) => {
                info!(mac, %addr, "connected to device socket");
                publish_status(&client, &status_topic, "online").await;

                let disconnected = bridge_loop(
                    &client,
                    &resp_topic,
                    &status_topic,
                    &mac,
                    &terminator,
                    &delimiter,
                    stream,
                    &mut rx,
                    &mut conn_rx,
                )
                .await;
                publish_status(&client, &status_topic, "offline").await;

                if disconnected.channel_closed {
                    info!(mac, "command channel closed; stopping bridge");
                    return;
                }
                warn!(mac, %addr, "device socket closed; reconnecting");
            }
            Err(e) => {
                warn!(mac, %addr, error = %e, "failed to connect to device socket");
            }
        }

        // Drain reconnect backoff, but bail immediately if shutdown was requested.
        let backoff = tokio::time::sleep(RECONNECT_DELAY);
        tokio::pin!(backoff);
        loop {
            tokio::select! {
            _ = &mut backoff => break,
            // Broker reconnected: restore the retained status (device is offline here).
            _ = conn_rx.changed() => publish_status(&client, &status_topic, "offline").await,
            msg = rx.recv() => {
                if msg.is_none() {
                    info!(mac, "command channel closed during backoff; stopping bridge");
                    return;
                }
                // A command arrived while disconnected; drop it and retry connecting.
                warn!(mac, "dropping command received while device disconnected");
            }
            }
        }
    }
}

struct BridgeExit {
    channel_closed: bool,
}

#[allow(clippy::too_many_arguments)]
/// Pump data between the connected socket and MQTT until either side ends.
async fn bridge_loop(
    client: &AsyncClient,
    resp_topic: &str,
    status_topic: &str,
    mac: &str,
    terminator: &[u8],
    delimiter: &[u8],
    stream: TcpStream,
    rx: &mut mpsc::Receiver<Vec<u8>>,
    conn_rx: &mut watch::Receiver<u64>,
) -> BridgeExit {
    let (mut read_half, mut write_half) = stream.into_split();
    let mut acc: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];

    loop {
        // Emit any complete frames already buffered before awaiting more I/O.
        emit_frames(client, resp_topic, mac, delimiter, &mut acc).await;

        tokio::select! {
            // MQTT command -> socket
            cmd = rx.recv() => match cmd {
                Some(mut payload) => {
                    if !terminator.is_empty() && !payload.ends_with(terminator) {
                        payload.extend_from_slice(terminator);
                    }
                    if let Err(e) = write_half.write_all(&payload).await {
                        warn!(mac, error = %e, "socket write failed");
                        return BridgeExit { channel_closed: false };
                    }
                }
                None => return BridgeExit { channel_closed: true },
            },

            // Broker (re)connected: re-announce the retained online status.
            _ = conn_rx.changed() => publish_status(client, status_topic, "online").await,

            // Socket bytes -> accumulate (read() is cancel-safe in select!)
            n = read_half.read(&mut chunk) => match n {
                Ok(0) => return BridgeExit { channel_closed: false }, // socket EOF
                Ok(n) => acc.extend_from_slice(&chunk[..n]),
                Err(e) => {
                    warn!(mac, error = %e, "socket read failed");
                    return BridgeExit { channel_closed: false };
                }
            },
        }
    }
}

/// Drain complete frames from `acc` and publish each to the response topic.
///
/// With a non-empty delimiter, splits on it and leaves any trailing partial
/// frame buffered. As a convenience for the default `\n` delimiter, a trailing
/// `\r` is stripped so CRLF-terminated devices yield clean frames. With an
/// empty delimiter, framing is disabled: the whole buffer is published as one
/// message per read.
async fn emit_frames(
    client: &AsyncClient,
    resp_topic: &str,
    mac: &str,
    delimiter: &[u8],
    acc: &mut Vec<u8>,
) {
    if delimiter.is_empty() {
        if !acc.is_empty() {
            let frame = std::mem::take(acc);
            publish_frame(client, resp_topic, mac, frame).await;
        }
        return;
    }

    while let Some(pos) = find_subslice(acc, delimiter) {
        let mut frame: Vec<u8> = acc.drain(..pos + delimiter.len()).collect();
        frame.truncate(pos); // drop the delimiter itself
        if delimiter == b"\n" && frame.last() == Some(&b'\r') {
            frame.pop();
        }
        publish_frame(client, resp_topic, mac, frame).await;
    }
}

async fn publish_frame(client: &AsyncClient, resp_topic: &str, mac: &str, frame: Vec<u8>) {
    info!(mac, bytes = frame.len(), "socket -> mqtt");
    if let Err(e) = client.try_publish(resp_topic, QoS::AtLeastOnce, false, frame) {
        warn!(mac, error = %e, "failed to publish device response");
    }
}

/// Index of the first occurrence of `needle` within `haystack`.
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

async fn publish_status(client: &AsyncClient, topic: &str, status: &str) {
    // Retained so late subscribers see the current online/offline state.
    if let Err(e) = client.try_publish(topic, QoS::AtLeastOnce, true, status.as_bytes()) {
        warn!(topic, error = %e, "failed to publish status");
    }
}

/// Choose the best address to dial from the set mDNS resolved.
///
/// Preference order: IPv4 > routable IPv6 > link-local IPv6. A link-local
/// (`fe80::/10`) address is unusable without a scope/zone id identifying the
/// interface — connecting to a bare `fe80::...` fails with EINVAL (os error 22).
/// We attach a best-effort scope id from the host's interfaces for those.
fn select_socket_addr<'a, I>(addrs: I, port: u16) -> Option<SocketAddr>
where
    I: IntoIterator<Item = &'a IpAddr>,
{
    let mut v4 = None;
    let mut v6_global = None;
    let mut v6_linklocal = None;

    for ip in addrs {
        match ip {
            IpAddr::V4(_) => v4.get_or_insert(*ip),
            IpAddr::V6(v6) if (v6.segments()[0] & 0xffc0) == 0xfe80 => {
                v6_linklocal.get_or_insert(*v6);
                continue;
            }
            IpAddr::V6(_) => v6_global.get_or_insert(*ip),
        };
    }

    if let Some(ip) = v4.or(v6_global) {
        return Some(SocketAddr::new(ip, port));
    }

    // Only a link-local IPv6 address is available; it needs a scope id.
    let v6 = v6_linklocal?;
    let scope = link_local_scope_id().unwrap_or(0);
    if scope == 0 {
        warn!(
            %v6,
            "only a link-local IPv6 address is available and no interface scope \
             could be determined; connection will likely fail"
        );
    }
    Some(SocketAddr::V6(SocketAddrV6::new(v6, port, 0, scope)))
}

/// Best-effort scope id: the index of the first up, non-loopback interface that
/// itself carries an IPv6 link-local address.
fn link_local_scope_id() -> Option<u32> {
    let ifaces = if_addrs::get_if_addrs().ok()?;
    ifaces.into_iter().find_map(|iface| {
        if iface.is_loopback() {
            return None;
        }
        match iface.addr.ip() {
            IpAddr::V6(v6) if (v6.segments()[0] & 0xffc0) == 0xfe80 => iface.index,
            _ => None,
        }
    })
}

/// Pull the MAC out of an mDNS hostname like `pdk-board[5c:85:7e:60:05:90].local.`
/// and return it as a topic-safe segment (`5c-85-7e-60-05-90`).
fn extract_mac(hostname: &str) -> Option<String> {
    let start = hostname.find('[')?;
    let end = hostname[start..].find(']')? + start;
    let raw = &hostname[start + 1..end];
    if raw.is_empty() {
        return None;
    }
    Some(sanitize_mac(raw))
}

/// Normalize a MAC into a topic-safe segment: lowercase, `:`/spaces -> `-`.
fn sanitize_mac(mac: &str) -> String {
    mac.trim()
        .to_lowercase()
        .chars()
        .map(|c| match c {
            ':' | ' ' | '/' | '.' => '-',
            other => other,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_caps() {
        let mut b = MQTT_BACKOFF_MIN;
        let mut seen = vec![b];
        for _ in 0..6 {
            b = next_backoff(b);
            seen.push(b);
        }
        let secs: Vec<u64> = seen.iter().map(|d| d.as_secs()).collect();
        assert_eq!(secs, vec![1, 2, 4, 8, 16, 30, 30]);
    }
}
