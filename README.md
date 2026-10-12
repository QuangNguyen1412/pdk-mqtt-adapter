# rust-mqtt-adapter

Bridges MQTT commands to the TCP sockets of `_pdk-board._tcp` devices discovered
over mDNS. Each device gets a unique topic namespace keyed by its MAC address,
and the bridge is bidirectional: commands flow MQTT → socket, replies flow
socket → MQTT.

## How it works

1. Connects to the MQTT broker defined in `config.yaml`. If the broker cannot be
   reached (or rejects the credentials) within 10 s, the adapter exits with a
   non-zero status.
2. Subscribes to `<prefix>/+/cmd` and browses mDNS for the configured service
   type (`_pdk-board._tcp.local.`).
3. For each resolved device it extracts the MAC from the mDNS **hostname**
   (e.g. `pdk-board[5c:85:7e:60:05:90].local.` → `5c-85-7e-60-05-90`), picks an
   address (IPv4 > global IPv6 > link-local IPv6 with a scope id), and opens a TCP
   connection to the advertised port. Multiple devices → multiple bridges, one
   task each.
4. Routes traffic on per-device topics.

## Topic layout

For a device with MAC `aa:bb:cc:dd:ee:ff` (normalized to `aa-bb-cc-dd-ee-ff`)
and prefix `pdk-board`:

| Topic                                | Direction      | Notes                                                        |
| ------------------------------------ | -------------- | ------------------------------------------------------------ |
| `pdk-board/aa-bb-cc-dd-ee-ff/cmd`    | MQTT → socket  | Payload written to the socket; the command terminator is appended if absent |
| `pdk-board/aa-bb-cc-dd-ee-ff/resp`   | socket → MQTT  | One message per frame, split on the response delimiter       |
| `pdk-board/aa-bb-cc-dd-ee-ff/status` | publish        | `online` / `offline`, retained                               |
| `pdk-board/adapter/status`           | publish        | Adapter itself: `online` on connect, `offline` via MQTT Last Will; retained |

## Configuration

See `config.yaml` (path can be passed as the first CLI argument).

| Key                         | Default                    | Description |
| --------------------------- | -------------------------- | ----------- |
| `mqtt.host`                 | (required)                 | Broker host |
| `mqtt.port`                 | `1883`                     | Broker port |
| `mqtt.client_id`            | `rust-mqtt-adapter`        | MQTT client id |
| `mqtt.username` / `password`| none                       | Optional credentials |
| `mqtt.keep_alive_secs`      | `30`                       | Keep-alive interval |
| `mqtt.topic_prefix`         | `pdk-board`                | Root of the topic tree |
| `discovery.service_type`    | `_pdk-board._tcp.local.`   | mDNS service type to browse |
| `socket.command_terminator` | `\n`                       | Appended to each command if not already present. Escapes `\n \r \t \0 \\` are interpreted; `""` appends nothing |
| `socket.response_delimiter` | `\n`                       | Splits socket output into frames, one MQTT message each. A trailing `\r` is stripped when the delimiter is `\n`. `""` disables framing (each raw read is published) |

## Run

```sh
cargo run --release            # uses ./config.yaml
cargo run --release -- /etc/pdk/config.yaml
RUST_LOG=debug cargo run        # verbose logging
```

## Cross compile

```sh
scripts/cross-build.sh                      # aarch64-unknown-linux-gnu, release
scripts/cross-build.sh --debug              # debug build
scripts/cross-build.sh armv7-unknown-linux-gnueabihf
```

The script installs the Rust target via `rustup` and checks for the linker
(e.g. `sudo apt install gcc-aarch64-linux-gnu`). Linkers are configured in
`.cargo/config.toml`. Output: `target/<triple>/<profile>/rust-mqtt-adapter`.

## Notes

- **Broker reconnect:** after the initial connection, broker outages are retried
  forever with exponential backoff (1 s doubling up to 30 s). The command
  subscription is re-established on every reconnect, and retained device
  statuses are re-published.
- **Publishing is non-blocking:** while the broker is down, responses and status
  updates that cannot be queued are dropped (with a warning) rather than
  stalling device sockets.
- **Device reconnect:** disconnected device sockets reconnect automatically
  (5 s backoff). Commands arriving while a device is offline are dropped (with a
  warning).
- Device status is published retained so late subscribers see current state.
