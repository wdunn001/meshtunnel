# meshtunnel

Carry arbitrary **UDP over a [Reticulum](https://reticulum.network) mesh.**
`meshtunnel` is a small, dependency-light transport primitive: two ends and a
tiny per-datagram header move a real-time UDP protocol across Reticulum with
nothing app-specific baked in. A game is one configuration; any UDP service —
game servers, voice, telemetry, DNS, custom protocols — tunnels the same way.

It's the unreliable/streaming companion to request/response-over-Reticulum:
where a REST-shaped service does one call → one reply, meshtunnel is a raw
datagram pipe for latency-sensitive, high-rate protocols that own their own
reliability.

Why bother? Because it lets you reach a service that has **no public IP and no
forwarded port** — the only path to it is the mesh. And because your client's
identity on the far end is a Reticulum destination hash, not a spoofable IP:
access control and per-client tracking finally stop depending on the network
layer lying to you.

## How it works

Two ends, named for the direction traffic flows (not "client/server" — that
would collide with the client/server of whatever you're tunneling):

```
app-client ──UDP──▶ INGRESS ══ Reticulum Link ══ EGRESS ──UDP──▶ app-server
           ◀──────                                        ◀──────
```

- **EGRESS** runs next to the target service. It announces a Reticulum
  destination and replays each tunneled datagram onto the real service.
- **INGRESS** runs next to the user. It binds local UDP ports; anything sent to
  them is framed and shipped across the Link to the egress.

Reticulum has no ports, so every service port collapses onto **one Link**,
demultiplexed by a 2-byte destination-port tag in the frame header. One port
range — say a game's per-zone ports — costs nothing extra: the egress just
replays each frame to the port named in its header.

```
frame:  ┌────────┬────────────┬──────────────────────┐
        │ flags  │ dest_port  │ payload (raw UDP)     │
        │ 1 byte │ 2 bytes BE │ variable             │
        └────────┴────────────┴──────────────────────┘
```

The data plane uses **unreliable** Link packets (`create_receipt=False`) — the
tunneled protocol owns its own retransmit end-to-end. Stacking a reliable
transport under one that already retransmits is how you get latency spirals.

## Install

```sh
pip install rns          # Reticulum, the one dependency
# then run meshtunnel.py directly, or `pip install .` for a `meshtunnel` command
```

Python 3.7+. You need a working Reticulum config on both ends (any transport:
TCP, I2P, LoRa/RNode…). meshtunnel uses whatever interfaces Reticulum is
configured with; point `--rns-config` at a specific config dir if not the
default.

## Quick start

**Egress** (next to the service — e.g. a game server on `192.0.2.10`):

```sh
python meshtunnel.py egress --service mygame --target-host 192.0.2.10 \
       --identity ./mt_egress_identity
# prints its Reticulum destination hash — give that to the ingress.
```

**Ingress** (next to the user):

```sh
python meshtunnel.py ingress --service mygame --egress-hash <hash-from-egress> \
       --bind 127.0.0.1 --ports 5998,5999,9000,7000-7400 \
       --identity ./mt_ingress_identity
```

Now point the app at `127.0.0.1` on those ports and it reaches the service over
the mesh. `--service` names the tunnel (its RNS app namespace); both ends must
use the same name, and different services get different destinations.

If the app resolves the service by hostname, map that hostname to `127.0.0.1`
on the client machine (hosts file) so the app dials the ingress for every port.

## Identity-based access control

Over Reticulum there is no client IP. Each ingress instead has a Reticulum
**Identity**, and the Link it opens is cryptographically bound to that
identity's destination hash — unforgeable and stable. Give the ingress a
persisted `--identity` and it identifies itself to the egress over the encrypted
link. The egress can then:

- **Allow/deny by hash** — `--deny-file` lists denied hashes (hot-reloaded, so a
  ban takes effect without a restart); `--allow-file` switches to allowlist
  mode. A denied or uninvited link is torn down at setup, before a single packet
  reaches the service. `--require-identity` (implied by `--allow-file`) drops
  links that never identify.

- **Assign a stable synthetic source IP per identity** — for a service that is
  IP-centric (per-client bans, dedup, per-player logs). `--identity-ip-range
  100.64.0.0/10` gives each identified ingress a stable synthetic IP, persisted
  in `--registry-file`, and the egress sources that client's datagrams from it.
  The service sees a distinct, traceable address per client; every synthetic IP
  resolves back through the registry to the real hash.

  Synthetic source IPs require the operator to make the range routable back to
  the egress host: assign it to a local dummy interface, and if the target
  service is on a *different* host, route the range back to the egress there.

```sh
# egress with a hash allowlist + synthetic per-client IPs
python meshtunnel.py egress --service mygame --target-host 192.0.2.10 \
       --allow-file ./allowed_hashes.txt \
       --identity-ip-range 100.64.0.0/10 --registry-file ./registry.json
```

## Options

`egress`:

| flag | meaning |
|------|---------|
| `--service` | tunnel name (RNS app namespace); must match the ingress |
| `--target-host` | host to replay datagrams to |
| `--allow-ports` | optional port allow-list, e.g. `5998,5999,7000-7400` |
| `--identity` | egress RNS identity file (created if missing) |
| `--identity-ip-range` | CIDR for stable synthetic per-client source IPs |
| `--registry-file` | JSON persisting hash → synthetic IP (the client registry) |
| `--deny-file` | denied ingress hashes, one hex per line, `#` comments, hot-reloaded |
| `--allow-file` | allowlist mode: only listed hashes may connect |
| `--require-identity` | drop links that don't identify (implied by `--allow-file`) |
| `--rns-config` | Reticulum config dir (default: system default) |

`ingress`:

| flag | meaning |
|------|---------|
| `--service` | tunnel name; must match the egress |
| `--egress-hash` | egress end's RNS destination hash |
| `--ports` | local UDP ports/ranges to bind, e.g. `5998,7000-7400` |
| `--bind` | address to bind on (default `127.0.0.1`) |
| `--identity` | persisted RNS identity; enables per-client identification |
| `--rns-config` | Reticulum config dir |

Both accept `--debug` to log every frame.

## Latency, by transport

meshtunnel adds a mesh hop, so playability depends on the underlying Reticulum
interface. Rough guidance:

| Transport            | Latency        | Real-time play | Latency-tolerant traffic |
|----------------------|----------------|----------------|--------------------------|
| TCP interface        | ~internet RTT  | usually fine   | yes                      |
| I2P interface        | 100s ms – s    | marginal       | yes                      |
| LoRa / packet radio  | seconds        | no             | yes (login/chat/turns)   |

Reticulum's Link keepalive tolerates up to ~1.75 s RTT, which is why
latency-tolerant protocols (older games built for dial-up, turn-based traffic,
telemetry) ride a mesh far better than twitch-sensitive modern ones.

## Why this exists

For years people wouldn't switch networks — or platforms — not because they were
worse, but because they couldn't bring the things they wanted to *do* there.
People don't adopt infrastructure; they adopt what runs on it. Put something
people actually want to use on the mesh and you give them a reason to run a node
— and once they're on the mesh, everything else it carries (messaging, maps,
files, off-grid comms) comes with them. A game server was the first thing this
tunnel carried. It'll carry yours too.

See [DESIGN.md](DESIGN.md) for the protocol and the optimizations (keepalive
termination, aggregation, compression) worth layering on once it works.

## License

MIT © William Dunn
