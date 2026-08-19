# meshtunnel, design

meshtunnel carries a UDP protocol across a Reticulum mesh without changing
either end of that protocol. A **bridge pair** shims the transport underneath:

```
[app client] ─UDP→ [ingress] ══ Reticulum Link ══ [egress] ─UDP→ [app server]
                     (RNS)                          (RNS)
```

Neither the app client nor the app server is aware of it. They speak plain UDP
to a local address; the tunnel moves the bytes over the mesh in between.

## Grounded Reticulum numbers

- Base `Reticulum.MTU = 500`, single-packet `MDU ≈ 465 B`.
- With `LINK_MTU_DISCOVERY` and a large `TCPInterface.HW_MTU`, a Link over a TCP
  interface negotiates a large MTU, so `Link.mdu` is big enough to carry a full
  datagram for most protocols, no bridge fragmentation on the TCP path. On
  LoRa/RNode interfaces the MTU stays ~500, so the bridge must fragment there
  (see "Open items").
- `Link` gives an encrypted, sequenced session. `RNS.Packet(link, data).send()`
  is the low-latency **unreliable** datagram primitive. `Channel`/`Buffer` add
  reliable sequenced messaging, use those for control, **not** the data plane.
- Link keepalive is configurable (5-360 s) and tolerates up to ~1.75 s RTT.

## The port collapse (the core trick)

Reticulum has no ports. Every UDP port a service uses collapses onto **one
Link**, demultiplexed by a tiny header:

```
┌────────┬────────────┬───────────────────────────┐
│ flags  │ dest_port  │ payload (raw UDP bytes)    │
│ 1 byte │ 2 bytes BE │ variable                  │
└────────┴────────────┴───────────────────────────┘
flags bits: 0x01 = compressed payload   (reserved)
            0x02 = aggregated frames     (reserved)
            0x04 = control (bridge-internal)
```

`dest_port` is the *original* UDP destination port. A whole contiguous port
range (for example a game's per-zone ports) is handled by that single field for
free, the egress just replays each frame to the port named in its header. No
enumeration, no per-port config.

## Components

**Ingress (near the user).** Binds the app's UDP ports locally on the address
the app is pointed at. Each inbound datagram becomes a frame
(`flags + port + payload`) sent as one unreliable Link packet. Frames coming
back go out as UDP to the app's source address for that port.

**Egress (near the service).** Accepts the Link and, for each `(link, port)`,
maintains a UDP socket toward `target_host:port` so the service sees distinct,
stable sessions. Replies are framed back over the Link.

One Link carries one client. Multiple clients means multiple Links.

## Identity instead of IP

Over Reticulum there is no client IP to key on. Each ingress has a Reticulum
Identity, and the Link is cryptographically bound to that identity's destination
hash, unforgeable and stable. That hash is the client id:

- **Bans key on the hash**, not a spoofable IP. A denied hash is refused at link
  setup, before any packet reaches the service, and can't be evaded by
  reconnecting from elsewhere.
- For an IP-centric service, the egress maps each hash to a **stable synthetic
  source IP** and sources that client's datagrams from it, so per-client bans,
  dedup, and logs keep working unchanged, and every synthetic IP resolves back
  through the registry to the real hash.

## I/O model

The RNS packet callback (on RNS's thread) only *enqueues*. A single worker
thread owns every socket and the selector, so selector state is never mutated
across threads. The data plane is unreliable by design: the tunneled protocol
owns reliability end-to-end. Do **not** double-ACK by stacking a reliable
Reticulum transport under a protocol that already retransmits, that produces
retransmit-latency spirals. On lossy radio, prefer link-layer FEC over
retransmit.

## Optimizations to layer on (after it works)

1. **Terminate keepalives at the bridges.** Many protocols fire frequent
   heartbeats per service. Answer them locally at each bridge and only ship real
   state changes across the mesh, an order-of-magnitude traffic cut on metered
   links.
2. **Aggregate** several small datagrams into one Link packet (`flags 0x02`) to
   amortize header overhead, a natural fit given the large TCP Link MTU.
3. **Compress** with a static dictionary trained on the protocol's traffic
   (loaded identically on both ends); tiny structs don't compress alone, but a
   shared dict plus aggregation does. Delta-encode repetitive streams.

## Open items

- Log the exact negotiated `Link.mdu` on connect over your live interface.
- Fragmentation + reassembly for interfaces whose MTU stays ~500 (LoRa/RNode).
- Whether one-Link-per-client or a multiplexed Link (client-id in the frame)
  scales better for your workload.
