#!/usr/bin/env python3
"""
MeshTunnel, generic UDP-over-Reticulum tunnel.

A reusable transport primitive: it carries arbitrary UDP datagrams across a
Reticulum mesh, tagged by destination port, with nothing app-specific baked in.
EverQuest is just one configuration; any UDP service works the same way.

It is the streaming/unreliable companion to MeshAPI: MeshAPI is REST-shaped
request/response over the mesh; MeshTunnel is a raw datagram pipe for latency-
sensitive, high-rate protocols that own their own reliability.

Two ends, both leaf nodes that peer with the Local Peering Hub. They are named
for the direction traffic flows through them, NOT "client/server", which would
collide with the client/server of whatever protocol is being tunneled:

  EGRESS sits next to the target service; tunneled traffic EXITS here onto the real
           service. Announces the Reticulum destination; the ingress dials it.
      meshtunnel.py egress --service mygame --target-host 192.0.2.10
      # replays each tunneled datagram to (target_host, <port from frame>).
      # prints its Reticulum destination hash for the ingress end.

  INGRESS sits next to the user; the user's traffic ENTERS the tunnel here.
      meshtunnel.py ingress --service mygame --egress-hash <hash> \
                    --bind 127.0.0.1 --ports 5998,5999,9000,7778,7000-7400 \
                    --identity ./mt_ingress_identity
      # binds those local UDP ports; anything sent to them tunnels across.

Data flow:   app-client -> INGRESS -> RNS Link -> EGRESS -> app-server   (and back)

--service names the tunnel (its RNS app namespace); both ends must match, and
different services get different destinations. Nothing above is EQ-specific
except the example values.

IDENTITY & ACCESS CONTROL
  Over Reticulum there is no client IP. Every ingress instead has a Reticulum
  Identity, and the Link it opens is cryptographically bound to that Identity's
  destination hash. That hash is unforgeable and stable: it is the sovereign
  player id. Give the ingress a persisted --identity and it identifies itself to
  the egress over the encrypted link (RNS Link.identify). The egress then can:

    * BAN by hash (strongest, mesh-native): --deny-file lists denied hashes and
      --allow-file switches to allowlist mode; a denied/uninvited link is torn
      down at setup, before a single packet reaches the service, and cannot be
      evaded by reconnecting from elsewhere. --require-identity (implied by
      --allow-file) drops links that never identify.

    * TRACK by synthetic IP (so an IP-centric service keeps working): with
      --identity-ip-range CIDR (e.g. 100.64.0.0/10) each identity is assigned a
      stable synthetic source IP, persisted in --registry-file, and the egress
      sources that player's datagrams from it. The service then sees a distinct,
      stable IP per player (bans, dedup, per-player logs all work unchanged), and
      every synthetic IP resolves back through the registry to the real hash.

  Synthetic source IPs require the operator to make the range routable back to
  the egress host (assign it to a local dummy interface, and on a remote target
  route the range back to the egress). See README.

Frame (one per datagram):  >BH  flags(1) dest_port(2 BE) | payload
  flags: 0x01 zstd  0x02 aggregated  0x04 control   (0x01/0x02 reserved)

I/O model: the RNS packet callback (RNS's thread) only ENQUEUES; a single
worker thread owns every socket and the selector, so selector state is never
mutated across threads. Data plane is UNRELIABLE (create_receipt=False) because the
tunneled protocol owns reliability end-to-end.
"""

import argparse
import ipaddress
import json
import os
import queue
import selectors
import socket
import struct
import threading
import time

import RNS

ASPECT = "tunnel"
DEBUG = False                                  # --debug: log every frame (verbose)

FRAME_HDR = struct.Struct(">BH")               # flags, dest_port
FLAG_ZSTD, FLAG_AGG, FLAG_CTRL = 0x01, 0x02, 0x04
POLL = 0.002
IDENTIFY_GRACE = 10.0                          # seconds an ingress has to identify
IP_FREEBIND = getattr(socket, "IP_FREEBIND", 15)  # Linux: bind a not-yet-local src IP


def parse_ports(spec: str):
    """'5998,5999,9000,7778,7000-7400' -> [5998, 5999, 9000, 7778, 7000..7400]."""
    ports = []
    for part in (spec or "").split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            ports.extend(range(int(a), int(b) + 1))
        else:
            ports.append(int(part))
    return ports


def frame(dest_port, payload, flags=0):
    return FRAME_HDR.pack(flags, dest_port) + payload


def unframe(data):
    flags, dest_port = FRAME_HDR.unpack_from(data, 0)
    return flags, dest_port, data[FRAME_HDR.size:]


def link_send(link, data):
    if link.mdu and len(data) > link.mdu:
        pass  # TODO(radio): fragment into <=mdu chunks with a reassembly tag.
    RNS.Packet(link, data, create_receipt=False).send()


class IdentityRegistry:
    """Persists ingress hash -> stable synthetic client IP: the player registry.

    The mapping is deterministic-per-registry (sequential from the range) and
    persisted, so an identity always resolves to the same synthetic IP. That is
    what makes an IP ban in the downstream service equivalent to a hash ban.
    """

    def __init__(self, path, ip_range=None):
        self.path = path
        self.lock = threading.Lock()
        self.map = {}          # hexhash -> {"ip", "first_seen", "last_seen", "note"}
        self.next_offset = 1   # skip .0 (network address)
        self.net = ipaddress.ip_network(ip_range, strict=False) if ip_range else None
        self._load()
        if self.net is not None and not self.path:
            RNS.log("registry: --identity-ip-range set without --registry-file; "
                    "synthetic IPs will NOT survive a restart", RNS.LOG_WARNING)

    def _load(self):
        if self.path and os.path.isfile(self.path):
            try:
                with open(self.path) as f:
                    d = json.load(f)
                self.map = d.get("identities", {})
                self.next_offset = d.get("next_offset", 1)
                RNS.log(f"registry: loaded {len(self.map)} identities from {self.path}",
                        RNS.LOG_INFO)
            except Exception as e:
                RNS.log(f"registry: could not load {self.path}: {e}", RNS.LOG_WARNING)

    def _save(self):
        if not self.path:
            return
        tmp = self.path + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump({"identities": self.map, "next_offset": self.next_offset},
                          f, indent=2, sort_keys=True)
            os.replace(tmp, self.path)
        except Exception as e:
            RNS.log(f"registry: could not save {self.path}: {e}", RNS.LOG_WARNING)

    def _alloc_ip(self):
        ip = str(ipaddress.ip_address(int(self.net.network_address) + self.next_offset))
        self.next_offset += 1
        return ip

    def synthetic_ip(self, hexhash, ts):
        """Record this identity (the player registry) and return its stable
        synthetic IP, or None when no IP range is configured. The sighting is
        recorded either way, so the registry tracks players even without IPs;
        an IP is back-filled if a range is added after the first sighting."""
        with self.lock:
            rec = self.map.get(hexhash)
            if rec is None:
                ip = self._alloc_ip() if self.net is not None else None
                rec = {"ip": ip, "first_seen": ts, "last_seen": ts, "note": ""}
                self.map[hexhash] = rec
                RNS.log(f"registry: new identity {hexhash}" + (f" -> {ip}" if ip else ""),
                        RNS.LOG_INFO)
            else:
                rec["last_seen"] = ts
                if rec.get("ip") is None and self.net is not None:
                    rec["ip"] = self._alloc_ip()
                    RNS.log(f"registry: back-filled {hexhash} -> {rec['ip']}", RNS.LOG_INFO)
            self._save()
            return rec["ip"]


class AccessControl:
    """Hash allow/deny lists, re-read on each check so bans hot-reload."""

    def __init__(self, deny_file=None, allow_file=None):
        self.deny_file = deny_file
        self.allow_file = allow_file
        self.allowlist_mode = allow_file is not None

    @staticmethod
    def _read(path):
        out = set()
        if path and os.path.isfile(path):
            try:
                with open(path) as f:
                    for line in f:
                        line = line.split("#", 1)[0].strip().lower()
                        if line:
                            out.add(line)
            except Exception as e:
                RNS.log(f"access: could not read {path}: {e}", RNS.LOG_WARNING)
        return out

    def allowed(self, hexhash):
        hexhash = hexhash.lower()
        if hexhash in self._read(self.deny_file):
            return False
        if self.allowlist_mode:
            return hexhash in self._read(self.allow_file)
        return True


class EgressEnd:
    """Next to the target service. Announces a destination; replays to target."""

    def __init__(self, service, target_host, identity_path, allow_ports=None,
                 registry=None, access=None, require_identity=False):
        self.target_host = target_host
        self.allow = set(allow_ports) if allow_ports else None
        self.registry = registry
        self.access = access
        self.require_identity = bool(require_identity or (access and access.allowlist_mode))
        self.sel = selectors.DefaultSelector()
        self.inbound = queue.Queue()
        self.sockets = {}
        self.links = {}
        self.link_info = {}    # id(link) -> {link, hexhash, synth_ip, ready}
        self.identity = load_or_create_identity(identity_path)
        self.dest = RNS.Destination(self.identity, RNS.Destination.IN,
                                    RNS.Destination.SINGLE, service, ASPECT)
        self.dest.set_link_established_callback(self.on_link)
        RNS.log(f"[{service}] egress destination: {RNS.prettyhexrep(self.dest.hash)}", RNS.LOG_INFO)
        if self.require_identity:
            RNS.log("egress: identity REQUIRED (unidentified links are dropped)", RNS.LOG_INFO)
        if self.registry and self.registry.net is not None:
            RNS.log(f"egress: synthetic client IPs from {self.registry.net}", RNS.LOG_INFO)
        threading.Thread(target=self._worker, daemon=True).start()
        self._announce_loop()

    def on_link(self, link):
        info = {"link": link, "hexhash": None, "synth_ip": None,
                "ready": not self.require_identity}
        self.link_info[id(link)] = info
        self.links[id(link)] = link
        link.set_packet_callback(self.on_frame)
        link.set_remote_identified_callback(self.on_identified)
        link.set_link_closed_callback(lambda l: self.inbound.put((id(l), None, None)))
        RNS.log(f"ingress link up mtu={link.mtu} mdu={link.mdu} "
                f"(identity {'required' if self.require_identity else 'optional'})", RNS.LOG_INFO)
        if self.require_identity:
            threading.Timer(IDENTIFY_GRACE, self._identity_deadline, args=(id(link),)).start()

    def _identity_deadline(self, lid):
        info = self.link_info.get(lid)
        if info and not info["ready"]:
            RNS.log("ingress did not identify within grace window, tearing down", RNS.LOG_WARNING)
            try:
                info["link"].teardown()
            except Exception:
                pass

    def on_identified(self, link, identity):
        info = self.link_info.get(id(link))
        if info is None:
            return
        hexhash = identity.hexhash
        if self.access and not self.access.allowed(hexhash):
            RNS.log(f"DENIED ingress {hexhash}, tearing down link", RNS.LOG_WARNING)
            try:
                link.teardown()
            except Exception:
                pass
            return
        synth_ip = self.registry.synthetic_ip(hexhash, time.time()) if self.registry else None
        info["hexhash"] = hexhash
        info["synth_ip"] = synth_ip
        info["ready"] = True
        RNS.log(f"ingress identified {hexhash}" + (f" -> {synth_ip}" if synth_ip else ""),
                RNS.LOG_INFO)

    def on_frame(self, data, packet):
        info = self.link_info.get(id(packet.link))
        if info is not None and not info["ready"]:
            return  # not identified yet (require_identity): drop until it is
        flags, port, payload = unframe(data)
        if DEBUG:
            RNS.log(f"egress: rx frame {len(payload)}B for :{port}", RNS.LOG_DEBUG)
        if flags & FLAG_CTRL:
            return
        if self.allow is not None and port not in self.allow:
            return  # refuse ports outside the allow-list
        self.inbound.put((id(packet.link), port, payload))

    def _worker(self):
        while True:
            try:
                while True:
                    lid, port, payload = self.inbound.get_nowait()
                    if port is None:
                        self._teardown(lid); continue
                    self._socket_for(lid, port).sendto(payload, (self.target_host, port))
            except queue.Empty:
                pass
            except OSError as e:
                RNS.log(f"target sendto failed: {e}", RNS.LOG_ERROR)
            if not self.sel.get_map():      # no target sockets yet; Windows select() errors on empty
                time.sleep(POLL); continue
            for k, _ in self.sel.select(timeout=POLL):
                lid, port = k.data
                link = self.links.get(lid)
                try:
                    payload = k.fileobj.recv(65535)
                except OSError:
                    continue
                if link and link.status == RNS.Link.ACTIVE:
                    link_send(link, frame(port, payload))

    def _socket_for(self, lid, port):
        key = (lid, port)
        sock = self.sockets.get(key)
        if sock is None:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            info = self.link_info.get(lid)
            synth_ip = info.get("synth_ip") if info else None
            if synth_ip:
                # source this player's traffic from their stable synthetic IP so
                # the downstream service sees a distinct, traceable client address.
                try:
                    sock.setsockopt(socket.IPPROTO_IP, IP_FREEBIND, 1)
                except (AttributeError, OSError):
                    pass  # non-Linux / unsupported: rely on the IP being local
                try:
                    sock.bind((synth_ip, 0))
                except OSError as e:
                    RNS.log(f"could not bind synthetic ip {synth_ip} ({e}); "
                            f"using default source (is the range routed to this host?)",
                            RNS.LOG_WARNING)
            sock.connect((self.target_host, port))
            sock.setblocking(False)
            self.sockets[key] = sock
            self.sel.register(sock, selectors.EVENT_READ, (lid, port))
        return sock

    def _teardown(self, lid):
        for key in [k for k in self.sockets if k[0] == lid]:
            try:
                self.sel.unregister(self.sockets[key]); self.sockets[key].close()
            except Exception:
                pass
            del self.sockets[key]
        self.links.pop(lid, None)
        self.link_info.pop(lid, None)

    def _announce_loop(self):
        while True:
            self.dest.announce()
            time.sleep(300)


class IngressEnd:
    """Next to the user. Binds local ports; dials the egress destination."""

    def __init__(self, service, egress_hash, bind_addr, ports, identity_path=None):
        self.service = service
        self.egress_hash = egress_hash
        self.bind_addr = bind_addr
        self.ports_wanted = ports
        self.identity = load_or_create_identity(identity_path) if identity_path else None
        self.sel = selectors.DefaultSelector()
        self.inbound = queue.Queue()
        self.ports = {}
        self.link = None
        for p in self.ports_wanted:      # local ports bind up front, independent of the link
            self._bind(p)
        threading.Thread(target=self._worker, daemon=True).start()
        self._establish()
        threading.Thread(target=self._link_monitor, daemon=True).start()

    def _establish(self):
        if not RNS.Transport.has_path(self.egress_hash):
            RNS.Transport.request_path(self.egress_hash)
            RNS.log("requesting path to egress...", RNS.LOG_INFO)
            deadline = time.time() + 15
            while not RNS.Transport.has_path(self.egress_hash) and time.time() < deadline:
                time.sleep(0.2)
        recalled = RNS.Identity.recall(self.egress_hash)
        if recalled is None:
            RNS.log("no path/identity to egress yet, will retry", RNS.LOG_INFO)
            return
        dest = RNS.Destination(recalled, RNS.Destination.OUT,
                               RNS.Destination.SINGLE, self.service, ASPECT)
        link = RNS.Link(dest)
        link.set_link_established_callback(self.on_up)
        link.set_packet_callback(self.on_frame)
        self.link = link
        RNS.log("establishing link to egress...", RNS.LOG_INFO)

    def _link_monitor(self):
        # re-establish the link if the egress restarts or the path drops, so an
        # egress bounce never requires restarting this end (which risks flap-block).
        # A dead egress leaves the link STALE before it reaches CLOSED, so recover
        # from both; tear a STALE link down first so the destination is re-dialed.
        while True:
            time.sleep(3)
            status = self.link.status if self.link else None
            if self.link is None or status in (RNS.Link.CLOSED, RNS.Link.STALE):
                if self.link is not None and status == RNS.Link.STALE:
                    try:
                        self.link.teardown()
                    except Exception:
                        pass
                RNS.log("egress link down, reconnecting", RNS.LOG_INFO)
                self._establish()

    def on_up(self, link):
        RNS.log(f"link to egress up mtu={link.mtu} mdu={link.mdu}", RNS.LOG_INFO)
        if self.identity is not None:
            try:
                link.identify(self.identity)
                RNS.log(f"identified to egress as {self.identity.hexhash}", RNS.LOG_INFO)
            except Exception as e:
                RNS.log(f"could not identify to egress: {e}", RNS.LOG_ERROR)

    def _bind(self, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.bind((self.bind_addr, port))
        except OSError:
            s.close(); return
        s.setblocking(False)
        self.ports[port] = [s, None]
        self.sel.register(s, selectors.EVENT_READ, port)

    def on_frame(self, data, packet):
        _flags, port, payload = unframe(data)
        self.inbound.put((port, payload))

    def _worker(self):
        while True:
            try:
                while True:
                    port, payload = self.inbound.get_nowait()
                    entry = self.ports.get(port)
                    if entry and entry[1] is not None:
                        entry[0].sendto(payload, entry[1])
            except queue.Empty:
                pass
            if not self.sel.get_map():      # Windows select() errors on an empty set
                time.sleep(POLL); continue
            for k, _ in self.sel.select(timeout=POLL):
                port = k.data
                try:
                    payload, src = k.fileobj.recvfrom(65535)
                except OSError:
                    continue
                self.ports[port][1] = src
                if DEBUG:
                    RNS.log(f"ingress: {len(payload)}B in on :{port}, link={self.link.status if self.link else None}", RNS.LOG_DEBUG)
                if self.link and self.link.status == RNS.Link.ACTIVE:
                    link_send(self.link, frame(port, payload))


def load_or_create_identity(path):
    if os.path.isfile(path):
        ident = RNS.Identity.from_file(path)
        if ident:
            return ident
    ident = RNS.Identity()
    if not ident.to_file(path):
        RNS.log(f"WARNING: could not persist identity to {path}", RNS.LOG_WARNING)
    else:
        RNS.log(f"created + saved identity at {path}", RNS.LOG_INFO)
    return ident


def main():
    ap = argparse.ArgumentParser(description="Generic UDP-over-Reticulum tunnel.")
    sub = ap.add_subparsers(dest="role", required=True)

    e = sub.add_parser("egress", help="run next to the target service")
    e.add_argument("--service", required=True, help="tunnel name (RNS app namespace)")
    e.add_argument("--target-host", required=True, help="host to replay datagrams to")
    e.add_argument("--allow-ports", default=None, help="optional port allow-list spec")
    e.add_argument("--identity", default="./meshtunnel_identity")
    e.add_argument("--identity-ip-range", default=None,
                   help="CIDR (e.g. 100.64.0.0/10): give each identified ingress a stable "
                        "synthetic source IP toward the target")
    e.add_argument("--registry-file", default=None,
                   help="JSON file persisting hash->synthetic-IP (the player registry)")
    e.add_argument("--deny-file", default=None,
                   help="file of denied ingress hashes (one hex per line, # comments); hot-reloaded")
    e.add_argument("--allow-file", default=None,
                   help="allowlist mode: only ingress hashes listed here may connect")
    e.add_argument("--require-identity", action="store_true",
                   help="tear down ingress links that do not identify (implied by --allow-file)")
    e.add_argument("--rns-config", default=None)

    i = sub.add_parser("ingress", help="run next to the user")
    i.add_argument("--service", required=True)
    i.add_argument("--egress-hash", required=True, help="egress end's RNS destination hash")
    i.add_argument("--ports", required=True, help="ports/ranges to bind, e.g. 5998,7000-7400")
    i.add_argument("--bind", default="127.0.0.1")
    i.add_argument("--identity", default=None,
                   help="persisted RNS identity file; enables per-player identification "
                        "(required for the egress's ban/registry/synthetic-IP features)")
    i.add_argument("--rns-config", default=None)

    for p in (e, i):
        p.add_argument("--debug", action="store_true", help="log every tunneled frame")

    a = ap.parse_args()
    global DEBUG
    DEBUG = bool(getattr(a, "debug", False))
    RNS.Reticulum(a.rns_config)
    if a.role == "egress":
        registry = None
        if a.registry_file or a.identity_ip_range:
            registry = IdentityRegistry(a.registry_file, a.identity_ip_range)
        access = None
        if a.deny_file or a.allow_file:
            access = AccessControl(a.deny_file, a.allow_file)
        EgressEnd(a.service, a.target_host, a.identity,
                  parse_ports(a.allow_ports) if a.allow_ports else None,
                  registry=registry, access=access, require_identity=a.require_identity)
    else:
        IngressEnd(a.service, bytes.fromhex(a.egress_hash), a.bind, parse_ports(a.ports),
                   identity_path=a.identity)
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
