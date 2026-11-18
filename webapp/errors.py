#!/usr/bin/env python3
"""Error analysis for Aerospike server logs.

asloglatency answers "how slow was it". This module answers "what went wrong",
from the same log files, in one pass each:

  * severity lines      WARNING / CRITICAL / ERROR, grouped by message shape,
                        with (repeated:N) collapses expanded back out
  * outcome counters    the cumulative error/timeout/not-found/filtered-out
                        fields of the per-namespace ticker lines, differenced
                        into per-interval rates
  * health gauges       fd usage, clock skew, device available pct, queues,
                        long queries, batch delays
  * cluster events      cluster-size changes, evictions to orphan, rebalances

Server logs do not carry client result codes (AS_ERR_*); the counters above are
the server-side record of the same failures, which is why they are the spine of
this report rather than a code histogram.
"""

import calendar
import collections
import datetime
import re
import time

LOG_DT_FMT = "%b %d %Y %H:%M:%S"

SEVERITY_RE = re.compile(
    r"^(\w{3} \d{2} \d{4} \d{2}:\d{2}:\d{2}) GMT: (WARNING|CRITICAL|ERROR) \(([\w-]+)\): \(([\w.+-]+:\d+)\) (.*)$")
TS_RE = re.compile(r"^(\w{3} \d{2} \d{4} \d{2}:\d{2}:\d{2}) GMT")
REPEAT_RE = re.compile(r"^\(repeated:(\d+)\)\s*")
NS_PREFIX_RE = re.compile(r"^\{(\S+)\}\s+")
QUEUE_TOO_DEEP_RE = re.compile(r"queue too deep", re.I)

# ticker blocks:  {ns} client: tsvc (0,0) proxy (4,0,0) read (1,0,0,2,0) ...
BLOCK_RE = re.compile(
    r"\{(\S+)\} (client|from-proxy|batch-sub|from-proxy-batch-sub|xdr-client|xdr-from-proxy"
    r"|pi-query|si-query|scan): (.*)")
TUPLE_RE = re.compile(r"([a-z-]+) \(([\d,]+)\)")
# Field layouts from the Database Log Reference (S,C,E,T,N,F / R,W,D,E).
LAYOUT = {
    "tsvc": ("error", "timeout"),
    "proxy": ("complete", "error", "timeout"),
    "read": ("success", "error", "timeout", "not_found", "filtered_out"),
    "write": ("success", "error", "timeout", "filtered_out"),
    "delete": ("success", "error", "timeout", "not_found", "filtered_out"),
    "udf": ("complete", "error", "timeout", "filtered_out"),
    "lang": ("read", "write", "delete", "error"),
    # pi-query / si-query job outcomes (Complete, Error, Abort). "scan" is the
    # pre-6.0 name for the pi-query ticker and uses "basic" instead of *-basic.
    "short-basic": ("complete", "error", "abort"),
    "long-basic": ("complete", "error", "abort"),
    "basic": ("complete", "error", "abort"),
    "aggr": ("complete", "error", "abort"),
    "udf-bg": ("complete", "error", "abort"),
    "ops-bg": ("complete", "error", "abort"),
}
# An aborted query returned incomplete results to the caller, so it counts as a failure
# even though some aborts (user cancel, fail-on-cluster-change) are deliberate.
FAILURE_FIELDS = ("error", "timeout", "abort")
WATCH_FIELDS = ("not_found", "filtered_out")

BATCH_INDEX_RE = re.compile(r"batch-index: batches \((\d+),(\d+),(\d+)\) delays (\d+)")
EARLY_FAIL_RE = re.compile(r"early-fail: demarshal (\d+) tsvc-client (\d+) tsvc-from-proxy (\d+)(?: tsvc-from-proxy-batch-sub (\d+))?")
FD_RE = re.compile(r"fds: proto \((\d+),(\d+),(\d+)\)")
SKEW_RE = re.compile(r"cluster-clock: skew-ms (\d+)")
AVAIL_RE = re.compile(r"data-usage: used-bytes \d+ avail-pct (\d+)")
INPROGRESS_RE = re.compile(r"in-progress: info-q (\d+) rw-hash (\d+) proxy-hash (\d+) tree-gc-q (\d+) long-queries (\d+)")
DEVICE_RE = re.compile(r"free-wblocks (\d+) write \([\d.,]+\) defrag-q (\d+)")
WRITE_Q_RE = re.compile(r"write-q (\d+)")
SYSTEM_RE = re.compile(r"system: total-cpu-pct (\d+)")
NSUP_RE = re.compile(r"nsup-done: non-expirable \d+ expired \((\d+),(\d+)\) evicted \((\d+),(\d+)\) evict-ttl (\d+)")
STOP_WRITES_RE = re.compile(r"stop-writes|hwm[- _]breached")
CLUSTER_SIZE_RE = re.compile(r"CLUSTER-SIZE (\d+)")
MIGRATION_RE = re.compile(
    r"\{(\S+)\} migrations: remaining \(([\d,]+)\) active \(([\d,]+)\) complete-pct ([\d.]+)")
CONFIG_PROTO_FD_RE = re.compile(r"proto-fd-max[:\s]+(\d+)", re.I)
CONFIG_OS_FD_RE = re.compile(
    r"system file descriptor limit:\s*(\d+).*proto-fd-max:\s*(\d+)", re.I)

# {ns} special-errors: key-busy (1234, 40) record-too-big 5678 lost-conflict (256,32)
# Only printed when one of these has occurred. Before 5.6.0 the values were bare
# scalars, and 7.2.0 added the xdr count as a second number, so read numbers loosely.
SPECIAL_ERR_RE = re.compile(r"\{(\S+)\} special-errors: (.*)")
# {ns} retransmits: migration 1 dup-res (1,...,10) repl-ping (1,2) repl-write (1,...,8)
RETRANSMIT_RE = re.compile(r"\{(\S+)\} retransmits: (.*)")
# {ns} dup-res: ask 1234 respond (10,4321)
DUP_RES_RE = re.compile(r"\{(\S+)\} dup-res: ask (\d+) respond \((\d+),(\d+)\)")
FIELD_RE = re.compile(r"([a-z][a-z-]*) (\(?[\d,\s]+\)?)")

# Each number in a special-errors field, in print order.
SPECIAL_LABELS = {
    "key-busy": ("key-busy", "key-busy (xdr)"),
    "record-too-big": ("record-too-big",),
    "lost-conflict": ("lost-conflict", "lost-conflict (xdr)"),
}
# Each number in a retransmits group, in print order (Database Log Reference).
RETRANSMIT_LABELS = {
    "migration": ("migration",),
    "dup-res": ("read", "write", "delete", "udf", "batch-sub-read", "batch-sub-write",
                "batch-sub-delete", "batch-sub-udf", "udf-sub", "ops-sub"),
    "repl-ping": ("read", "batch-sub-read"),
    "repl-write": ("write", "delete", "udf", "batch-sub-write", "batch-sub-delete",
                   "batch-sub-udf", "udf-sub", "ops-sub"),
}

# Counters are cumulative; these are differenced per sample.
CUMULATIVE_GAUGES = {"batch index delays", "early-fail demarshal", "early-fail tsvc-client",
                     "expired objects", "evicted objects"}

# Gauges we chart, with the direction that means trouble.
GAUGE_SPECS = {
    # name:                  (unit,      worse,  warn_at, critical_at, help)
    "client fds in use": ("fds", "high", None, None,
                          "Client connections held open. Compare with proto-fd-max; "
                          "hitting it makes the node refuse new client connections."),
    "clock skew": ("ms", "high", 1000, 20000,
                   "Cluster clock skew. Past 20s an AP namespace with nsup enabled stops writes; "
                   "in strong consistency it trips clock_skew_stop_writes."),
    "device available pct": ("%", "low", 20, 10,
                            "Contiguous device space. Crossing min-avail-pct stops writes "
                            "(AS_ERR_OUT_OF_SPACE, 8)."),
    "device write queue": ("wblocks", "high", 1, 100,
                           "Writes waiting to reach the device. Sustained growth means the device "
                           "is not keeping up and leads to AS_ERR_DEVICE_OVERLOAD (18)."),
    "device defrag queue": ("wblocks", "high", 100, 1000,
                            "Blocks waiting for defragmentation."),
    "rw in progress": ("txns", "high", None, None,
                       "Read/write transactions in flight; a standing backlog means the node is saturated."),
    "proxy in progress": ("txns", "high", None, None,
                          "Transactions being proxied to another node; normally only during migrations."),
    "long queries": ("queries", "high", None, None, "Long-running queries in flight."),
    "batch index delays": ("delays/interval", "high", None, None,
                           "Batch transactions that had to wait for a batch thread."),
    "total cpu pct": ("%", "high", None, None, "Whole-host CPU, summed across cores."),
    "early-fail tsvc-client": ("fails/interval", "high", None, None,
                               "Client transactions rejected before any work was done."),
    "migration partitions remaining": ("partitions", "high", 1, None,
                                       "Partitions still waiting to migrate on this node. "
                                       "Latency and proxy traffic are often elevated until complete-pct reaches 100."),
    "migration complete pct": ("%", "low", 99.0, 95.0,
                               "How far rebalance has progressed on the busiest namespace at this node."),
}

# Curated meaning + action for the messages that actually matter. First match wins.
CATALOG = [
    (re.compile(r"abandoned batch from"), {
        "title": "Batch abandoned before completion",
        "meaning": "The server gave up on a batch parent transaction: the client had already "
                   "timed out or disconnected, so the remaining sub-transactions were dropped.",
        "action": "Usually a client-side socketTimeout shorter than the batch's real service time. "
                  "Correlate with batch-index latency and batch-index delays; check batch-max-buffers-per-queue "
                  "and the client's batch policy.",
        "severity_hint": "warning"}),
    (re.compile(r"could not create heartbeat connection"), {
        "title": "Heartbeat connection to a peer failed",
        "meaning": "This node could not open a heartbeat connection to the listed peer, so that peer "
                   "is a candidate for eviction from the cluster.",
        "action": "Check whether the peer was down or its heartbeat port (3002) was unreachable. "
                  "Clusters that drop a node here will rebalance and migrate.",
        "severity_hint": "warning"}),
    (re.compile(r"Error while connecting socket to"), {
        "title": "Outbound socket connect failed",
        "meaning": "A TCP connect to the listed peer endpoint failed. Paired with a heartbeat or "
                   "fabric warning it identifies which peer went unreachable.",
        "action": "Network or peer-down. Check the peer's asd process and the port in the message "
                  "(3002 heartbeat, 3001 fabric, 3000 client).",
        "severity_hint": "warning"}),
    (re.compile(r"Timeout while connecting"), {
        "title": "Outbound connect timed out",
        "meaning": "A TCP connect did not complete in time, typically to the peer named in the "
                   "adjacent socket/heartbeat warning.",
        "action": "Look for packet loss or a saturated peer rather than a clean refusal.",
        "severity_hint": "warning"}),
    (re.compile(r"queue too deep"), {
        "title": "Device write queue full (AS_ERR_DEVICE_OVERLOAD 18)",
        "meaning": "The namespace write cache is full: pending storage write blocks are not flushing "
                   "to disk fast enough. Master client writes fail with Error Code 18 (device "
                   "overload). Log context may be storage or drv_ssd depending on server version.",
        "action": "Check device write-q / defrag-q ticker lines and iostat; reduce write load, "
                  "tune migrate/defrag, or raise max-write-cache (ensure enough RAM). "
                  "Correlate with client write error counters in Failed transactions.",
        "severity_hint": "critical"}),
    (re.compile(r"refusing client connection - proto-fd-max"), {
        "title": "Client connection limit reached",
        "meaning": "The node is at proto-fd-max and is refusing new client connections. Clients see "
                   "connection errors even though the node is otherwise healthy.",
        "action": "Raise proto-fd-max (and the OS nofile limit), or find the client that is leaking "
                  "connections. Compare with the 'client fds in use' gauge.",
        "severity_hint": "critical"}),
    (re.compile(r"changing state to orphan"), {
        "title": "Node evicted from the cluster",
        "meaning": "The principal dropped this node from the cluster and it became an orphan. An "
                   "orphan node serves no client traffic.",
        "action": "Serious. Find the heartbeat or network fault that preceded it, and expect "
                  "migrations plus a client error spike around this timestamp.",
        "severity_hint": "critical"}),
    (re.compile(r"blocking client transactions in orphan state"), {
        "title": "Client transactions blocked (orphan node)",
        "meaning": "The node is in orphan state and is rejecting client transactions. Clients get "
                   "AS_ERR_UNAVAILABLE (11).",
        "action": "Directly client-visible. Resolve the cluster split; check min-cluster-size.",
        "severity_hint": "critical"}),
    (re.compile(r"could not allocate .*arena stage"), {
        "title": "Index arena allocation failed",
        "meaning": "With index-type flash, the index mount points are out of space.",
        "action": "Add space on the index mounts; may require deleting arena files and running fsck.",
        "severity_hint": "critical"}),
    (re.compile(r"SSL_accept|certificate verify failed|peer did not return"), {
        "title": "TLS handshake failure",
        "meaning": "A client or peer failed the TLS handshake, or closed the connection mid-handshake.",
        "action": "Check client certificates and CA trust on both ends; a timed-out client also "
                  "produces an unexpected-EOF form of this message.",
        "severity_hint": "warning"}),
    (re.compile(r"file descriptor|Too many open files"), {
        "title": "Out of file descriptors",
        "meaning": "The process hit the system file-descriptor limit, which can abort the node.",
        "action": "Raise the nofile limit for the asd service and re-check proto-fd-max.",
        "severity_hint": "critical"}),
    (re.compile(r"stop-writes|hwm.breached"), {
        "title": "Writes stopped or high-water mark breached",
        "meaning": "The namespace crossed a stop-writes or eviction high-water threshold. Writes "
                   "fail with AS_ERR_FORBIDDEN (22) or AS_ERR_OUT_OF_SPACE (8).",
        "action": "Add capacity, lower retention, or raise the threshold. Client writes are failing now.",
        "severity_hint": "critical"}),
    (re.compile(r"is deprecated"), {
        "title": "Deprecated interface in use",
        "meaning": "Something still calls an interface scheduled for removal. Harmless today.",
        "action": "Update the caller (often an old asadm, exporter or monitoring script) before upgrading.",
        "severity_hint": "info"}),
]

CLUSTER_EVENT_PATTERNS = [
    (re.compile(r"clique based evicted nodes at quantum start: (.*)"), "nodes evicted at cluster formation"),
    (re.compile(r"applied cluster size (\d+)"), "applied cluster size"),
    (re.compile(r"applied new cluster key"), "new cluster key"),
    (re.compile(r"changing state to orphan"), "became orphan"),
    (re.compile(r"rebalanced: expected-migrations \(([\d,]+)\)"), "rebalance scheduled migrations"),
    (re.compile(r"allowing (\d+) fill-migrations"), "fill-migrations released"),
    (re.compile(r"migrations: complete"), None),           # too frequent to log as an event
    (re.compile(r"finished clean shutdown"), "clean shutdown"),
    (re.compile(r"SIGTERM received"), "SIGTERM received"),
]

NORMALIZERS = [
    (re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+\b"), "<ip:port>"),
    (re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"), "<ip>"),
    (re.compile(r"\{[^}]*\}"), "{ns}"),
    (re.compile(r"\b[0-9a-f]{10,}\b"), "<node-id>"),
    (re.compile(r"\b\d[\d.,]*\b"), "N"),
]


def normalize(message):
    for rx, sub in NORMALIZERS:
        message = rx.sub(sub, message)
    return message[:200]


def catalog_for(message):
    for rx, info in CATALOG:
        if rx.search(message):
            return info
    return None


class TimeCache:
    """Timestamps repeat dozens of times per ticker interval; parse each once."""

    def __init__(self):
        self.cache = {}

    def epoch(self, text):
        hit = self.cache.get(text)
        if hit is None:
            try:
                hit = calendar.timegm(datetime.datetime.strptime(text, LOG_DT_FMT).timetuple())
            except ValueError:
                hit = 0
            self.cache[text] = hit
        return hit


class Binner:
    """Fixed-width time bins shared by every series in the report."""

    # The default ticker-interval is 10s, so finer bins would just alternate between
    # one sample and none, making every counter series look like a comb.
    MIN_STEP = 10

    def __init__(self, start, end, target_buckets=600):
        span = max(1, (end or start) - start)
        step = max(self.MIN_STEP, span // target_buckets)
        for nice in (10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 21600, 86400):
            if nice >= step:
                step = nice
                break
        self.start = start - (start % step)
        self.step = step
        self.count = max(1, int((end - self.start) // step) + 1)

    def index(self, when):
        slot = int((when - self.start) // self.step)
        return min(max(slot, 0), self.count - 1)

    def axis(self):
        return {"t0": self.start, "step": self.step, "buckets": self.count}


def densify(sparse, buckets, rounding=0):
    out = [0] * buckets
    for slot, value in sparse.items():
        out[slot] = round(value, rounding) if rounding else int(value)
    return out


def scan_log_errors(path, node, window, binner, state):
    """One pass over one log, folding everything into the shared `state`."""
    from_ts, to_ts = window
    clock = state["clock"]
    node_key = node.get("ip") or node.get("name") or path
    lines = 0
    last_size = None

    with open(path, "r", errors="replace") as fh:
        for line in fh:
            lines += 1
            # hist.c dumps are the bulk of a server log and never carry error info.
            if "(hist.c:" in line:
                continue

            stamp = TS_RE.match(line)
            if not stamp:
                continue
            when = clock.epoch(stamp.group(1))
            if (from_ts and when < from_ts) or (to_ts and when > to_ts):
                continue
            slot = binner.index(when)

            if "WARNING" in line or "CRITICAL" in line or ": ERROR (" in line:
                event = SEVERITY_RE.match(line)
                if event:
                    _, severity, context, site, message = event.groups()
                    repeat = REPEAT_RE.match(message)
                    occurrences = 1
                    if repeat:
                        occurrences = int(repeat.group(1))
                        message = message[repeat.end():]
                    key = (severity, context, site, normalize(message))
                    bucket = state["events"].get(key)
                    if bucket is None:
                        bucket = state["events"][key] = {
                            "severity": severity, "context": context, "site": site,
                            "shape": normalize(message), "count": 0, "collapsed": 0,
                            "nodes": collections.Counter(), "namespaces": collections.Counter(),
                            "first": when, "last": when,
                            "example": line.strip()[:300], "series": collections.Counter(),
                        }
                    ns_m = NS_PREFIX_RE.match(message.strip())
                    if ns_m:
                        bucket["namespaces"][ns_m.group(1)] += occurrences
                    bucket["count"] += occurrences
                    bucket["collapsed"] += occurrences - 1
                    bucket["nodes"][node_key] += occurrences
                    bucket["first"] = min(bucket["first"], when)
                    bucket["last"] = max(bucket["last"], when)
                    bucket["series"][slot] += occurrences
                    state["severity_totals"][severity] += occurrences
                    state["severity_series"][severity][slot] += occurrences
                    if severity in ("CRITICAL", "ERROR"):
                        state["node_severity"][node_key]["critical"] += occurrences
                    else:
                        state["node_severity"][node_key]["warning"] += occurrences
                    fd = re.search(r"proto-fd-max (\d+)", message)
                    if fd:
                        state["proto_fd_max"][node_key] = int(fd.group(1))
                continue

            # Startup config lines carry proto-fd-max and the OS nofile ceiling before any
            # refusal WARNING appears — same lines as `grep proto-fd-max aerospike.log`.
            if "(config):" in line:
                proto = CONFIG_PROTO_FD_RE.search(line)
                if proto:
                    state["proto_fd_max"][node_key] = int(proto.group(1))
                oslim = CONFIG_OS_FD_RE.search(line)
                if oslim:
                    state["os_fd_limit"][node_key] = int(oslim.group(1))
                    state["proto_fd_max"][node_key] = int(oslim.group(2))
                continue

            if "(ticker.c:" in line:
                read_ticker(line, node_key, slot, state)
                continue

            if "stripe-" in line or "device-" in line or "/dev/" in line:
                device = DEVICE_RE.search(line)
                if device:
                    gauge(state, "device defrag queue", node_key, slot, int(device.group(2)))
                wq = WRITE_Q_RE.search(line)
                if wq:
                    gauge(state, "device write queue", node_key, slot, int(wq.group(1)))
                continue

            if "nsup-done:" in line:
                nsup = NSUP_RE.search(line)
                if nsup:
                    state["nsup"]["expired"] += int(nsup.group(1))
                    state["nsup"]["evicted"] += int(nsup.group(3))
                    if int(nsup.group(3)):
                        state["nsup"]["evicting_nodes"].add(node_key)
                continue

            for rx, label in CLUSTER_EVENT_PATTERNS:
                if label and rx.search(line):
                    state["cluster_events"].append({
                        "t": when, "node": node_key, "kind": label,
                        "text": re.sub(r"^.*GMT: ", "", line.strip())[:200]})
                    break

    state["lines"] += lines
    state["logs"] += 1
    return last_size


def read_ticker(line, node_key, slot, state):
    size = CLUSTER_SIZE_RE.search(line)
    if size:
        value = int(size.group(1))
        seen = state["cluster_size"][node_key]
        if seen and seen[-1] != value:
            state["cluster_events"].append({
                "t": None, "node": node_key, "kind": "cluster size changed",
                "text": "cluster size %d -> %d" % (seen[-1], value)})
            state["size_changes"][node_key] += 1
        if not seen or seen[-1] != value:
            seen.append(value)
        return

    block = BLOCK_RE.search(line)
    if block:
        namespace, source, rest = block.groups()
        for op, numbers in TUPLE_RE.findall(rest):
            fields = LAYOUT.get(op)
            if not fields:
                continue
            values = [int(v) for v in numbers.split(",")]
            for field, value in zip(fields, values):
                counter_sample(state, namespace, source, op, field, node_key, slot, value)
        return

    special = SPECIAL_ERR_RE.search(line)
    if special:
        namespace, rest = special.groups()
        for name, raw in FIELD_RE.findall(rest):
            labels = SPECIAL_LABELS.get(name)
            if not labels:
                continue
            for label, value in zip(labels, re.findall(r"\d+", raw)):
                counter_sample(state, namespace, "special-errors", label, "error",
                               node_key, slot, int(value))
        return

    retransmit = RETRANSMIT_RE.search(line)
    if retransmit:
        namespace, rest = retransmit.groups()
        for name, raw in FIELD_RE.findall(rest):
            labels = RETRANSMIT_LABELS.get(name)
            if not labels:
                continue
            for label, value in zip(labels, re.findall(r"\d+", raw)):
                op = label if name == "migration" else "%s %s" % (name, label)
                counter_sample(state, namespace, "retransmits", op, "retransmit",
                               node_key, slot, int(value))
        return

    dup = DUP_RES_RE.search(line)
    if dup:
        namespace, ask, with_read, without_read = dup.groups()
        counter_sample(state, namespace, "dup-res", "ask", "requests",
                       node_key, slot, int(ask))
        counter_sample(state, namespace, "dup-res", "respond", "with-read",
                       node_key, slot, int(with_read))
        counter_sample(state, namespace, "dup-res", "respond", "without-read",
                       node_key, slot, int(without_read))
        return

    mig = MIGRATION_RE.search(line)
    if mig:
        namespace, remaining, active, pct = mig.groups()
        rem = sum(int(x) for x in remaining.split(","))
        act = sum(int(x) for x in active.split(","))
        pct_f = float(pct)
        gauge(state, "migration partitions remaining", node_key, slot, rem)
        gauge(state, "migration complete pct", node_key, slot, pct_f)
        if rem > 0 or act > 0 or pct_f < 100.0:
            state["migration_active"][node_key].add(namespace)
        return

    fds = FD_RE.search(line)
    if fds:
        in_use, opened, closed = (int(fds.group(i)) for i in (1, 2, 3))
        gauge(state, "client fds in use", node_key, slot, in_use)
        state["proto_fd_opened"][node_key] = max(state["proto_fd_opened"].get(node_key, 0), opened)
        state["proto_fd_closed"][node_key] = max(state["proto_fd_closed"].get(node_key, 0), closed)
        return
    skew = SKEW_RE.search(line)
    if skew:
        gauge(state, "clock skew", node_key, slot, int(skew.group(1)))
        return
    avail = AVAIL_RE.search(line)
    if avail:
        gauge(state, "device available pct", node_key, slot, int(avail.group(1)))
        return
    progress = INPROGRESS_RE.search(line)
    if progress:
        gauge(state, "rw in progress", node_key, slot, int(progress.group(2)))
        gauge(state, "proxy in progress", node_key, slot, int(progress.group(3)))
        gauge(state, "long queries", node_key, slot, int(progress.group(5)))
        return
    batch = BATCH_INDEX_RE.search(line)
    if batch:
        counter_sample(state, "-", "batch-index", "batches", "error", node_key, slot, int(batch.group(2)))
        counter_sample(state, "-", "batch-index", "batches", "timeout", node_key, slot, int(batch.group(3)))
        counter_sample(state, "-", "batch-index", "batches", "success", node_key, slot, int(batch.group(1)))
        cumulative_gauge(state, "batch index delays", node_key, slot, int(batch.group(4)))
        return
    early = EARLY_FAIL_RE.search(line)
    if early:
        cumulative_gauge(state, "early-fail tsvc-client", node_key, slot, int(early.group(2)))
        return
    system = SYSTEM_RE.search(line)
    if system:
        gauge(state, "total cpu pct", node_key, slot, int(system.group(1)))
        return
    if STOP_WRITES_RE.search(line):
        state["stop_writes"][node_key] += 1


def counter_sample(state, namespace, source, op, field, node_key, slot, value):
    """Difference a cumulative ticker counter into per-bin increments."""
    key = (namespace, source, op, field)
    seen = state["counter_last"].setdefault(key, {})
    previous = seen.get(node_key)
    seen[node_key] = value
    if previous is None:
        return
    delta = value - previous
    if delta < 0:
        # Counter went backwards: asd restarted. One restart rewinds every counter at
        # once, so restarts are counted per time bucket, not per counter.
        state["restarts"][node_key].add(slot)
        return
    if not delta:
        return
    entry = state["counters"].get(key)
    if entry is None:
        entry = state["counters"][key] = {
            "namespace": namespace, "source": source, "op": op, "field": field,
            "total": 0, "nodes": collections.Counter(), "series": collections.Counter()}
    entry["total"] += delta
    entry["nodes"][node_key] += delta
    entry["series"][slot] += delta
    if field in FAILURE_FIELDS:
        state["failure_series"][slot] += delta
        state["node_failures"][node_key] += delta


def gauge(state, name, node_key, slot, value):
    entry = state["gauges"].get(name)
    if entry is None:
        spec = GAUGE_SPECS.get(name, ("", "high", None, None, ""))
        entry = state["gauges"][name] = {
            "name": name, "unit": spec[0], "worse": spec[1],
            "warn_at": spec[2], "critical_at": spec[3], "help": spec[4],
            "min": value, "max": value, "worst_node": node_key, "samples": 0,
            "series": {}, "breaches": 0, "cumulative": name in CUMULATIVE_GAUGES}
    entry["samples"] += 1
    # "worst_node" tracks whichever end of the range is the bad one for this gauge.
    if entry["worse"] == "high":
        entry["min"] = min(entry["min"], value)
        if value > entry["max"]:
            entry["max"], entry["worst_node"] = value, node_key
    else:
        entry["max"] = max(entry["max"], value)
        if value < entry["min"]:
            entry["min"], entry["worst_node"] = value, node_key
    limit = entry["critical_at"] if entry["critical_at"] is not None else entry["warn_at"]
    if limit is not None:
        if (entry["worse"] == "high" and value >= limit) or (entry["worse"] == "low" and value <= limit):
            entry["breaches"] += 1
    slots = entry["series"]
    if entry["worse"] == "high":
        slots[slot] = max(slots.get(slot, value), value)
    else:
        slots[slot] = min(slots.get(slot, value), value)


def cumulative_gauge(state, name, node_key, slot, value):
    """Same idea as counter_sample but charted as a gauge (delays, early-fails)."""
    seen = state["gauge_last"].setdefault(name, {})
    previous = seen.get(node_key)
    seen[node_key] = value
    if previous is None or value < previous:
        return
    gauge(state, name, node_key, slot, value - previous)


def new_state(binner):
    return {
        "clock": TimeCache(),
        "events": {},
        "counters": {},
        "counter_last": {},
        "gauges": {},
        "gauge_last": {},
        "cluster_events": [],
        "cluster_size": collections.defaultdict(list),
        "size_changes": collections.Counter(),
        "restarts": collections.defaultdict(set),
        "stop_writes": collections.Counter(),
        "proto_fd_max": {},
        "os_fd_limit": {},
        "proto_fd_opened": {},
        "proto_fd_closed": {},
        "migration_active": collections.defaultdict(set),
        "severity_totals": collections.Counter(),
        "severity_series": collections.defaultdict(collections.Counter),
        "node_severity": collections.defaultdict(collections.Counter),
        "failure_series": collections.Counter(),
        "node_failures": collections.Counter(),
        "nsup": {"expired": 0, "evicted": 0, "evicting_nodes": set()},
        "lines": 0,
        "logs": 0,
        "binner": binner,
    }


def iso(when):
    if not when:
        return None
    return datetime.datetime.fromtimestamp(when, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def merge_series_lists(series_list, buckets):
    out = [0] * buckets
    for series in series_list:
        for i, value in enumerate(series[:buckets]):
            out[i] += value or 0
    return out


def build_conditions(events, gauges, cluster_events, node_rows, state, buckets):
    """Curated operational findings an operator would grep for (proto-fd-max, stop-writes, …)."""
    out = []
    fd_gauge = next((g for g in gauges if g["name"] == "client fds in use"), None)
    mig_rem = next((g for g in gauges if g["name"] == "migration partitions remaining"), None)
    mig_pct = next((g for g in gauges if g["name"] == "migration complete pct"), None)

    refusal = [e for e in events
               if (e.get("title") == "Client connection limit reached"
                   or "refusing client connection" in (e.get("example") or ""))]
    if refusal:
        total = sum(e["count"] for e in refusal)
        by_node = collections.Counter()
        for e in refusal:
            for n in e["nodes"]:
                by_node[n["node"]] += n["count"]
        merged = merge_series_lists([e["series"] for e in refusal], buckets)
        peak_bucket = max(range(len(merged)), key=lambda i: merged[i]) if any(merged) else 0
        out.append({
            "id": "proto_fd_refusal",
            "severity": "critical",
            "title": "Client connections refused — proto-fd-max",
            "total": total,
            "peak_per_bucket": max(merged) if merged else 0,
            "peak_at": iso(state["binner"].start + peak_bucket * state["binner"].step)
            if any(merged) else None,
            "node_count": len(by_node),
            "nodes": [{"node": n, "count": c,
                       "proto_fd_max": state["proto_fd_max"].get(n)}
                      for n, c in by_node.most_common(20)],
            "first": min(e["first"] for e in refusal if e["first"]),
            "last": max(e["last"] for e in refusal if e["last"]),
            "series": merged,
            "meaning": "The node hit proto-fd-max and logged refusing client connection. "
                       "Each line includes (repeated:N) in the count — that is how many "
                       "connect attempts were turned away in that 10s window.",
            "action": "Compare with the client fds in use gauge and reduce client pool size "
                      "(maxConnsPerNode), or raise proto-fd-max below the OS nofile limit.",
            "fd_limit": min(state["proto_fd_max"].values()) if state["proto_fd_max"] else None,
            "os_fd_limit": min(state["os_fd_limit"].values()) if state["os_fd_limit"] else None,
            "fd_in_use_peak": fd_gauge["max"] if fd_gauge else None,
        })

    queue_ev = [e for e in events
                if e.get("title") == "Device write queue full (AS_ERR_DEVICE_OVERLOAD 18)"
                or QUEUE_TOO_DEEP_RE.search(e.get("example") or "")]
    if queue_ev:
        total = sum(e["count"] for e in queue_ev)
        by_node = collections.Counter()
        by_ns = collections.Counter()
        for e in queue_ev:
            for n in e["nodes"]:
                by_node[n["node"]] += n["count"]
            for ns in e.get("namespaces") or []:
                by_ns[ns["namespace"]] += ns["count"]
        merged = merge_series_lists([e["series"] for e in queue_ev], buckets)
        peak_bucket = max(range(len(merged)), key=lambda i: merged[i]) if any(merged) else 0
        out.append({
            "id": "storage_queue",
            "severity": "critical",
            "title": "Storage write queue too deep — device overload",
            "total": total,
            "peak_per_bucket": max(merged) if merged else 0,
            "peak_at": iso(state["binner"].start + peak_bucket * state["binner"].step)
            if any(merged) else None,
            "node_count": len(by_node),
            "nodes": [{"node": n, "count": c} for n, c in by_node.most_common(20)],
            "namespaces": [{"namespace": ns, "count": c} for ns, c in by_ns.most_common(10)],
            "first": min(e["first"] for e in queue_ev if e["first"]),
            "last": max(e["last"] for e in queue_ev if e["last"]),
            "series": merged,
            "meaning": "Same as grep for 'queue too deep': the disk path cannot drain the write "
                       "cache fast enough. Clients see AerospikeException Error Code 18.",
            "action": "Inspect write-q on device ticker lines; check migrations/defrag load; "
                      "consider max-write-cache, hardware throughput, or lowering write pressure.",
        })

    if not refusal and fd_gauge and state["proto_fd_max"]:
        limit = min(state["proto_fd_max"].values())
        if fd_gauge["max"] >= limit * 0.9:
            out.append({
                "id": "proto_fd_pressure",
                "severity": "warning",
                "title": "Client fds near proto-fd-max (no refusals logged in this window)",
                "total": 0,
                "peak_per_bucket": 0,
                "peak_at": None,
                "node_count": 1,
                "nodes": [{"node": fd_gauge["worst_node"], "count": 0,
                           "proto_fd_max": limit}],
                "first": None, "last": None,
                "series": fd_gauge["series"],
                "meaning": "Ticker fds: proto (in-use, opened, closed) shows the node within "
                           "~10%% of proto-fd-max. Refusals may appear on the next connection burst.",
                "action": "Plan a higher proto-fd-max or lower client connection pools before the next deploy.",
                "fd_limit": limit,
                "os_fd_limit": state["os_fd_limit"].get(fd_gauge["worst_node"]),
                "fd_in_use_peak": fd_gauge["max"],
            })

    stop_ev = [e for e in events if e.get("title") == "Writes stopped or high-water mark breached"]
    stop_ticker = sum(state["stop_writes"].values())
    if stop_ev or stop_ticker:
        total = sum(e["count"] for e in stop_ev) + stop_ticker
        out.append({
            "id": "stop_writes",
            "severity": "critical",
            "title": "Stop-writes or high-water breach",
            "total": total,
            "peak_per_bucket": 0,
            "peak_at": stop_ev[0]["first"] if stop_ev else None,
            "node_count": len(set(n["node"] for e in stop_ev for n in e["nodes"])
                            | set(state["stop_writes"].keys())),
            "nodes": [{"node": n, "count": state["stop_writes"].get(n, 0)}
                      for n in sorted(state["stop_writes"].keys())][:20],
            "first": min(e["first"] for e in stop_ev) if stop_ev else None,
            "last": max(e["last"] for e in stop_ev) if stop_ev else None,
            "series": merge_series_lists([e["series"] for e in stop_ev], buckets) if stop_ev else [0] * buckets,
            "meaning": "Namespace crossed stop-writes-pct, min-avail-pct, memory budget, or a "
                       "high-water mark. Client writes fail with AS_ERR_OUT_OF_SPACE (8) or "
                       "AS_ERR_FORBIDDEN (22).",
            "action": "Free capacity, tune retention/eviction, or raise thresholds. "
                      "Check device available pct and memory-usage tickers.",
        })

    if mig_rem and (mig_rem["max"] > 0 or (mig_pct and mig_pct["min"] < 100.0)):
        active_nodes = sorted(state["migration_active"].keys())
        out.append({
            "id": "migrations",
            "severity": "warning" if mig_pct and mig_pct["min"] >= 95 else "info",
            "title": "Partition migrations in progress",
            "total": int(mig_rem["max"]),
            "peak_per_bucket": mig_rem["max"],
            "peak_at": None,
            "node_count": len(active_nodes),
            "nodes": [{"node": n, "namespaces": sorted(state["migration_active"].get(n, ()))[:6]}
                      for n in active_nodes[:20]],
            "first": None, "last": None,
            "series": mig_rem["series"],
            "meaning": "The migrations ticker showed remaining or active partition work "
                       "(complete-pct below 100). Object counts and latency are mid-rebalance.",
            "action": "Expect elevated proxy traffic and dup-res until complete-pct reaches 100. "
                      "Correlate with cluster events (rebalance, node add/remove).",
            "complete_pct_min": mig_pct["min"] if mig_pct else None,
        })

    orphan_ev = [e for e in events if e.get("title") in (
        "Node evicted from the cluster", "Client transactions blocked (orphan node)")]
    if orphan_ev or any(e["kind"] == "became orphan" for e in cluster_events):
        out.append({
            "id": "cluster_orphan",
            "severity": "critical",
            "title": "Node orphan / eviction",
            "total": sum(e["count"] for e in orphan_ev) or len(
                [e for e in cluster_events if e.get("kind") == "became orphan"]),
            "peak_per_bucket": 0,
            "peak_at": cluster_events[0]["iso"] if cluster_events else None,
            "node_count": len(set(n["node"] for e in orphan_ev for n in e["nodes"])),
            "nodes": [],
            "first": min(e["first"] for e in orphan_ev) if orphan_ev else None,
            "last": max(e["last"] for e in orphan_ev) if orphan_ev else None,
            "series": merge_series_lists([e["series"] for e in orphan_ev], buckets) if orphan_ev else [0] * buckets,
            "meaning": "A node was evicted or is serving as an orphan. Clients on that node "
                       "see AS_ERR_UNAVAILABLE (11).",
            "action": "Fix heartbeat/network; restore cluster size before relying on the node.",
        })

    order = {"critical": 0, "warning": 1, "info": 2}
    out.sort(key=lambda c: (order.get(c["severity"], 2), -c["total"]))
    return out


def analyze_errors(entries, window, coverage):
    """entries: [(path, node)].  window: (from_epoch|None, to_epoch|None)."""
    started = time.time()
    start = window[0] or coverage[0]
    end = window[1] or coverage[1]
    binner = Binner(start, end)
    state = new_state(binner)

    for path, node in entries:
        scan_log_errors(path, node, window, binner, state)

    buckets = binner.count
    events = []
    for key, bucket in state["events"].items():
        info = catalog_for(bucket["example"]) or {}
        events.append({
            "id": "%s-%s-%d" % (bucket["context"], bucket["site"].replace(":", "_"), len(events)),
            "severity": bucket["severity"],
            "context": bucket["context"],
            "site": bucket["site"],
            "shape": bucket["shape"],
            "count": bucket["count"],
            "collapsed": bucket["collapsed"],
            "node_count": len(bucket["nodes"]),
            "nodes": [{"node": n, "count": c} for n, c in bucket["nodes"].most_common(20)],
            "namespaces": [{"namespace": ns, "count": c}
                           for ns, c in bucket.get("namespaces", {}).most_common(10)],
            "first": iso(bucket["first"]),
            "last": iso(bucket["last"]),
            "example": bucket["example"],
            "series": densify(bucket["series"], buckets),
            "title": info.get("title"),
            "meaning": info.get("meaning"),
            "action": info.get("action"),
            "impact": info.get("severity_hint") or bucket["severity"].lower(),
        })
    # Rank by operator impact, not by log severity: Aerospike logs a client-visible
    # connection refusal at WARNING and a deprecation notice at the same level.
    impact_rank = {"critical": 0, "error": 1, "warning": 2, "info": 3}
    events.sort(key=lambda e: (impact_rank.get(e["impact"], 2), -e["count"]))

    counters = []
    success_by_op = {}
    for key, entry in state["counters"].items():
        if entry["field"] in ("success", "complete"):
            success_by_op[(entry["namespace"], entry["source"], entry["op"])] = entry["total"]
    for key, entry in state["counters"].items():
        field = entry["field"]
        if field in ("success", "complete", "read", "write", "delete"):
            continue
        base = success_by_op.get((entry["namespace"], entry["source"], entry["op"]), 0)
        total = entry["total"]
        # tsvc has no success counter of its own, so a percentage there would always
        # read 100% and mean nothing.
        rate = round(100.0 * total / (total + base), 4) if base else None
        counters.append({
            "namespace": entry["namespace"], "source": entry["source"],
            "op": entry["op"], "field": field, "total": total,
            "success": base,
            "rate_pct": rate,
            "class": "failure" if field in FAILURE_FIELDS else "watch",
            "node_count": len(entry["nodes"]),
            "nodes": [{"node": n, "count": c} for n, c in entry["nodes"].most_common(20)],
            "series": densify(entry["series"], buckets),
        })
    counters.sort(key=lambda c: (0 if c["class"] == "failure" else 1, -c["total"]))

    # proto-fd-max is a per-node config value only visible once a node complains,
    # so the fd gauge gets its threshold after the scan rather than up front.
    fd_limit = min(state["proto_fd_max"].values()) if state["proto_fd_max"] else None
    if fd_limit and "client fds in use" in state["gauges"]:
        fd_entry = state["gauges"]["client fds in use"]
        fd_entry["warn_at"] = int(fd_limit * 0.8)
        fd_entry["critical_at"] = fd_limit
        fd_entry["help"] += " This cluster reports proto-fd-max %d." % fd_limit
        fd_entry["breaches"] = sum(1 for v in fd_entry["series"].values() if v >= fd_limit)

    gauges = []
    for name, entry in state["gauges"].items():
        series = entry["series"]
        filler = 0 if entry["worse"] == "high" else entry["max"]
        dense = [filler] * buckets
        for slot, value in series.items():
            dense[slot] = value
        gauges.append({
            "name": name, "unit": entry["unit"], "worse": entry["worse"],
            "min": entry["min"], "max": entry["max"], "worst_node": entry["worst_node"],
            "warn_at": entry["warn_at"], "critical_at": entry["critical_at"],
            "breaches": entry["breaches"], "samples": entry["samples"],
            "help": entry["help"], "series": dense,
            "flat": entry["min"] == entry["max"],
        })
    order = list(GAUGE_SPECS.keys())
    gauges.sort(key=lambda g: (order.index(g["name"]) if g["name"] in order else 99))

    for event in state["cluster_events"]:
        if event["t"]:
            event["iso"] = iso(event["t"])
    cluster_events = [e for e in state["cluster_events"] if e.get("iso")]
    cluster_events.sort(key=lambda e: e["t"])

    node_rows = []
    for node_key in set(list(state["node_severity"]) + list(state["node_failures"])
                        + list(state["cluster_size"])):
        severity = state["node_severity"].get(node_key, {})
        node_rows.append({
            "node": node_key,
            "critical": severity.get("critical", 0),
            "warning": severity.get("warning", 0),
            "failures": state["node_failures"].get(node_key, 0),
            "size_changes": state["size_changes"].get(node_key, 0),
            "restarts": len(state["restarts"].get(node_key, ())),
            "cluster_sizes": sorted(set(state["cluster_size"].get(node_key, []))),
            "proto_fd_max": state["proto_fd_max"].get(node_key),
            "os_fd_limit": state["os_fd_limit"].get(node_key),
        })
    node_rows.sort(key=lambda n: (-n["critical"], -n["failures"], -n["warning"], n["node"]))

    conditions = build_conditions(events, gauges, cluster_events, node_rows, state, buckets)
    refusal_series = next((c["series"] for c in conditions if c["id"] == "proto_fd_refusal"), None)
    if refusal_series is None:
        refusal_series = [0] * buckets
    storage_queue_series = next((c["series"] for c in conditions if c["id"] == "storage_queue"), None)
    if storage_queue_series is None:
        storage_queue_series = [0] * buckets

    return {
        "axis": binner.axis(),
        "window": {"from": iso(window[0]), "to": iso(window[1]),
                   "start": iso(start), "end": iso(end),
                   "mode": "window" if window[0] or window[1] else "complete log"},
        "scanned": {"logs": state["logs"], "lines": state["lines"],
                    "seconds": round(time.time() - started, 2)},
        "severity_totals": dict(state["severity_totals"]),
        "severity_series": {name: densify(series, buckets)
                            for name, series in state["severity_series"].items()},
        "failure_series": densify(state["failure_series"], buckets),
        "events": events,
        "counters": counters,
        "gauges": gauges,
        "cluster_events": cluster_events[:400],
        "cluster_event_count": len(cluster_events),
        "nodes": node_rows,
        "nsup": {"expired": state["nsup"]["expired"], "evicted": state["nsup"]["evicted"],
                 "evicting_nodes": sorted(state["nsup"]["evicting_nodes"])},
        "restarts": sum(len(slots) for slots in state["restarts"].values()),
        "conditions": conditions,
        "connection_refusal_series": refusal_series,
        "storage_queue_series": storage_queue_series,
        "headline": headline(events, counters, gauges, cluster_events, node_rows, state, conditions),
    }


def headline(events, counters, gauges, cluster_events, node_rows, state, conditions=()):
    """A few sentences an operator can act on, worst first."""
    out = []
    for cond in conditions:
        if cond["id"] == "proto_fd_refusal":
            out.append("%s — %s refused connection attempt(s) on %d node(s), peak %s in one "
                       "%ss bucket at %s GMT (proto-fd-max %s%s)" % (
                           cond["title"], f"{cond['total']:,}", cond["node_count"],
                           f"{cond['peak_per_bucket']:,}", state["binner"].step,
                           cond["peak_at"] or cond["last"],
                           cond["fd_limit"] or "?",
                           ", OS limit %s" % cond["os_fd_limit"] if cond.get("os_fd_limit") else ""))
            break
    for cond in conditions:
        if cond["id"] == "storage_queue":
            ns_part = ""
            if cond.get("namespaces"):
                ns_part = " in {%s}" % ", ".join(
                    "%s (%s)" % (n["namespace"], f"{n['count']:,}")
                    for n in cond["namespaces"][:3])
            out.append("%s — %s overload signal(s) on %d node(s)%s, peak %s in one %ss bucket" % (
                cond["title"], f"{cond['total']:,}", cond["node_count"], ns_part,
                f"{cond['peak_per_bucket']:,}", state["binner"].step))
            break
    for cond in conditions:
        if cond["id"] == "stop_writes":
            out.append("%s — %s signal(s) across %d node(s)" % (
                cond["title"], f"{cond['total']:,}", cond["node_count"]))
            break
    for cond in conditions:
        if cond["id"] == "migrations":
            out.append("%s — up to %s partitions remaining (lowest complete-pct %.2f%%)" % (
                cond["title"], f"{cond['total']:,}", cond.get("complete_pct_min") or 100))
            break
    serious = [e for e in events if e["impact"] in ("critical", "error")]
    for top in serious[:2]:
        out.append("%s — %s occurred %s time(s) on %d node(s), %s to %s GMT" % (
            top["title"] or top["shape"], top["severity"], f"{top['count']:,}",
            top["node_count"], top["first"], top["last"]))
    failures = [c for c in counters if c["class"] == "failure" and c["total"]]
    if failures:
        top = failures[0]
        share = " (%.4f%% of %s attempts)" % (top["rate_pct"], f"{top['total'] + top['success']:,}") \
            if top["rate_pct"] is not None else ""
        out.append("%s %s %s totalled %s across %d node(s)%s" % (
            top["source"], top["op"], top["field"], f"{top['total']:,}",
            top["node_count"], share))
    elif events:
        out.append("no transaction error or timeout counter moved in this window")

    # These three are specific enough to name outright: each points at one cause and one
    # fix, rather than at "something went wrong somewhere".
    special = [c for c in counters if c["source"] == "special-errors" and c["total"]]
    for entry in special[:3]:
        out.append("%s rejected %s operation(s) on %d node(s) in {%s} — %s" % (
            entry["op"], f"{entry['total']:,}", entry["node_count"], entry["namespace"],
            {"key-busy": "one record is taking more concurrent writes than "
                         "transaction-pending-limit permits",
             "record-too-big": "writes are exceeding max-record-size",
             "lost-conflict": "client writes are losing to XDR writes under bin convergence"}
            .get(entry["op"].split(" (")[0], "see the counter detail")))

    queries = [c for c in counters
               if c["source"] in ("pi-query", "si-query", "scan") and c["class"] == "failure"]
    if queries:
        total = sum(c["total"] for c in queries)
        out.append("%s query job(s) did not finish cleanly (%s)" % (
            f"{total:,}", ", ".join("%s %s %s" % (c["source"], c["op"], c["field"])
                                    for c in queries[:3])))

    retransmits = [c for c in counters if c["source"] == "retransmits" and c["total"]]
    if retransmits:
        top = retransmits[0]
        out.append("%s inter-node message(s) were retransmitted after a timeout, most of them "
                   "%s on %d node(s) — check the fabric and the receiving nodes" % (
                       f"{sum(c['total'] for c in retransmits):,}", top["op"], top["node_count"]))
    orphans = [e for e in cluster_events if e["kind"] in ("became orphan", "nodes evicted at cluster formation")]
    if orphans:
        out.append("cluster membership changed: %d eviction/orphan event(s), first at %s GMT on %s" % (
            len(orphans), orphans[0]["iso"], orphans[0]["node"]))
    breached = [g for g in gauges if g["breaches"]]
    for entry in breached[:2]:
        limit = entry["critical_at"] if entry["critical_at"] is not None else entry["warn_at"]
        out.append("%s crossed %s %s on %s (worst %s)" % (
            entry["name"], "<=" if entry["worse"] == "low" else ">=", limit,
            entry["worst_node"], entry["max"] if entry["worse"] == "high" else entry["min"]))
    fd = [n for n in node_rows if n["proto_fd_max"]]
    fd_gauge = next((g for g in gauges if g["name"] == "client fds in use"), None)
    reported = {entry["name"] for entry in breached[:2]}
    if (fd and fd_gauge and "client fds in use" not in reported
            and fd_gauge["max"] >= fd[0]["proto_fd_max"] * 0.95):
        out.append("client file descriptors reached %s against a proto-fd-max of %s on %s" % (
            f"{fd_gauge['max']:,}", f"{fd[0]['proto_fd_max']:,}", fd_gauge["worst_node"]))
    if state["restarts"]:
        out.append("counters reset on %d node(s), which means the service restarted mid-window: %s" % (
            len(state["restarts"]), ", ".join(sorted(state["restarts"])[:5])))
    if not events and not failures:
        watched = [c for c in counters if c["total"]]
        out.insert(0, "no warnings, errors or failed transactions in this window"
                   + (", though %d not-found/filtered-out counter(s) moved (both are normal outcomes)"
                      % len(watched) if watched else ""))
    return out
