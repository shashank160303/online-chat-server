#!/usr/bin/env python3
"""Load test for the chat server.

What it does
  1. Connects N clients (a few at a time), each registers a name and joins a room.
  2. Every client sends a number of timestamped messages; the server fans each one
     out to the other members of that client's room.
  3. Reports delivery loss, latency percentiles, and the SERVER's own resource use
     (threads, memory, CPU seconds), read from /proc.

Usage
  python3 loadtest.py --server ./chat_server --clients 1000
  python3 loadtest.py --server ./chat_server_threads --clients 1000
  python3 loadtest.py --port 8080 --pid 1234 --clients 500     # test a server you started yourself

Notes
  * Linux/WSL only (uses /proc). The load generator is Python, so on a small
    machine it can become the bottleneck: compare the SERVER CPU numbers, which
    do not depend on how fast the generator is.
  * Run it a few times and report typical values, not the single best run.
"""
import argparse
import asyncio
import os
import random
import resource
import signal
import socket
import subprocess
import sys
import time
from collections import Counter


# ----------------------------------------------------------- /proc helpers

def proc_status(pid):
    """Returns (rss_mb, threads) of a process."""
    rss = threads = None
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    rss = int(line.split()[1]) / 1024
                elif line.startswith("Threads:"):
                    threads = int(line.split()[1])
    except OSError:
        pass
    return rss, threads


def cpu_seconds(pid):
    """User+system CPU seconds the process has consumed so far."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            fields = f.read().rsplit(")", 1)[1].split()
        ticks = int(fields[11]) + int(fields[12])           # utime + stime
        return ticks / os.sysconf("SC_CLK_TCK")
    except (OSError, IndexError, ValueError):
        return None


def raise_fd_limit(needed):
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    want = needed if hard == resource.RLIM_INFINITY else min(hard, needed)
    if soft < want:
        resource.setrlimit(resource.RLIMIT_NOFILE, (want, hard))
    return resource.getrlimit(resource.RLIMIT_NOFILE)[0]


# ----------------------------------------------------------------- clients

class Stats:
    def __init__(self):
        self.failures = 0
        self.first_error = None
        self.sent = 0
        self.received = 0
        self.latencies = []          # milliseconds


class Session:
    def __init__(self, i, reader, writer):
        self.i, self.reader, self.writer = i, reader, writer


async def handshake(i, args, sem, stats):
    """Connect, register as u<i>, join room r<i // room_size>."""
    async with sem:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(args.host, args.port), 30)
            writer.write(f"u{i}\n".encode())
            line = await asyncio.wait_for(reader.readline(), 30)
            if line.strip() != b"OK":
                raise RuntimeError(f"registration answered {line!r}")
            writer.write(f"/join r{i // args.room_size}\n".encode())
            while True:                                      # skip announcements until confirmed
                line = await asyncio.wait_for(reader.readline(), 30)
                if not line:
                    raise RuntimeError("connection closed during join")
                if line.startswith(b"You joined"):
                    return Session(i, reader, writer)
        except Exception as e:                               # noqa: BLE001
            stats.failures += 1
            stats.first_error = stats.first_error or repr(e)
            return None


async def receiver(s, stats):
    try:
        while True:
            line = await s.reader.readline()
            if not line:
                return
            k = line.find(b"LT|")
            if k < 0:
                continue                                     # announcements etc.
            try:
                sent_ns = int(line[k:].rstrip().split(b"|")[3])
            except (IndexError, ValueError):
                continue
            stats.received += 1
            stats.latencies.append((time.perf_counter_ns() - sent_ns) / 1e6)
    except (ConnectionError, asyncio.CancelledError):
        return


async def sender(s, args, stats):
    pad = "x" * args.size
    await asyncio.sleep(random.random() * args.interval)     # spread the start times
    try:
        for seq in range(args.msgs):
            s.writer.write(f"LT|{s.i}|{seq}|{time.perf_counter_ns()}|{pad}\n".encode())
            await s.writer.drain()
            stats.sent += 1
            await asyncio.sleep(args.interval)
    except ConnectionError:
        stats.failures += 1


def percentile(sorted_vals, p):
    if not sorted_vals:
        return float("nan")
    return sorted_vals[min(len(sorted_vals) - 1, int(len(sorted_vals) * p / 100))]


# -------------------------------------------------------------------- main

def wait_for_port(host, port, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        try:
            socket.create_connection((host, port), 0.5).close()
            return True
        except OSError:
            time.sleep(0.1)
    return False


async def run(args):
    server = None
    pid = args.pid
    if args.server:
        server = subprocess.Popen([args.server, str(args.port), os.devnull, "--quiet"],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        pid = server.pid
        if not wait_for_port(args.host, args.port):
            sys.exit("server did not start listening")
        time.sleep(0.3)

    stats = Stats()
    sem = asyncio.Semaphore(args.handshakes)
    baseline_cpu = cpu_seconds(pid) if pid else None
    n_rooms = (args.clients + args.room_size - 1) // args.room_size

    print(f"Load test: {args.clients} clients, {n_rooms} rooms of up to {args.room_size}, "
          f"{args.msgs} msgs/client, {args.interval}s between messages, {args.size}B padding")

    # ---- phase 1: connect + register + join
    t0 = time.perf_counter()
    results = await asyncio.gather(*(handshake(i, args, sem, stats) for i in range(args.clients)))
    sessions = [r for r in results if r]
    connect_time = time.perf_counter() - t0
    rss_conn, threads_conn = proc_status(pid) if pid else (None, None)
    cpu_conn = (cpu_seconds(pid) - baseline_cpu) if pid else None
    print(f"connected      : {len(sessions)}/{args.clients} in {connect_time:.2f}s "
          f"({len(sessions) / connect_time:.0f} clients/s), {stats.failures} failed"
          + (f"  first error: {stats.first_error}" if stats.first_error else ""))

    # ---- phase 2: chat traffic
    sizes = Counter(s.i // args.room_size for s in sessions)
    expected = sum(args.msgs * n * (n - 1) for n in sizes.values())
    rx_tasks = [asyncio.create_task(receiver(s, stats)) for s in sessions]
    cpu_before = cpu_seconds(pid) if pid else None
    t1 = time.perf_counter()
    await asyncio.gather(*(sender(s, args, stats) for s in sessions))

    last, last_change = stats.received, time.perf_counter()
    while stats.received < expected:                         # wait for stragglers
        await asyncio.sleep(0.1)
        now = time.perf_counter()
        if stats.received != last:
            last, last_change = stats.received, now
        elif now - last_change > args.drain:
            break
    traffic_time = time.perf_counter() - t1
    cpu_traffic = (cpu_seconds(pid) - cpu_before) if pid else None
    rss_end, threads_end = proc_status(pid) if pid else (None, None)

    lat = sorted(stats.latencies)
    loss = 100.0 * (expected - stats.received) / expected if expected else 0.0
    print(f"messages       : sent {stats.sent}, deliveries expected {expected}, "
          f"received {stats.received} ({loss:.2f}% loss)")
    print(f"throughput     : {stats.received / traffic_time:.0f} deliveries/s over {traffic_time:.1f}s")
    print(f"latency (ms)   : p50 {percentile(lat, 50):.1f}   p95 {percentile(lat, 95):.1f}   "
          f"p99 {percentile(lat, 99):.1f}   max {lat[-1] if lat else float('nan'):.1f}")
    if pid and threads_end is not None:
        print(f"server         : {threads_end} thread(s), {rss_end:.0f} MB RSS (at peak), "
              f"CPU {cpu_conn:.2f}s connecting + {cpu_traffic:.2f}s for traffic")

    # ---- cleanup
    for s in sessions:
        s.writer.close()
    for t in rx_tasks:
        t.cancel()
    if server:
        server.send_signal(signal.SIGTERM)
        try:
            code = server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            code = "killed"
        print(f"server exit    : {code}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--server", help="path of a server binary to start (and stop) for the test")
    ap.add_argument("--pid", type=int, help="pid of an already running server (for /proc stats)")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=18080)
    ap.add_argument("--clients", type=int, default=1000)
    ap.add_argument("--room-size", type=int, default=20, help="clients per room (fan-out = size-1)")
    ap.add_argument("--msgs", type=int, default=5, help="messages sent per client")
    ap.add_argument("--interval", type=float, default=0.5, help="seconds between a client's messages")
    ap.add_argument("--size", type=int, default=0, help="extra payload bytes per message")
    ap.add_argument("--handshakes", type=int, default=50, help="simultaneous connection setups")
    ap.add_argument("--drain", type=float, default=3.0, help="seconds without progress before giving up")
    args = ap.parse_args()

    limit = raise_fd_limit(args.clients + 200)
    if limit < args.clients + 100:
        print(f"warning: file descriptor limit is {limit}; run `ulimit -n 65536` first", file=sys.stderr)
    asyncio.run(run(args))


if __name__ == "__main__":
    main()