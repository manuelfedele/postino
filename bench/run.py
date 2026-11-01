"""Measures Postino, and optionally a server that speaks RESP, with the same
client on the same machine, and writes the medians to bench/results.json.

    python3 bench/run.py --compare "<command>" --compare-label "<name>"

The command starts the comparison server in the foreground. In it, {port} is
replaced with a free port and {dir} with an empty data directory. Both servers
take the same requests from the same client (examples/bench.rs): each client
holds one connection and sends one command at a time.
"""

import argparse
import json
import platform
import random
import shlex
import socket
import statistics
import subprocess
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
POSTINO = ROOT / "target" / "release" / "postino"
CLIENT = ROOT / "target" / "release" / "examples" / "bench"
CLIENTS = [1, 2, 4, 8, 16, 32, 64]


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def start_postino(data):
    """Starts Postino on a port the system picks; returns it and its URL."""
    server = subprocess.Popen(
        [POSTINO, "serve", "--port", "0", "--data-dir", data],
        stderr=subprocess.PIPE,
        text=True,
    )
    banner = server.stderr
    assert banner is not None
    for line in banner:
        words = line.split()
        if words[:1] == ["api"]:
            url = words[1].removesuffix("/api")
            break
    else:
        raise SystemExit("postino exited before it listened")
    # Read on, so the server never blocks writing to a full pipe.
    threading.Thread(target=banner.read, daemon=True).start()
    return server, url


def start_compared(command, data):
    """Starts the comparison server and waits until it answers PING."""
    port = free_port()
    argv = shlex.split(command.format(port=port, dir=data))
    server = subprocess.Popen(
        argv, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1) as conn:
                conn.sendall(b"*1\r\n$4\r\nPING\r\n")
                if conn.recv(64).startswith(b"+PONG"):
                    return server, f"resp://127.0.0.1:{port}"
        except OSError:
            time.sleep(0.05)
    server.kill()
    raise SystemExit(f"the comparison server did not answer: {argv}")


def measure(target, op, clients, seconds, extra=()):
    out = subprocess.run(
        [CLIENT, target, "--op", op, "--clients", str(clients)]
        + ["--seconds", str(seconds), "--warmup", "1", *extra],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(out.stdout)


def median_run(runs):
    """The median of each figure across repeated runs."""
    keys = runs[0]["latency_us"].keys()
    return {
        "throughput": statistics.median(r["throughput"] for r in runs),
        "errors": sum(r["errors"] for r in runs),
        "latency_us": {
            k: statistics.median(r["latency_us"][k] for r in runs) for k in keys
        },
    }


def sysctl(name):
    try:
        return subprocess.run(
            ["sysctl", "-n", name], capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def machine():
    performance, efficiency = (
        sysctl("hw.perflevel0.physicalcpu"),
        sysctl("hw.perflevel1.physicalcpu"),
    )
    return {
        "cpu": sysctl("machdep.cpu.brand_string") or platform.processor(),
        "cores": int(sysctl("hw.ncpu") or 0) or None,
        "performance_cores": int(performance) if performance else None,
        "efficiency_cores": int(efficiency) if efficiency else None,
        "os": f"{platform.system()} {platform.mac_ver()[0] or platform.release()}",
    }


def main():
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("--compare", help="command that starts a RESP server")
    parser.add_argument("--compare-label", default="compared")
    parser.add_argument("--seconds", type=float, default=3)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--out", type=Path, default=ROOT / "bench" / "results.json")
    args = parser.parse_args()

    subprocess.run(
        ["cargo", "build", "--release", "--bin", "postino", "--example", "bench"],
        cwd=ROOT,
        check=True,
    )
    version = subprocess.run(
        [POSTINO, "--version"], capture_output=True, text=True, check=True
    ).stdout.split()[-1]
    servers, targets = [], {}
    with tempfile.TemporaryDirectory() as postino_dir, tempfile.TemporaryDirectory() as compared_dir:
        try:
            server, url = start_postino(postino_dir)
            servers.append(server)
            targets["Postino"] = url
            if args.compare:
                server, url = start_compared(args.compare, compared_dir)
                servers.append(server)
                targets[args.compare_label] = url
            for target in targets.values():
                measure(target, "get", 16, 0.1, ["--preload"])
            points = [
                (name, op, clients)
                for name in targets
                for op in ["set", "get"]
                for clients in CLIENTS
            ]
            runs = {point: [] for point in points}
            for n in range(args.repeat):
                # Every round visits every point in a new order, so a stretch
                # of other work on the machine slows one run of a point, not
                # all of them, and the median leaves it out.
                random.shuffle(points)
                for name, op, clients in points:
                    runs[name, op, clients].append(
                        measure(targets[name], op, clients, args.seconds)
                    )
                print(f"round {n + 1} of {args.repeat}", flush=True)
            results = []
            for (name, op, clients), measured in runs.items():
                row = {"server": name, "op": op, "clients": clients}
                results.append(row | median_run(measured))
                print(
                    f"{name:>12} {op} {clients:>2} clients: "
                    f"{results[-1]['throughput']:>9,.0f}/s"
                )
        finally:
            for server in servers:
                server.terminate()
                server.wait()
    report = {
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "postino": version,
        "compared": args.compare_label if args.compare else None,
        "machine": machine(),
        "workload": {
            "keys": 100_000,
            "value_bytes": 64,
            "seconds": args.seconds,
            "repeat": args.repeat,
        },
        "results": results,
    }
    args.out.write_text(json.dumps(report, indent=2) + "\n")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
