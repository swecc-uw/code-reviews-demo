import socket
import argparse
import time
from datetime import datetime


def scan_ports(host, ports):
    open_ports = []

    start_time = time.time()
    attempts = 0
    errors = 0

    for port in ports:
        attempts += 1
        print(attempts)
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            s.connect((host, port))
            open_ports.append(port)
            s.close()
        except socket.error:
            print("error")
            errors += 1
            pass
        finally:
            print('finally')
            s.close()

    scan_duration = time.time() - start_time
    stats = {
        "total_ports": attempts,
        "open_ports": len(open_ports),
        "closed_ports": errors - len(open_ports),
        "scan_duration": scan_duration,
        "ports_per_second": attempts / scan_duration if scan_duration > 0 else 0,
    }

    return open_ports, stats


def main(args):
    hosts = args.hosts
    ports = args.ports
    verbose = args.verbose

    open_ports = {}
    all_stats = {}
    total_start_time = time.time()

    print(f"\n[*] Scan started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[*] Scanning {len(hosts)} host(s) across {len(ports)} port(s)")

    for host in hosts:
        print(f"\n[+] Scanning host: {host}")
        open_ports[host], all_stats[host] = scan_ports(host, ports)

        if len(open_ports[host]) > 0:
            print(f"    [+] Found {len(open_ports[host])} open port(s) on {host}:")
            for port in open_ports[host]:
                print(f"        - Port {port} is open")
        else:
            print(f"    [-] No open ports found on {host}")

        if verbose:
            stats = all_stats[host]
            print(f"\n    [*] Host scan statistics:")
            print(f"        - Scanned ports: {stats['total_ports']}")
            print(f"        - Open ports: {stats['open_ports']}")
            print(f"        - Closed ports: {stats['closed_ports']}")
            print(f"        - Scan duration: {stats['scan_duration']:.2f} seconds")
            print(f"        - Scan speed: {stats['ports_per_second']:.2f} ports/second")

    total_duration = time.time() - total_start_time
    total_ports_scanned = sum(stats["total_ports"] for stats in all_stats.values())
    total_open_ports = sum(stats["open_ports"] for stats in all_stats.values())

    print(f"\n[*] Scan completed in {total_duration:.2f} seconds")
    print(f"[*] Total ports scanned: {total_ports_scanned}")
    print(f"[*] Total open ports found: {total_open_ports}")
    print(
        f"[*] Average scan speed: {total_ports_scanned/total_duration:.2f} ports/second"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scan ports on a host")
    parser.add_argument(
        "-H",
        "--hosts",
        nargs="+",
        type=str,
        default=["localhost"],
        help="Hosts to scan (default: localhost)",
    )

    parser.add_argument(
        "-p",
        "--ports",
        nargs="+",
        type=int,
        default=[80, 443],
        help="Ports to scan (default: 80, 443)",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show detailed statistics for each host",
    )



    main(parser.parse_args())
