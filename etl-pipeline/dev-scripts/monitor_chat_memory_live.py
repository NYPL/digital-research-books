#!/usr/bin/env python3
"""
Live memory monitoring for the chat endpoint during API requests.

This script can be run alongside the API server to monitor memory usage in real-time.
It polls the process memory and can detect spikes.

Usage:
    # In one terminal, start your API server
    make run

    # In another terminal, monitor memory
    python scripts/monitor_chat_memory_live.py --pid <API_PROCESS_PID>

    # Or auto-detect Flask process
    python scripts/monitor_chat_memory_live.py --auto-detect

    # Monitor and save data to CSV
    python scripts/monitor_chat_memory_live.py --auto-detect --output memory_data.csv
"""

import argparse
import psutil
import time
import sys
import csv
from datetime import datetime
from pathlib import Path


def find_api_server_process():
    """Find the Flask API process."""
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            cmdline = proc.info["cmdline"]
            if cmdline and ("main.py -p APIProcess" in " ".join(cmdline)):
                print(f"Found process {proc.name()} with id {proc.pid}")
                return proc.pid
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None


def format_bytes(bytes_value):
    """Format bytes to human readable string."""
    for unit in ["B", "KB", "MB", "GB"]:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.2f} TB"


def monitor_memory(pid, interval=0.5, threshold_mb=50, output_file=None):
    """
    Monitor memory usage of a process.

    Args:
        pid: Process ID to monitor
        interval: Polling interval in seconds
        threshold_mb: Alert threshold for memory spikes (MB)
        output_file: Optional CSV file to save data
    """
    try:
        process = psutil.Process(pid)
    except psutil.NoSuchProcess:
        print(f"Error: Process {pid} not found")
        sys.exit(1)

    print(f"Monitoring process {pid}: {process.name()}")
    print(f"Polling interval: {interval}s")
    print(f"Alert threshold: {threshold_mb} MB increase")
    print(f"{'=' * 80}\n")

    # Initialize CSV if requested
    csv_writer = None
    csv_file = None
    if output_file:
        csv_file = open(output_file, "w", newline="")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(
            [
                "timestamp",
                "rss_mb",
                "vms_mb",
                "percent",
                "num_threads",
                "num_fds",
                "cpu_percent",
            ]
        )
        print(f"Writing data to {output_file}\n")

    baseline_rss = None
    previous_rss = None
    max_rss = 0
    samples = []

    try:
        while True:
            try:
                # Get memory info
                mem_info = process.memory_info()
                mem_percent = process.memory_percent()
                num_threads = process.num_threads()
                cpu_percent = process.cpu_percent(interval=0.1)

                # Get file descriptors (Unix only)
                try:
                    num_fds = process.num_fds()
                except (AttributeError, psutil.AccessDenied):
                    num_fds = 0

                rss_mb = mem_info.rss / (1024**2)
                vms_mb = mem_info.vms / (1024**2)

                # Track baseline
                if baseline_rss is None:
                    baseline_rss = rss_mb

                # Track max
                if rss_mb > max_rss:
                    max_rss = rss_mb

                # Calculate delta
                delta = rss_mb - previous_rss if previous_rss else 0
                delta_from_baseline = rss_mb - baseline_rss

                # Prepare display
                timestamp = datetime.now().strftime("%H:%M:%S")

                # Color coding for terminal
                alert = ""
                if delta > threshold_mb:
                    alert = "⚠️  SPIKE!"
                elif delta > threshold_mb / 2:
                    alert = "⚡"

                # Display
                status = (
                    f"[{timestamp}] "
                    f"RSS: {rss_mb:7.2f} MB "
                    f"(Δ: {delta:+6.2f} MB) "
                    f"| VMS: {vms_mb:7.2f} MB "
                    f"| CPU: {cpu_percent:5.1f}% "
                    f"| Threads: {num_threads:3d} "
                    f"{alert}"
                )
                print(status)

                # Save to CSV
                if csv_writer:
                    csv_writer.writerow(
                        [
                            datetime.now().isoformat(),
                            f"{rss_mb:.2f}",
                            f"{vms_mb:.2f}",
                            f"{mem_percent:.2f}",
                            num_threads,
                            num_fds,
                            f"{cpu_percent:.2f}",
                        ]
                    )
                    csv_file.flush()

                # Store sample for stats
                samples.append(
                    {"timestamp": datetime.now(), "rss_mb": rss_mb, "delta": delta}
                )

                # Keep only last 100 samples for stats
                if len(samples) > 100:
                    samples.pop(0)

                previous_rss = rss_mb
                time.sleep(interval)

            except psutil.NoSuchProcess:
                print("\n⚠️  Process terminated")
                break
            except psutil.AccessDenied:
                print("\n⚠️  Access denied to process")
                break

    except KeyboardInterrupt:
        print("\n\nStopping monitor...")

    finally:
        if csv_file:
            csv_file.close()

        # Print summary
        print(f"\n{'=' * 80}")
        print("Memory Monitoring Summary")
        print(f"{'=' * 80}")
        print(f"Baseline RSS:        {baseline_rss:.2f} MB")
        print(f"Final RSS:           {previous_rss:.2f} MB")
        print(f"Peak RSS:            {max_rss:.2f} MB")
        print(
            f"Total increase:      {previous_rss - baseline_rss:+.2f} MB ({(previous_rss - baseline_rss) / baseline_rss * 100:+.1f}%)"
        )

        if samples:
            avg_rss = sum(s["rss_mb"] for s in samples) / len(samples)
            max_delta = max(abs(s["delta"]) for s in samples)
            print(f"Average RSS:         {avg_rss:.2f} MB")
            print(f"Max single delta:    {max_delta:.2f} MB")

        print(f"{'=' * 80}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Monitor memory usage of the chat endpoint API process"
    )
    parser.add_argument("--pid", type=int, help="Process ID to monitor")
    parser.add_argument(
        "--auto-detect", action="store_true", help="Auto-detect Flask API process"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.5,
        help="Polling interval in seconds (default: 0.5)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=50.0,
        help="Memory spike alert threshold in MB (default: 50)",
    )
    parser.add_argument("--output", help="Output CSV file for data logging")

    args = parser.parse_args()

    # Determine PID
    pid = args.pid
    if args.auto_detect:
        print("Auto-detecting Flask process...")
        pid = find_api_server_process()
        if not pid:
            print("Error: Could not find Flask API process")
            print("Try specifying --pid manually")
            sys.exit(1)
        print(f"Found Flask process: {pid}\n")

    if not pid:
        parser.error("Either --pid or --auto-detect must be specified")

    # Start monitoring
    monitor_memory(
        pid=pid,
        interval=args.interval,
        threshold_mb=args.threshold,
        output_file=args.output,
    )


if __name__ == "__main__":
    main()
