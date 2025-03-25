import psutil
import time
from datetime import datetime
import csv
import configparser
from prometheus_client import start_http_server, Gauge
from socket import gethostname
from graphiteudp import GraphiteUDPClient

# Load configuration
config = configparser.ConfigParser()
config.read('config.ini')

# Observability backends
BACKENDS = config['observability']['backends'].split(',')

# Prometheus setup
if 'prometheus' in BACKENDS:
    PROMETHEUS_PORT = int(config['prometheus']['port'])
    PROCESS_CPU_USAGE = Gauge('process_cpu_usage', 'Process CPU Usage (%)', ['role'])
    PROCESS_MEMORY_USAGE = Gauge('process_memory_usage', 'Process Memory Usage (MB)', ['role'])
    PROCESS_THREADS = Gauge('process_num_threads', 'Process Number of Threads', ['role'])
    PROCESS_DISK_READ_BYTES = Gauge('process_disk_read_bytes', 'Process Disk Read Bytes', ['role'])
    PROCESS_DISK_WRITE_BYTES = Gauge('process_disk_write_bytes', 'Process Disk Write Bytes', ['role'])
    PROCESS_NETWORK_SENT_BYTES = Gauge('process_network_sent_bytes', 'Process Network Sent Bytes', ['role'])
    PROCESS_NETWORK_RECV_BYTES = Gauge('process_network_recv_bytes', 'Process Network Received Bytes', ['role'])

# Graphite setup
if 'graphite' in BACKENDS:
    GRAPHITE_HOST = config['graphite']['host']
    GRAPHITE_PORT = int(config['graphite']['port'])
    graphite_client = GraphiteUDPClient(GRAPHITE_HOST, GRAPHITE_PORT)
    graphite_client.connect()

# File logging setup
if 'file' in BACKENDS:
    LOG_FILE = config['file']['log_file']

# Application configuration
APP_CONFIG = {
    "nodejs_service": {
        "master": {"cmdline": "index.js"},
        "worker": {"cmdline": "--worker"}
    },
    "java_service": {
        "main": {"name": "java"}
    }
}

def collect_application_metrics():
    """Collect application-level metrics."""
    metrics_by_role = defaultdict(lambda: {
        "cpu_usage": [],
        "memory_usage": 0,
        "num_threads": 0,
        "disk_read_bytes": 0,
        "disk_write_bytes": 0,
        "network_sent_bytes": 0,
        "network_recv_bytes": 0,
        "open_files": []
    })

    for process in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            pid = process.info['pid']
            name = process.info['name']
            cmdline = " ".join(process.info['cmdline'])

            # Determine the role of the process based on the configuration
            role = None
            for app_name, app_roles in APP_CONFIG.items():
                for role_name, role_config in app_roles.items():
                    if ("name" in role_config and role_config["name"] in name) or \
                       ("cmdline" in role_config and role_config["cmdline"] in cmdline):
                        role = f"{app_name}_{role_name}"
                        break
                if role:
                    break

            if not role:
                continue  # Skip processes that don't match any role

            # Collect process-specific metrics
            cpu_usage = process.cpu_percent(interval=0.1)
            memory_info = process.memory_info().rss / (1024 * 1024)  # Convert to MB
            num_threads = process.num_threads()

            # Disk I/O metrics
            io_counters = process.io_counters()
            disk_read_bytes = io_counters.read_bytes
            disk_write_bytes = io_counters.write_bytes

            # Network I/O metrics
            net_io_counters = psutil.net_io_counters()
            network_sent_bytes = net_io_counters.bytes_sent
            network_recv_bytes = net_io_counters.bytes_recv

            # File operations
            open_files = [f.path for f in process.open_files()]

            # Aggregate metrics by role
            metrics_by_role[role]["cpu_usage"].append(cpu_usage)
            metrics_by_role[role]["memory_usage"] += memory_info
            metrics_by_role[role]["num_threads"] += num_threads
            metrics_by_role[role]["disk_read_bytes"] += disk_read_bytes
            metrics_by_role[role]["disk_write_bytes"] += disk_write_bytes
            metrics_by_role[role]["network_sent_bytes"] += network_sent_bytes
            metrics_by_role[role]["network_recv_bytes"] += network_recv_bytes
            metrics_by_role[role]["open_files"].extend(open_files)

        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue

    return metrics_by_role

def publish_to_prometheus(metrics_by_role):
    """Publish metrics to Prometheus."""
    for role, metrics in metrics_by_role.items():
        avg_cpu_usage = sum(metrics["cpu_usage"]) / len(metrics["cpu_usage"]) if metrics["cpu_usage"] else 0
        total_memory_usage = metrics["memory_usage"]
        total_num_threads = metrics["num_threads"]
        total_disk_read_bytes = metrics["disk_read_bytes"]
        total_disk_write_bytes = metrics["disk_write_bytes"]
        total_network_sent_bytes = metrics["network_sent_bytes"]
        total_network_recv_bytes = metrics["network_recv_bytes"]

        PROCESS_CPU_USAGE.labels(role=role).set(avg_cpu_usage)
        PROCESS_MEMORY_USAGE.labels(role=role).set(total_memory_usage)
        PROCESS_THREADS.labels(role=role).set(total_num_threads)
        PROCESS_DISK_READ_BYTES.labels(role=role).set(total_disk_read_bytes)
        PROCESS_DISK_WRITE_BYTES.labels(role=role).set(total_disk_write_bytes)
        PROCESS_NETWORK_SENT_BYTES.labels(role=role).set(total_network_sent_bytes)
        PROCESS_NETWORK_RECV_BYTES.labels(role=role).set(total_network_recv_bytes)

def publish_to_graphite(metrics_by_role):
    """Publish metrics to Graphite."""
    hostname = gethostname()
    for role, metrics in metrics_by_role.items():
        avg_cpu_usage = sum(metrics["cpu_usage"]) / len(metrics["cpu_usage"]) if metrics["cpu_usage"] else 0
        total_memory_usage = metrics["memory_usage"]
        total_num_threads = metrics["num_threads"]
        total_disk_read_bytes = metrics["disk_read_bytes"]
        total_disk_write_bytes = metrics["disk_write_bytes"]
        total_network_sent_bytes = metrics["network_sent_bytes"]
        total_network_recv_bytes = metrics["network_recv_bytes"]

        graphite_client.send(f"{hostname}.{role}.cpu_usage", avg_cpu_usage)
        graphite_client.send(f"{hostname}.{role}.memory_usage", total_memory_usage)
        graphite_client.send(f"{hostname}.{role}.num_threads", total_num_threads)
        graphite_client.send(f"{hostname}.{role}.disk_read_bytes", total_disk_read_bytes)
        graphite_client.send(f"{hostname}.{role}.disk_write_bytes", total_disk_write_bytes)
        graphite_client.send(f"{hostname}.{role}.network_sent_bytes", total_network_sent_bytes)
        graphite_client.send(f"{hostname}.{role}.network_recv_bytes", total_network_recv_bytes)

def publish_to_file(metrics_by_role):
    """Publish metrics to a CSV file."""
    aggregated_metrics = []
    for role, metrics in metrics_by_role.items():
        avg_cpu_usage = sum(metrics["cpu_usage"]) / len(metrics["cpu_usage"]) if metrics["cpu_usage"] else 0
        total_memory_usage = metrics["memory_usage"]
        total_num_threads = metrics["num_threads"]
        total_disk_read_bytes = metrics["disk_read_bytes"]
        total_disk_write_bytes = metrics["disk_write_bytes"]
        total_network_sent_bytes = metrics["network_sent_bytes"]
        total_network_recv_bytes = metrics["network_recv_bytes"]
        open_files = ", ".join(metrics["open_files"])

        aggregated_metrics.append({
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'role': role,
            'avg_cpu_usage': avg_cpu_usage,
            'total_memory_usage': total_memory_usage,
            'total_num_threads': total_num_threads,
            'total_disk_read_bytes': total_disk_read_bytes,
            'total_disk_write_bytes': total_disk_write_bytes,
            'total_network_sent_bytes': total_network_sent_bytes,
            'total_network_recv_bytes': total_network_recv_bytes,
            'open_files': open_files
        })

    # Write metrics to the CSV file
    with open(LOG_FILE, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'timestamp', 'role', 'avg_cpu_usage', 'total_memory_usage', 'total_num_threads',
            'total_disk_read_bytes', 'total_disk_write_bytes', 'total_network_sent_bytes',
            'total_network_recv_bytes', 'open_files'
        ])
        if f.tell() == 0:  # Write the header only if the file is new
            writer.writeheader()
        writer.writerows(aggregated_metrics)

def main():
    # Start Prometheus HTTP server if selected
    if 'prometheus' in BACKENDS:
        start_http_server(PROMETHEUS_PORT)
        print(f"Metrics exposed on Prometheus port {PROMETHEUS_PORT}.")

    while True:
        # Collect metrics
        metrics_by_role = collect_application_metrics()

        # Publish metrics to selected backends
        if 'prometheus' in BACKENDS:
            publish_to_prometheus(metrics_by_role)
        if 'graphite' in BACKENDS:
            publish_to_graphite(metrics_by_role)
        if 'file' in BACKENDS:
            publish_to_file(metrics_by_role)

        # Sleep for the collection interval
        time.sleep(5)

if __name__ == "__main__":
    main()
