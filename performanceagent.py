import psutil
import time
import logging
import configparser
from prometheus_client import start_http_server, Gauge
from socket import gethostname
from graphiteudp import GraphiteUDPClient

# Load configuration
config = configparser.ConfigParser()
config.read('config.ini')

# Observability backend
OBSERVABILITY_BACKEND = config['observability']['backend']

# Prometheus setup
if OBSERVABILITY_BACKEND == 'prometheus':
    PROMETHEUS_PORT = int(config['prometheus']['port'])
    SYSTEM_CPU_USAGE = Gauge('system_cpu_usage', 'System CPU Usage (%)')
    SYSTEM_MEMORY_USAGE = Gauge('system_memory_usage', 'System Memory Usage (%)')
    DISK_IO_READ = Gauge('disk_io_read', 'Disk I/O Read (bytes)')
    DISK_IO_WRITE = Gauge('disk_io_write', 'Disk I/O Write (bytes)')
    NETWORK_BYTES_SENT = Gauge('network_bytes_sent', 'Network Bytes Sent')
    NETWORK_BYTES_RECV = Gauge('network_bytes_recv', 'Network Bytes Received')
    PROCESS_CPU_USAGE = Gauge('process_cpu_usage', 'Process CPU Usage (%)', ['pid', 'name'])
    PROCESS_MEMORY_USAGE = Gauge('process_memory_usage', 'Process Memory Usage (MB)', ['pid', 'name'])

# Graphite setup
if OBSERVABILITY_BACKEND == 'graphite':
    GRAPHITE_HOST = config['graphite']['host']
    GRAPHITE_PORT = int(config['graphite']['port'])
    graphite_client = GraphiteUDPClient(GRAPHITE_HOST, GRAPHITE_PORT)
    graphite_client.connect()

# File logging setup
if OBSERVABILITY_BACKEND == 'file':
    LOG_FILE = config['file']['log_file']
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format='%(asctime)s - %(message)s'
    )

def collect_system_metrics():
    """Collect system-level metrics."""
    cpu_usage = psutil.cpu_percent(interval=1)
    memory_usage = psutil.virtual_memory().percent
    disk_io = psutil.disk_io_counters()
    net_io = psutil.net_io_counters()

    if OBSERVABILITY_BACKEND == 'prometheus':
        SYSTEM_CPU_USAGE.set(cpu_usage)
        SYSTEM_MEMORY_USAGE.set(memory_usage)
        DISK_IO_READ.set(disk_io.read_bytes)
        DISK_IO_WRITE.set(disk_io.write_bytes)
        NETWORK_BYTES_SENT.set(net_io.bytes_sent)
        NETWORK_BYTES_RECV.set(net_io.bytes_recv)

    elif OBSERVABILITY_BACKEND == 'graphite':
        hostname = gethostname()
        graphite_client.send(f"{hostname}.cpu_usage", cpu_usage)
        graphite_client.send(f"{hostname}.memory_usage", memory_usage)
        graphite_client.send(f"{hostname}.disk_io_read", disk_io.read_bytes)
        graphite_client.send(f"{hostname}.disk_io_write", disk_io.write_bytes)
        graphite_client.send(f"{hostname}.network_bytes_sent", net_io.bytes_sent)
        graphite_client.send(f"{hostname}.network_bytes_recv", net_io.bytes_recv)

    elif OBSERVABILITY_BACKEND == 'file':
        logging.info(f"CPU Usage: {cpu_usage}%")
        logging.info(f"Memory Usage: {memory_usage}%")
        logging.info(f"Disk I/O - Read: {disk_io.read_bytes} bytes, Write: {disk_io.write_bytes} bytes")
        logging.info(f"Network I/O - Sent: {net_io.bytes_sent} bytes, Received: {net_io.bytes_recv} bytes")

def collect_application_metrics():
    """Collect application-level metrics."""
    for process in psutil.process_iter(['pid', 'name']):
        try:
            pid = process.info['pid']
            name = process.info['name']
            cpu_usage = process.cpu_percent(interval=0.1)
            memory_info = process.memory_info().rss / (1024 * 1024)  # Convert to MB

            if OBSERVABILITY_BACKEND == 'prometheus':
                PROCESS_CPU_USAGE.labels(pid=pid, name=name).set(cpu_usage)
                PROCESS_MEMORY_USAGE.labels(pid=pid, name=name).set(memory_info)

            elif OBSERVABILITY_BACKEND == 'graphite':
                hostname = gethostname()
                graphite_client.send(f"{hostname}.process.{name}.{pid}.cpu_usage", cpu_usage)
                graphite_client.send(f"{hostname}.process.{name}.{pid}.memory_usage", memory_info)

            elif OBSERVABILITY_BACKEND == 'file':
                logging.info(f"Process {name} (PID: {pid}) - CPU: {cpu_usage}%, Memory: {memory_info:.2f} MB")

        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue

def main():
    # Start Prometheus HTTP server if selected
    if OBSERVABILITY_BACKEND == 'prometheus':
        start_http_server(PROMETHEUS_PORT)
        print(f"Performance agent started. Metrics exposed on port {PROMETHEUS_PORT}.")

    while True:
        collect_system_metrics()
        collect_application_metrics()
        time.sleep(5)  # Collect metrics every 5 seconds

if __name__ == "__main__":
    main()