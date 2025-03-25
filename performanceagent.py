import psutil
import time
from datetime import datetime
import csv

# Define the CSV file name
CSV_FILE = "performance_metrics.csv"

def collect_system_metrics():
    """Collect system-level metrics."""
    cpu_usage = psutil.cpu_percent(interval=1)
    memory_usage = psutil.virtual_memory().percent
    disk_io = psutil.disk_io_counters()
    net_io = psutil.net_io_counters()

    return {
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'cpu_usage': cpu_usage,
        'memory_usage': memory_usage,
        'disk_io_read': disk_io.read_bytes,
        'disk_io_write': disk_io.write_bytes,
        'network_bytes_sent': net_io.bytes_sent,
        'network_bytes_recv': net_io.bytes_recv,
    }

def write_to_csv(metrics, csv_file):
    """Write metrics to a CSV file."""
    # Check if the file exists to determine if we need to write the header
    file_exists = False
    try:
        with open(csv_file, 'r'):
            file_exists = True
    except FileNotFoundError:
        pass

    # Write metrics to the CSV file
    with open(csv_file, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=metrics.keys())
        if not file_exists:
            writer.writeheader()  # Write the header only if the file is new
        writer.writerow(metrics)

def main():
    print(f"Logging metrics to: {CSV_FILE}")

    while True:
        # Collect metrics
        metrics = collect_system_metrics()

        # Write metrics to the CSV file
        write_to_csv(metrics, CSV_FILE)

        # Sleep for the collection interval
        time.sleep(5)

if __name__ == "__main__":
    main()
