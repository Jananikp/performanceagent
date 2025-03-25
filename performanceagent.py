import psutil
import time
from datetime import datetime
import csv
from collections import defaultdict

# Define the CSV file name
CSV_FILE = "application_metrics.csv"

# Configuration for identifying processes
APP_CONFIG = {
    "cpp_service": {
        "master": {"name": "cpp_master"},
        "worker": {"name": "cpp_worker"}
    },
    "nodejs_service": {
        "master": {"cmdline": "index.js"},  # Master process runs index.js
        "worker": {"cmdline": "--worker"}  # Worker processes have --worker in cmdline
    },
    "java_service": {
        "main": {"name": "java"}
    },
    "go_service": {
        "main": {"name": "myapp"},  # Replace "myapp" with the actual binary name
        "worker": {"cmdline": "worker"}  # Identify workers by command-line argument
    },
    "sap_service": {
        "dispatcher": {"name": "disp+work"},
        "hana_server": {"name": "hdbnameserver"},
        "netweaver": {"name": "sapstartsrv"}
    }
}

def collect_application_metrics():
    """Collect application-level metrics."""
    metrics_by_role = defaultdict(lambda: {"cpu_usage": [], "memory_usage": 0, "num_threads": 0})

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

            # Aggregate metrics by role
            metrics_by_role[role]["cpu_usage"].append(cpu_usage)
            metrics_by_role[role]["memory_usage"] += memory_info
            metrics_by_role[role]["num_threads"] += num_threads

        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue

    # Compute averages and totals
    aggregated_metrics = []
    for role, metrics in metrics_by_role.items():
        avg_cpu_usage = sum(metrics["cpu_usage"]) / len(metrics["cpu_usage"]) if metrics["cpu_usage"] else 0
        total_memory_usage = metrics["memory_usage"]
        total_num_threads = metrics["num_threads"]

        aggregated_metrics.append({
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'role': role,
            'avg_cpu_usage': avg_cpu_usage,
            'total_memory_usage': total_memory_usage,
            'total_num_threads': total_num_threads
        })

    return aggregated_metrics

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
        writer = csv.DictWriter(f, fieldnames=[
            'timestamp', 'role', 'avg_cpu_usage', 'total_memory_usage', 'total_num_threads'
        ])
        if not file_exists:
            writer.writeheader()  # Write the header only if the file is new
        writer.writerows(metrics)

def main():
    print(f"Logging metrics to: {CSV_FILE}")

    while True:
        # Collect metrics
        metrics = collect_application_metrics()

        # Write metrics to the CSV file
        write_to_csv(metrics, CSV_FILE)

        # Sleep for the collection interval
        time.sleep(5)

if __name__ == "__main__":
    main()
