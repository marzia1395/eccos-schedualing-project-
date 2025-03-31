import os
import subprocess
import time
import toml
import csv
import statistics
from pathlib import Path
import pandas as pd

CONFIG_DIR = "../build_scripts"
SERVER_IDS = [1, 2, 3]
CLIENT_IDS = [1]
LOG_DIR = "../benchmarks/logs/local-run"
RESULTS_FILE = "../benchmarks/logs/local-run/experiment_results.csv"

# Number of experiment runs per strategy-highlighted phase
runs_per_highlight = 3

# Strategy list
STRATEGIES = ["FCFS", "ThroughputPriority", "Fairness", "TargetedThroughput"]
TARGET_PARTITION_IDS = {0, 1, 2, 3, 4}

# Define highlight workloads for each strategy
HIGHLIGHT_WORKLOADS = {
    "ThroughputPriority": {
        "weights": [500, 200, 100, 50, 25, 10, 5, 5, 5, 5],
        "requests_per_sec": 140,
        "num_partitions": 30,
        "partition_size": 2
    },
    "Fairness": {
        "weights": [600, 300, 100, 20, 10, 5, 5, 5, 5, 5],
        "requests_per_sec": 80,
        "num_partitions": 50,
        "partition_size": 1
    },
    "TargetedThroughput": {
        "weights": [300, 150, 60, 20, 10, 5, 5, 5, 5, 5],
        "requests_per_sec": 100,
        "num_partitions": 40,
        "partition_size": 1
    }
}

REQUEST_DURATION = 60

def update_config_files(strategy: str, workload):
    num_partitions = workload["num_partitions"]
    partition_size = workload["partition_size"]
    weights = workload["weights"]
    rps = workload["requests_per_sec"]
    num_keys = num_partitions * partition_size
    num_weights = len(weights)

    if num_keys % num_weights != 0:
        raise ValueError(
            f"Key range ({num_keys}) must divide evenly by number of weights ({num_weights})"
        )

    for server_id in SERVER_IDS:
        path = os.path.join(CONFIG_DIR, f"server-{server_id}-config.toml")
        config = toml.load(path)
        config["in_scheduling_strategy"] = strategy
        config["out_scheduling_strategy"] = strategy
        config["num_partitions"] = num_partitions
        config["partition_size"] = partition_size
        with open(path, "w") as f:
            toml.dump(config, f)

    for client_id in CLIENT_IDS:
        path = os.path.join(CONFIG_DIR, f"client-{client_id}-config.toml")
        config = toml.load(path)
        config["request_config"]["requests_per_sec"] = rps
        config["request_config"]["skew"]["weights"] = weights
        config["request_config"]["duration_sec"] = REQUEST_DURATION
        config["request_config"]["key_range"] = [0, num_keys - 1]
        with open(path, "w") as f:
            toml.dump(config, f)

    print(f"Updated configs for strategy: {strategy}")

def update_client_seeds(seed: int, num_keys: int):
    for client_id in CLIENT_IDS:
        path = os.path.join(CONFIG_DIR, f"client-{client_id}-config.toml")
        config = toml.load(path)
        config["seed"] = seed
        config["request_config"]["key_range"] = [0, num_keys - 1]
        with open(path, "w") as f:
            toml.dump(config, f)
    print(f"Updated client seed to: {seed} and key_range to: [0, {num_keys - 1}]")

def run_server(server_id):
    env = os.environ.copy()
    env["CONFIG_FILE"] = os.path.join(CONFIG_DIR, f"server-{server_id}-config.toml")
    return subprocess.Popen(
        ["cargo", "run", "--release", "--manifest-path=../Cargo.toml", "--bin", "server"],
        env=env
    )

def run_client(client_id):
    env = os.environ.copy()
    env["CONFIG_FILE"] = os.path.join(CONFIG_DIR, f"client-{client_id}-config.toml")
    subprocess.run(
        ["cargo", "run", "--release", "--manifest-path=../Cargo.toml", "--bin", "client"],
        env=env
    )

def read_partition_stats(server_id):
    path = os.path.join(LOG_DIR, f"server-{server_id}.csv")
    entries = []
    target_entries = 0
    try:
        with open(path, "r") as f:
            for row in csv.reader(f):
                if row[0] == "total":
                    continue
                if len(row) == 3:
                    partition_range = row[0]
                    count = int(row[2])
                    partition_start = int(partition_range.split(",")[0])
                    if partition_start // 2 in TARGET_PARTITION_IDS:
                        target_entries += count
                    entries.append(count)
        stddev = statistics.stdev(entries) if len(entries) > 1 else 0
        return stddev, sum(entries), target_entries
    except Exception as e:
        print(f"Failed to read results from server {server_id}: {e}")
        return 0.0, 0, 0

def run_experiment(strategy: str, run: int, writer, all_results, seed: int, workload):
    print(f"\n{strategy} - Run {run}")
    update_config_files(strategy, workload)
    num_keys = workload["num_partitions"] * workload["partition_size"]
    update_client_seeds(seed, num_keys)

    server_procs = [run_server(sid) for sid in SERVER_IDS]
    time.sleep(2)

    for cid in CLIENT_IDS:
        run_client(cid)

    for proc in server_procs:
        proc.wait()

    for sid in SERVER_IDS:
        std, total, target_total = read_partition_stats(sid)
        target_share = (target_total / total * 100) if total > 0 else 0
        row = [
            strategy, f"Run {run}", f"Server {sid}",
            round(std, 2),
            total,
            round(target_share, 2)
        ]
        writer.writerow(row)
        all_results.append(row)

def write_summary(all_results):
    print("\nCalculating summary statistics...")
    df = pd.DataFrame(all_results, columns=[
        "Strategy", "Run", "Server",
        "Partition StdDev", "Total Decided Entries",
        "Target Partition Share (%)"
    ])

    df["Partition StdDev"] = pd.to_numeric(df["Partition StdDev"], errors="coerce")
    df["Total Decided Entries"] = pd.to_numeric(df["Total Decided Entries"], errors="coerce")
    df["Target Partition Share (%)"] = pd.to_numeric(df["Target Partition Share (%)"], errors="coerce")

    def phase_from_run(run_str):
        run_num = int(run_str.split(" ")[1])
        if run_num <= runs_per_highlight:
            return "TP-Weighted Workload"
        elif run_num <= 2 * runs_per_highlight:
            return "Fairness-Weighted Workload"
        elif run_num <= 3 * runs_per_highlight:
            return "Targeted-TP-Weighted Workload"
        else:
            return "Unknown"

    df["Workload Phase"] = df["Run"].apply(phase_from_run)

    summary = []
    for phase in df["Workload Phase"].unique():
        summary.append([])
        summary.append([f"Summary Averages – {phase}"])
        summary.append([
            "Strategy", "Avg Partition StdDev",
            "Avg Total Decided Entries", "Avg Target Partition Share (%)"
        ])
        for strategy in df["Strategy"].unique():
            strat_df = df[(df["Strategy"] == strategy) & (df["Workload Phase"] == phase)]
            avg_std = strat_df["Partition StdDev"].mean()
            avg_total = strat_df["Total Decided Entries"].mean()
            avg_target = strat_df["Target Partition Share (%)"].mean()
            summary.append([
                strategy,
                round(avg_std, 2),
                int(avg_total),
                round(avg_target, 2)
            ])

    with open(RESULTS_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([])
        for block in summary:
            writer.writerow(block)

    print("Summary written to results.")

def main():
    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
    all_results = []

    with open(RESULTS_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Strategy", "Run", "Server", "Partition StdDev",
            "Total Decided Entries", "Target Partition Share (%)"
        ])

        run_id = 1
        for highlight_strategy, workload in HIGHLIGHT_WORKLOADS.items():
            for _ in range(runs_per_highlight):
                seed = run_id
                for strategy in STRATEGIES:
                    run_experiment(strategy, run_id, writer, all_results, seed, workload)
                run_id += 1

    write_summary(all_results)
    print(f"\nResults written to: {RESULTS_FILE}")

if __name__ == "__main__":
    main()


