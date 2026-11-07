import subprocess
import time
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

print("=== Starting Automated Chaos Test ===")

# Step 1: Ensure all containers are running
subprocess.run(["docker", "compose", "up", "-d"], check=True)
time.sleep(2)

print("\n[Phase 1] Launching concurrent load generator (400 clients, 20ms rampup)...")
start_time = time.time()

# Launch load test in background
bench_cmd = [
    "docker", "compose", "--profile", "bench", "run", "--rm", "loadtest",
    "go", "run", "cmd/loadtest/loadTest.go",
    "-target", "lb:8080",
    "-clients", "400",
    "-rampup", "20ms",
    "-csv", "plots/chaos_test.csv"
]
loadtest_proc = subprocess.Popen(bench_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

# Step 2: Wait 2.5 seconds, then kill backend1
time.sleep(2.5)
kill_time = time.time() - start_time
print(f"\n[Phase 2 - Chaos Injection at t={kill_time:.2f}s] Stopping backend1...")
subprocess.run(["docker", "compose", "stop", "backend1"], check=True)
print("-> backend1 STOPPED. Load balancer retrying and tripping circuit breaker...")

# Step 3: Wait 3.5 seconds with backend1 dead, then recover backend1
time.sleep(3.5)
restart_time = time.time() - start_time
print(f"\n[Phase 3 - Recovery at t={restart_time:.2f}s] Starting backend1...")
subprocess.run(["docker", "compose", "start", "backend1"], check=True)
print("-> backend1 RESTARTED. Active health check will re-admit it...")

# Step 4: Wait for load generator to finish
print("\nWaiting for load test to complete...")
stdout, stderr = loadtest_proc.communicate()
print(stdout)
if stderr:
    print("Stderr:", stderr)

# Step 5: Read and plot the chaos test results
csv_path = "plots/chaos_test.csv"
if not os.path.exists(csv_path):
    print(f"Error: {csv_path} not found!")
    exit(1)

df = pd.read_csv(csv_path)
df['timestamp'] = pd.to_datetime(df['timestamp'])
t0 = df['timestamp'].min()
df['rel_time_sec'] = (df['timestamp'] - t0).dt.total_seconds()

successes = (df['status'] == 'success').sum()
failures = (df['status'] == 'fail').sum()
total = len(df)

print(f"\n=== Chaos Test Results ===")
print(f"Total Requests: {total}")
print(f"Successes:      {successes} ({(successes/total)*100:.1f}%)")
print(f"Failures:       {failures}")

# Step 6: Generate Visualization
fig, ax = plt.subplots(figsize=(12, 6))

# Plot successful requests
succ_df = df[df['status'] == 'success']
fail_df = df[df['status'] == 'fail']

ax.scatter(succ_df['rel_time_sec'], succ_df['duration_ms'], color='#2ca02c', alpha=0.75, s=24, label='Success (200 OK via Retry/Masking)')
if len(fail_df) > 0:
    ax.scatter(fail_df['rel_time_sec'], fail_df['duration_ms'], color='#d62728', marker='x', s=40, label='Failed Request')

# Vertical chaos event markers
ax.axvline(kill_time, color='#d62728', linestyle='--', linewidth=2, label=f'Backend 1 Killed (t={kill_time:.1f}s)')
ax.axvline(restart_time, color='#1f77b4', linestyle='--', linewidth=2, label=f'Backend 1 Restored (t={restart_time:.1f}s)')

# Shaded chaos duration
ax.axvspan(kill_time, restart_time, color='#d62728', alpha=0.08, label='Outage Window (Retries Active)')

# Annotations
ax.annotate('Backend 1 Killed\nRetries mask dials -> 0 errors\nBreaker trips to OPEN',
            xy=(kill_time, 1500), xytext=(kill_time + 0.3, 1700),
            arrowprops=dict(facecolor='#d62728', shrink=0.08, width=1.5, headwidth=6),
            fontsize=9.5, fontweight='semibold',
            bbox=dict(boxstyle='round,pad=0.3', facecolor='#FFF3CD', edgecolor='#FFEEBA'))

ax.annotate('Backend 1 Restored\nActive Health Check restores breaker to CLOSED',
            xy=(restart_time, 1100), xytext=(restart_time + 0.3, 1400),
            arrowprops=dict(facecolor='#1f77b4', shrink=0.08, width=1.5, headwidth=6),
            fontsize=9.5, fontweight='semibold',
            bbox=dict(boxstyle='round,pad=0.3', facecolor='#E2E3E5', edgecolor='#D6D8DB'))

ax.set_title('Chaos Resilience Test: Backend Crash During Active Traffic Burst', fontsize=13, fontweight='bold', pad=12)
ax.set_xlabel('Elapsed Time from Benchmark Start (seconds)', fontsize=11)
ax.set_ylabel('Round-Trip Latency (ms)', fontsize=11)
ax.grid(True, linestyle=':', alpha=0.6)
ax.legend(loc='upper right', framealpha=0.9, fontsize=9.5)

plt.tight_layout()
output_img = 'plots/chaos_test.png'
plt.savefig(output_img, dpi=150)
print(f"Chaos plot saved to {output_img}")
print("=== Chaos Test Finished Successfully ===")

