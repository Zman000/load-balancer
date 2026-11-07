import matplotlib.pyplot as plt
import numpy as np

# Real data from results_combined.txt and results_{1,2,4}b_2000c.csv
# Benchmarking 2,000 concurrent clients across 1, 2, and 4 backends
backends = ['1 Backend', '2 Backends', '4 Backends']
p50 = [301, 302, 301]
p95 = [314, 314, 313]
p99 = [1109, 336, 365]

x = np.arange(len(backends))
width = 0.25

fig, ax = plt.subplots(figsize=(9, 6))
rects1 = ax.bar(x - width, p50, width, label='p50 Latency (ms)', color='#4C72B0')
rects2 = ax.bar(x, p95, width, label='p95 Latency (ms)', color='#55A868')
rects3 = ax.bar(x + width, p99, width, label='p99 Latency (ms)', color='#C44E52')

ax.set_ylabel('Latency (ms)', fontsize=11)
ax.set_title('Load Balancer Latency by Backend Count (2,000 Concurrent Clients)', fontsize=13, fontweight='bold', pad=12)
ax.set_xticks(x)
ax.set_xticklabels(backends, fontsize=11)
ax.legend(loc='upper right', fontsize=10)
ax.grid(axis='y', linestyle='--', alpha=0.5)

# Annotate values on top of bars
def autolabel(rects):
    for rect in rects:
        height = rect.get_height()
        ax.annotate(f'{height}',
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 4),  # 4 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=9, fontweight='semibold')

autolabel(rects1)
autolabel(rects2)
autolabel(rects3)

# Highlight the 1-backend p99 queueing spike
ax.annotate('TCP backlog queueing spike\n(1,109 ms at 1 backend)',
            xy=(0 + width, 1109), xytext=(0.5, 950),
            arrowprops=dict(facecolor='black', shrink=0.08, width=1, headwidth=6),
            fontsize=9, fontstyle='italic', bbox=dict(boxstyle='round,pad=0.3', facecolor='#FFF3CD', edgecolor='#FFEEBA'))

plt.tight_layout()
plt.savefig('d:/Programming/LoadBalancer/LB/plots/latency_comparison.png', dpi=150)
print("Updated latency_comparison.png successfully.")
