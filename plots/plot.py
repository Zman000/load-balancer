import matplotlib.pyplot as plt
import numpy as np

# Data from benchmark summaries (2000 clients)
backends = ['1 Backend', '4 Backends']
p50 = [402, 402]
p95 = [426, 415]
p99 = [470, 448]

x = np.arange(len(backends))
width = 0.25

fig, ax = plt.subplots(figsize=(8, 6))
rects1 = ax.bar(x - width, p50, width, label='p50 Latency (ms)', color='#4C72B0')
rects2 = ax.bar(x, p95, width, label='p95 Latency (ms)', color='#55A868')
rects3 = ax.bar(x + width, p99, width, label='p99 Latency (ms)', color='#C44E52')

ax.set_ylabel('Latency (ms)')
ax.set_title('Load Balancer Latency by Backend Count (2000 Concurrent Clients)')
ax.set_xticks(x)
ax.set_xticklabels(backends)
ax.legend()

# Add labels on top of bars
def autolabel(rects):
    for rect in rects:
        height = rect.get_height()
        ax.annotate(f'{height}',
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')

autolabel(rects1)
autolabel(rects2)
autolabel(rects3)

plt.tight_layout()
plt.savefig('latency_comparison.png')
print("Plot saved as latency_comparison.png")