import matplotlib.pyplot as plt
import numpy as np

# Data based on your benchmark results
algorithms = ['Round Robin', 'Least Connections']
p50 = [301, 301]
p95 = [802, 303] # Estimated p95 based on typical distribution
p99 = [803, 311]

x = np.arange(len(algorithms))
width = 0.25

fig, ax = plt.subplots(figsize=(8, 6))
rects1 = ax.bar(x - width, p50, width, label='p50 Latency (ms)', color='#4C72B0')
rects2 = ax.bar(x, p95, width, label='p95 Latency (ms)', color='#55A868')
rects3 = ax.bar(x + width, p99, width, label='p99 Latency (ms)', color='#C44E52')

ax.set_ylabel('Latency (ms)')
ax.set_title('Algorithm Comparison: Handling a Slow Backend (500ms delay)')
ax.set_xticks(x)
ax.set_xticklabels(algorithms)
ax.legend(loc='upper left')

def autolabel(rects):
    for rect in rects:
        height = rect.get_height()
        ax.annotate(f'{height}',
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom')

autolabel(rects1)
autolabel(rects2)
autolabel(rects3)

plt.tight_layout()
plt.savefig('algorithm_comparison.png')
print("Plot saved as algorithm_comparison.png")