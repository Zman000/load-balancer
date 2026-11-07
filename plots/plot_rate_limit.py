import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load real benchmark data from rate_limit_test.csv
csv_path = 'd:/Programming/LoadBalancer/LB/plots/rate_limit_test.csv'
df = pd.read_csv(csv_path)

# 1 = Admitted (success), 0 = Throttled (fail)
is_admitted = (df['status'] == 'success').astype(int)
request_num = np.arange(1, len(df) + 1)

admitted_count = (df['status'] == 'success').sum()
throttled_count = (df['status'] == 'fail').sum()
total_count = len(df)

# Create a 2-panel figure: Timeline + Summary
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5), gridspec_kw={'width_ratios': [2.2, 1]})

# --- Panel 1: Admission Timeline ---
colors = ['#2ca02c' if s == 'success' else '#d62728' for s in df['status']]
ax1.scatter(request_num, is_admitted, c=colors, alpha=0.7, s=18, edgecolors='none')

# Highlight the burst phase (first 55 requests)
ax1.axvspan(0.5, 55.5, color='#2ca02c', alpha=0.12, label='Initial Burst Window (Cap=50)')
ax1.axvspan(55.5, 500.5, color='#d62728', alpha=0.08, label='Steady-State Throttling (Refill=100 req/s)')

ax1.set_yticks([0, 1])
ax1.set_yticklabels(['429 Throttled', '200 Admitted'], fontsize=11, fontweight='semibold')
ax1.set_xlabel('Incoming Request Sequence (Client ID)', fontsize=11)
ax1.set_ylabel('Admission Fate', fontsize=11)
ax1.set_title('Token-Bucket Rate Limiter: Request Admission Timeline (500 Clients)', fontsize=12, fontweight='bold', pad=10)
ax1.set_xlim(0, 505)
ax1.grid(axis='x', linestyle=':', alpha=0.6)
ax1.legend(loc='center right', framealpha=0.9, fontsize=9)

# Annotation for token exhaustion
ax1.annotate('Bucket Exhausted\n(Only token refills pass)',
             xy=(56, 0), xytext=(120, 0.45),
             arrowprops=dict(facecolor='black', shrink=0.08, width=1, headwidth=5),
             fontsize=9, fontstyle='italic',
             bbox=dict(boxstyle='round,pad=0.3', facecolor='#FFF3CD', edgecolor='#FFEEBA'))

# --- Panel 2: Outcome Breakdown ---
categories = ['Admitted\n(200 OK)', 'Throttled\n(429 Rejected)']
counts = [admitted_count, throttled_count]
bar_colors = ['#2ca02c', '#d62728']

bars = ax2.bar(categories, counts, color=bar_colors, width=0.55, edgecolor='black', linewidth=0.5)
ax2.set_ylabel('Number of Requests', fontsize=11)
ax2.set_title(f'Request Distribution\n(Total: {total_count})', fontsize=12, fontweight='bold', pad=10)
ax2.grid(axis='y', linestyle='--', alpha=0.5)
ax2.set_ylim(0, 460)

for bar in bars:
    height = bar.get_height()
    pct = (height / total_count) * 100
    ax2.annotate(f'{height}\n({pct:.1f}%)',
                 xy=(bar.get_x() + bar.get_width() / 2, height),
                 xytext=(0, 4),
                 textcoords="offset points",
                 ha='center', va='bottom', fontsize=10, fontweight='bold')

plt.tight_layout()
output_path = 'd:/Programming/LoadBalancer/LB/plots/rate_limiting.png'
plt.savefig(output_path, dpi=150)
print(f"Generated {output_path} successfully.")

