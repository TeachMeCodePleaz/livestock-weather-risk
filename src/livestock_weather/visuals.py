"""Plot only the final small aggregate (safe to collect on the driver)."""
from pathlib import Path
import matplotlib.pyplot as plt


def plot_ranking(scores, output_path, top_n=15):
    rows = (scores.select("state", "overall_score").orderBy("overall_score")
            .limit(top_n).collect())
    if not rows:
        raise ValueError("No states eligible for risk scoring; check coverage settings")
    labels = [r["state"] for r in rows][::-1]
    values = [r["overall_score"] for r in rows][::-1]
    fig, ax = plt.subplots(figsize=(9, max(3.5, len(rows) * .35)))
    ax.barh(labels, values)
    ax.set_xlim(0, 100)
    ax.set_xlabel("Relative historical weather-exposure score (lower = less)")
    ax.set_title("Lowest-scoring states in the analysis cohort")
    fig.tight_layout()
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path
