# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Generate publication-quality figures for the Tragedy of the Commons experiment.

Produces four figures:
  1. Pool depletion curves (all scenarios overlaid)
  2. Per-agent cumulative harvest over time (mixed scenario)
  3. Cross-scenario bar chart (total harvest, final pool, avg energy)
  4. Free-rider inequity chart (per-strategy harvest in mixed vs cooperative)

Usage:
    uv run python experiments/generate_figures.py
"""

import asyncio
import os

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

# Run the simulation
from tragedy_of_the_commons import run_experiment

# ---------------------------------------------------------------------------
# Style
# ---------------------------------------------------------------------------

COLORS = {
    "cooperative": "#2ecc71",  # green
    "moderate": "#f39c12",  # amber
    "greedy": "#e74c3c",  # red
}

SCENARIO_COLORS = {
    "all_cooperative": "#2ecc71",
    "mixed": "#f39c12",
    "all_greedy": "#e74c3c",
}

SCENARIO_LABELS = {
    "all_cooperative": "All Cooperative",
    "mixed": "Mixed Society",
    "all_greedy": "All Greedy",
}


def setup_style():
    plt.rcParams.update(
        {
            "figure.facecolor": "#fafafa",
            "axes.facecolor": "#fafafa",
            "axes.grid": True,
            "grid.alpha": 0.3,
            "grid.linestyle": "--",
            "font.family": "sans-serif",
            "font.size": 11,
            "axes.titlesize": 14,
            "axes.titleweight": "bold",
            "axes.labelsize": 12,
            "legend.fontsize": 10,
            "figure.dpi": 150,
        }
    )


# ---------------------------------------------------------------------------
# Figure 1: Pool Depletion Curves
# ---------------------------------------------------------------------------


def fig_pool_depletion(results: list[dict], out_dir: str):
    fig, ax = plt.subplots(figsize=(10, 5))

    for res in results:
        scenario = res["scenario"]
        history = res["pool_history"]
        ticks = list(range(1, len(history) + 1))
        ax.plot(
            ticks,
            history,
            color=SCENARIO_COLORS[scenario],
            linewidth=2.5,
            label=SCENARIO_LABELS[scenario],
        )

    # Annotate equilibrium zones
    ax.axhline(y=981, color=COLORS["cooperative"], linestyle=":", alpha=0.4)
    ax.axhline(y=624, color=COLORS["moderate"], linestyle=":", alpha=0.4)
    ax.annotate(
        "Cooperative equilibrium (~981)",
        xy=(40, 981),
        fontsize=8,
        color=COLORS["cooperative"],
        alpha=0.7,
    )
    ax.annotate(
        "Mixed equilibrium (~624)",
        xy=(40, 640),
        fontsize=8,
        color=COLORS["moderate"],
        alpha=0.7,
    )

    ax.set_xlabel("Tick")
    ax.set_ylabel("Resource Pool")
    ax.set_title("Resource Pool Over Time")
    ax.set_xlim(0, 51)
    ax.set_ylim(-20, 1050)
    ax.legend(loc="center right")
    ax.fill_between(
        range(0, 52), 0, 100, color="#e74c3c", alpha=0.05, label="_collapse zone"
    )

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "fig1_pool_depletion.png"))
    plt.close(fig)
    print("  Saved fig1_pool_depletion.png")


# ---------------------------------------------------------------------------
# Figure 2: Per-Agent Harvest Accumulation (Mixed Scenario)
# ---------------------------------------------------------------------------


def fig_harvest_accumulation(results: list[dict], out_dir: str):
    mixed = next(r for r in results if r["scenario"] == "mixed")
    metrics = mixed["metrics"].tick_harvests

    # Build cumulative harvest per agent over time
    agents = {}
    for entry in metrics:
        name = entry["name"]
        if name not in agents:
            agents[name] = {"ticks": [], "cumulative": [], "strategy": entry["strategy"], "cum": 0.0}
        agents[name]["cum"] += entry["harvested"]
        agents[name]["ticks"].append(entry["tick"])
        agents[name]["cumulative"].append(agents[name]["cum"])

    fig, ax = plt.subplots(figsize=(10, 5))

    # Plot one representative per strategy (they're identical within strategy)
    plotted_strategies = set()
    for name, data in sorted(agents.items()):
        strategy = data["strategy"]
        color = COLORS[strategy]
        if strategy not in plotted_strategies:
            ax.plot(
                data["ticks"],
                data["cumulative"],
                color=color,
                linewidth=2.5,
                label=f"{strategy.title()} ({name})",
            )
            plotted_strategies.add(strategy)
        else:
            ax.plot(
                data["ticks"],
                data["cumulative"],
                color=color,
                linewidth=1.2,
                alpha=0.4,
            )

    ax.set_xlabel("Tick")
    ax.set_ylabel("Cumulative Harvest")
    ax.set_title("Cumulative Harvest per Agent (Mixed Society)")
    ax.legend(loc="upper left")

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "fig2_harvest_accumulation.png"))
    plt.close(fig)
    print("  Saved fig2_harvest_accumulation.png")


# ---------------------------------------------------------------------------
# Figure 3: Cross-Scenario Comparison (Grouped Bar Chart)
# ---------------------------------------------------------------------------


def fig_cross_scenario(results: list[dict], out_dir: str):
    fig, axes = plt.subplots(1, 3, figsize=(14, 5))

    scenarios = [r["scenario"] for r in results]
    labels = [SCENARIO_LABELS[s] for s in scenarios]
    colors = [SCENARIO_COLORS[s] for s in scenarios]

    # Panel 1: Total harvest
    totals = [sum(a["total_harvested"] for a in r["agents"]) for r in results]
    axes[0].bar(labels, totals, color=colors, edgecolor="white", linewidth=1.5)
    axes[0].set_title("Total Harvest (all agents)")
    axes[0].set_ylabel("Total Harvest")
    for i, v in enumerate(totals):
        axes[0].text(i, v + 200, f"{v:,.0f}", ha="center", fontsize=9, fontweight="bold")
    axes[0].set_ylim(0, max(totals) * 1.15)

    # Panel 2: Final pool
    pools = [r["final_pool"] for r in results]
    axes[1].bar(labels, pools, color=colors, edgecolor="white", linewidth=1.5)
    axes[1].set_title("Final Resource Pool")
    axes[1].set_ylabel("Pool Amount")
    axes[1].axhline(y=1000, color="gray", linestyle="--", alpha=0.3, label="Max capacity")
    for i, v in enumerate(pools):
        axes[1].text(i, v + 30, f"{v:,.0f}", ha="center", fontsize=9, fontweight="bold")
    axes[1].set_ylim(0, 1100)

    # Panel 3: Avg energy per agent
    energies = [
        sum(a["energy"] for a in r["agents"]) / len(r["agents"]) if r["agents"] else 0
        for r in results
    ]
    axes[2].bar(labels, energies, color=colors, edgecolor="white", linewidth=1.5)
    axes[2].set_title("Avg Agent Energy (tick 50)")
    axes[2].set_ylabel("Energy")
    for i, v in enumerate(energies):
        axes[2].text(i, v + 30, f"{v:,.0f}", ha="center", fontsize=9, fontweight="bold")
    axes[2].set_ylim(0, max(energies) * 1.2)

    for ax in axes:
        ax.tick_params(axis="x", rotation=15)

    fig.suptitle("Cross-Scenario Comparison", fontsize=15, fontweight="bold", y=1.02)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "fig3_cross_scenario.png"), bbox_inches="tight")
    plt.close(fig)
    print("  Saved fig3_cross_scenario.png")


# ---------------------------------------------------------------------------
# Figure 4: Free-Rider Inequity
# ---------------------------------------------------------------------------


def fig_free_rider(results: list[dict], out_dir: str):
    mixed = next(r for r in results if r["scenario"] == "mixed")
    coop = next(r for r in results if r["scenario"] == "all_cooperative")

    fig, ax = plt.subplots(figsize=(8, 5))

    # Per-strategy harvest in mixed scenario
    strategies = ["greedy", "moderate", "cooperative"]
    mixed_harvests = []
    for s in strategies:
        agents = [a for a in mixed["agents"] if a["strategy"] == s]
        mixed_harvests.append(agents[0]["total_harvested"] if agents else 0)

    # Cooperative-world baseline (same for all agents)
    coop_baseline = coop["agents"][0]["total_harvested"]

    x = np.arange(len(strategies))
    width = 0.35

    bars1 = ax.bar(
        x - width / 2,
        mixed_harvests,
        width,
        color=[COLORS[s] for s in strategies],
        edgecolor="white",
        linewidth=1.5,
        label="Mixed Society",
    )
    bars2 = ax.bar(
        x + width / 2,
        [coop_baseline] * 3,
        width,
        color=[COLORS[s] for s in strategies],
        edgecolor="white",
        linewidth=1.5,
        alpha=0.35,
        label="All-Cooperative World",
        hatch="//",
    )

    # Annotate the bars
    for bar, val in zip(bars1, mixed_harvests):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            val + 60,
            f"{val:,.0f}",
            ha="center",
            fontsize=9,
            fontweight="bold",
        )
    for bar in bars2:
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            coop_baseline + 60,
            f"{coop_baseline:,.0f}",
            ha="center",
            fontsize=9,
            alpha=0.6,
        )

    # Arrow showing free-rider advantage
    greedy_x = x[0] - width / 2
    ax.annotate(
        f"6.0x advantage",
        xy=(greedy_x, mixed_harvests[0]),
        xytext=(greedy_x + 0.8, mixed_harvests[0] + 300),
        fontsize=9,
        fontweight="bold",
        color=COLORS["greedy"],
        arrowprops=dict(arrowstyle="->", color=COLORS["greedy"], lw=1.5),
    )

    ax.set_xticks(x)
    ax.set_xticklabels([s.title() for s in strategies], fontsize=11)
    ax.set_ylabel("Per-Agent Harvest (50 ticks)")
    ax.set_title("Free-Rider Inequity: Mixed vs All-Cooperative")
    ax.legend(loc="upper right")
    ax.set_ylim(0, max(mixed_harvests) * 1.2)

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "fig4_free_rider.png"))
    plt.close(fig)
    print("  Saved fig4_free_rider.png")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main():
    setup_style()

    print("Running simulation...")
    results = await run_experiment(num_ticks=50)

    out_dir = os.path.join(os.path.dirname(__file__), "figures")
    os.makedirs(out_dir, exist_ok=True)

    print("Generating figures...")
    fig_pool_depletion(results, out_dir)
    fig_harvest_accumulation(results, out_dir)
    fig_cross_scenario(results, out_dir)
    fig_free_rider(results, out_dir)
    print(f"\nAll figures saved to {out_dir}/")


if __name__ == "__main__":
    asyncio.run(main())
