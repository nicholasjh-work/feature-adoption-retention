"""Feature Adoption & Retention demo using DuckDB.

Runs the equivalent of the dbt models locally and generates
retention heatmap and feature adoption charts.

Usage:
    python demo.py --db ../infra-data-pipelines/demo.duckdb
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd


def build_feature_adoption(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Execute fct_feature_adoption logic and return DataFrame."""
    sql = """
    WITH events AS (
        SELECT member_id, event_date, feature,
               DATE_TRUNC('week', event_date::DATE) AS week_start
        FROM raw.feature_events
    ),
    first_event AS (
        SELECT member_id, feature, MIN(event_date) AS first_event_date
        FROM raw.feature_events
        GROUP BY member_id, feature
    ),
    metrics AS (
        SELECT
            e.week_start,
            e.feature,
            COUNT(DISTINCT e.member_id) AS unique_users,
            COUNT(DISTINCT CASE
                WHEN fe.first_event_date >= e.week_start
                 AND fe.first_event_date < e.week_start + INTERVAL '7 day'
                THEN e.member_id END) AS new_adopters,
            COUNT(DISTINCT CASE
                WHEN fe.first_event_date < e.week_start
                THEN e.member_id END) AS repeat_users
        FROM events e
        JOIN first_event fe ON fe.member_id = e.member_id AND fe.feature = e.feature
        GROUP BY 1, 2
    )
    SELECT * FROM metrics ORDER BY week_start, feature
    """
    return con.execute(sql).df()


def build_retention_cohorts(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Execute fct_retention_cohorts logic and return DataFrame."""
    sql = """
    WITH members AS (
        SELECT member_id, plan_type, acquisition_channel,
               DATE_TRUNC('week', signup_date::DATE) AS cohort_week,
               signup_date::DATE AS signup_date
        FROM raw.members
    ),
    activity AS (
        SELECT member_id, metric_date::DATE AS metric_date
        FROM raw.daily_metrics
    ),
    retention AS (
        SELECT
            mem.plan_type,
            mem.acquisition_channel,
            mem.cohort_week,
            COUNT(DISTINCT mem.member_id) AS cohort_size,
            COUNT(DISTINCT CASE WHEN act.metric_date BETWEEN mem.signup_date + INTERVAL '6 day'
                AND mem.signup_date + INTERVAL '8 day'
                THEN mem.member_id END) AS active_d7,
            COUNT(DISTINCT CASE WHEN act.metric_date BETWEEN mem.signup_date + INTERVAL '28 day'
                AND mem.signup_date + INTERVAL '32 day'
                THEN mem.member_id END) AS active_d30,
            COUNT(DISTINCT CASE WHEN act.metric_date BETWEEN mem.signup_date + INTERVAL '88 day'
                AND mem.signup_date + INTERVAL '92 day'
                THEN mem.member_id END) AS active_d90
        FROM members mem
        LEFT JOIN activity act ON act.member_id = mem.member_id
        GROUP BY 1, 2, 3
    )
    SELECT *,
        ROUND(active_d7 * 1.0 / NULLIF(cohort_size, 0), 3) AS retention_d7,
        ROUND(active_d30 * 1.0 / NULLIF(cohort_size, 0), 3) AS retention_d30,
        ROUND(active_d90 * 1.0 / NULLIF(cohort_size, 0), 3) AS retention_d90
    FROM retention
    ORDER BY cohort_week
    """
    return con.execute(sql).df()


def plot_feature_adoption(df: pd.DataFrame, out_dir: Path) -> None:
    """Plot weekly feature adoption for top 5 features."""
    top_features = (
        df.groupby("feature")["unique_users"].sum()
        .nlargest(5).index.tolist()
    )
    subset = df[df["feature"].isin(top_features)]

    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # Unique users over time
    for feat in top_features:
        fdata = subset[subset["feature"] == feat].sort_values("week_start")
        axes[0].plot(fdata["week_start"], fdata["unique_users"], label=feat, linewidth=1.5)
    axes[0].set_title("Weekly Unique Users by Feature")
    axes[0].set_xlabel("Week")
    axes[0].set_ylabel("Unique Users")
    axes[0].legend(fontsize=8)
    axes[0].tick_params(axis="x", rotation=45)
    axes[0].grid(True, alpha=0.3)

    # New vs repeat users (stacked, aggregated)
    weekly = df.groupby("week_start")[["new_adopters", "repeat_users"]].sum().sort_index()
    axes[1].bar(range(len(weekly)), weekly["new_adopters"], label="New Adopters", color="#3498db", alpha=0.8)
    axes[1].bar(range(len(weekly)), weekly["repeat_users"],
                bottom=weekly["new_adopters"], label="Repeat Users", color="#2ecc71", alpha=0.8)
    axes[1].set_title("New Adopters vs Repeat Users (All Features)")
    axes[1].set_xlabel("Week Number")
    axes[1].set_ylabel("Users")
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)

    plt.tight_layout()
    path = out_dir / "feature_adoption.png"
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved {path}")


def plot_retention_heatmap(df: pd.DataFrame, out_dir: Path) -> None:
    """Plot a retention heatmap by cohort week."""
    # Aggregate across plan/channel to get overall cohort retention
    cohort_agg = (
        df.groupby("cohort_week")
        .agg({"cohort_size": "sum", "active_d7": "sum",
              "active_d30": "sum", "active_d90": "sum"})
        .sort_index()
    )
    cohort_agg["ret_d7"] = cohort_agg["active_d7"] / cohort_agg["cohort_size"]
    cohort_agg["ret_d30"] = cohort_agg["active_d30"] / cohort_agg["cohort_size"]
    cohort_agg["ret_d90"] = cohort_agg["active_d90"] / cohort_agg["cohort_size"]

    # Take first 20 cohorts for readability
    cohort_agg = cohort_agg.head(20)

    heatmap_data = cohort_agg[["ret_d7", "ret_d30", "ret_d90"]].values
    labels = [str(d.date()) if hasattr(d, "date") else str(d)[:10]
              for d in cohort_agg.index]

    fig, ax = plt.subplots(figsize=(10, 8))
    im = ax.imshow(heatmap_data, cmap="YlGn", aspect="auto", vmin=0, vmax=1)

    ax.set_xticks([0, 1, 2])
    ax.set_xticklabels(["D7", "D30", "D90"])
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=8)
    ax.set_title("Cohort Retention Heatmap (by signup week)")
    ax.set_xlabel("Retention Window")
    ax.set_ylabel("Cohort Week")

    # Annotate cells
    for i in range(len(labels)):
        for j in range(3):
            val = heatmap_data[i, j]
            ax.text(j, i, f"{val:.0%}", ha="center", va="center",
                    color="white" if val > 0.6 else "black", fontsize=8)

    plt.colorbar(im, ax=ax, label="Retention Rate")
    plt.tight_layout()
    path = out_dir / "retention_heatmap.png"
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved {path}")


def plot_retention_by_plan(df: pd.DataFrame, out_dir: Path) -> None:
    """Plot retention curves by plan type."""
    plan_ret = (
        df.groupby("plan_type")
        .agg({"cohort_size": "sum", "active_d7": "sum",
              "active_d30": "sum", "active_d90": "sum"})
    )
    plan_ret["ret_d7"] = plan_ret["active_d7"] / plan_ret["cohort_size"]
    plan_ret["ret_d30"] = plan_ret["active_d30"] / plan_ret["cohort_size"]
    plan_ret["ret_d90"] = plan_ret["active_d90"] / plan_ret["cohort_size"]

    colors = {"free": "#e74c3c", "pro": "#3498db", "enterprise": "#2ecc71"}
    fig, ax = plt.subplots(figsize=(10, 6))
    days = [0, 7, 30, 90]
    for plan in ["free", "pro", "enterprise"]:
        if plan in plan_ret.index:
            vals = [1.0, plan_ret.loc[plan, "ret_d7"],
                    plan_ret.loc[plan, "ret_d30"], plan_ret.loc[plan, "ret_d90"]]
            ax.plot(days, vals, "o-", label=plan.title(), color=colors[plan], linewidth=2)

    ax.set_title("Retention Curves by Plan Type")
    ax.set_xlabel("Days Since Signup")
    ax.set_ylabel("Retention Rate")
    ax.set_ylim(0, 1.05)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(1.0))
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    path = out_dir / "retention_by_plan.png"
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Feature Adoption & Retention Demo")
    parser.add_argument("--db", type=str, default="../infra-data-pipelines/demo.duckdb",
                        help="Path to DuckDB database from infra repo")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Database not found at {db_path}")
        print("Run the infra-data-pipelines demo first: cd ../infra-data-pipelines && python demo.py")
        return

    con = duckdb.connect(str(db_path), read_only=True)
    out_dir = Path(__file__).parent / "screenshots"
    out_dir.mkdir(exist_ok=True)

    print("Building fct_feature_adoption...")
    adoption_df = build_feature_adoption(con)
    print(f"  {len(adoption_df):,} rows")
    print(f"\n  Top 5 features by total unique users:")
    top5 = adoption_df.groupby("feature")["unique_users"].sum().nlargest(5)
    for feat, count in top5.items():
        print(f"    {feat:30s}  {count:>6,}")

    print("\nBuilding fct_retention_cohorts...")
    retention_df = build_retention_cohorts(con)
    print(f"  {len(retention_df):,} rows")

    # Overall retention
    totals = retention_df.agg({
        "cohort_size": "sum", "active_d7": "sum",
        "active_d30": "sum", "active_d90": "sum"
    })
    print(f"\n  Overall retention:")
    print(f"    D7:  {totals['active_d7'] / totals['cohort_size']:.1%}")
    print(f"    D30: {totals['active_d30'] / totals['cohort_size']:.1%}")
    print(f"    D90: {totals['active_d90'] / totals['cohort_size']:.1%}")

    print("\nGenerating charts...")
    plot_feature_adoption(adoption_df, out_dir)
    plot_retention_heatmap(retention_df, out_dir)
    plot_retention_by_plan(retention_df, out_dir)

    con.close()
    print(f"\nDemo complete. Charts saved to {out_dir}/")


if __name__ == "__main__":
    main()
