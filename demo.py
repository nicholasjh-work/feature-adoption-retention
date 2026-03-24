"""Feature Adoption & Retention demo using PostgreSQL.

Usage:
    python demo.py
    python demo.py --db-url postgresql://user:pass@host/dbname
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

DEFAULT_DB_URL = "postgresql://demo_user:demo_pass@localhost:5432/analytics_demo"


def build_feature_adoption(engine):
    sql = """
    WITH events AS (
        SELECT member_id, event_date, feature,
               DATE_TRUNC('week', event_date) AS week_start
        FROM raw.feature_events
    ),
    first_event AS (
        SELECT member_id, feature, MIN(event_date) AS first_event_date
        FROM raw.feature_events GROUP BY member_id, feature
    ),
    metrics AS (
        SELECT e.week_start, e.feature,
            COUNT(DISTINCT e.member_id) AS unique_users,
            COUNT(DISTINCT CASE WHEN fe.first_event_date >= e.week_start
                AND fe.first_event_date < e.week_start + INTERVAL '7 day' THEN e.member_id END) AS new_adopters,
            COUNT(DISTINCT CASE WHEN fe.first_event_date < e.week_start THEN e.member_id END) AS repeat_users
        FROM events e JOIN first_event fe ON fe.member_id = e.member_id AND fe.feature = e.feature
        GROUP BY 1, 2
    )
    SELECT * FROM metrics ORDER BY week_start, feature
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


def build_retention_cohorts(engine):
    sql = """
    WITH members AS (
        SELECT member_id, plan_type, acquisition_channel,
               DATE_TRUNC('week', signup_date) AS cohort_week, signup_date
        FROM raw.members
    ),
    activity AS (
        SELECT member_id, metric_date FROM raw.daily_metrics
    ),
    retention AS (
        SELECT mem.plan_type, mem.acquisition_channel, mem.cohort_week,
            COUNT(DISTINCT mem.member_id) AS cohort_size,
            COUNT(DISTINCT CASE WHEN act.metric_date BETWEEN mem.signup_date + INTERVAL '6 day'
                AND mem.signup_date + INTERVAL '8 day' THEN mem.member_id END) AS active_d7,
            COUNT(DISTINCT CASE WHEN act.metric_date BETWEEN mem.signup_date + INTERVAL '28 day'
                AND mem.signup_date + INTERVAL '32 day' THEN mem.member_id END) AS active_d30,
            COUNT(DISTINCT CASE WHEN act.metric_date BETWEEN mem.signup_date + INTERVAL '88 day'
                AND mem.signup_date + INTERVAL '92 day' THEN mem.member_id END) AS active_d90
        FROM members mem LEFT JOIN activity act ON act.member_id = mem.member_id
        GROUP BY 1, 2, 3
    )
    SELECT *, ROUND(active_d7 * 1.0 / NULLIF(cohort_size, 0), 3) AS retention_d7,
        ROUND(active_d30 * 1.0 / NULLIF(cohort_size, 0), 3) AS retention_d30,
        ROUND(active_d90 * 1.0 / NULLIF(cohort_size, 0), 3) AS retention_d90
    FROM retention ORDER BY cohort_week
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


def plot_feature_adoption(df, out_dir):
    top = df.groupby("feature")["unique_users"].sum().nlargest(5).index.tolist()
    subset = df[df["feature"].isin(top)]
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    for feat in top:
        fd = subset[subset["feature"] == feat].sort_values("week_start")
        axes[0].plot(fd["week_start"], fd["unique_users"], label=feat, linewidth=1.5)
    axes[0].set_title("Weekly Unique Users by Feature")
    axes[0].set_xlabel("Week"); axes[0].set_ylabel("Unique Users")
    axes[0].legend(fontsize=8); axes[0].tick_params(axis="x", rotation=45); axes[0].grid(True, alpha=0.3)

    weekly = df.groupby("week_start")[["new_adopters", "repeat_users"]].sum().sort_index()
    axes[1].bar(range(len(weekly)), weekly["new_adopters"], label="New Adopters", color="#3498db", alpha=0.8)
    axes[1].bar(range(len(weekly)), weekly["repeat_users"], bottom=weekly["new_adopters"], label="Repeat Users", color="#2ecc71", alpha=0.8)
    axes[1].set_title("New Adopters vs Repeat Users"); axes[1].set_xlabel("Week"); axes[1].set_ylabel("Users")
    axes[1].legend(); axes[1].grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_dir / "feature_adoption.png", dpi=150, bbox_inches="tight"); plt.close()
    print(f"  Saved {out_dir}/feature_adoption.png")


def plot_retention_heatmap(df, out_dir):
    agg = df.groupby("cohort_week").agg({"cohort_size":"sum","active_d7":"sum","active_d30":"sum","active_d90":"sum"}).sort_index()
    agg["r7"] = agg["active_d7"]/agg["cohort_size"]
    agg["r30"] = agg["active_d30"]/agg["cohort_size"]
    agg["r90"] = agg["active_d90"]/agg["cohort_size"]
    agg = agg.head(20)
    data = agg[["r7","r30","r90"]].values
    labels = [str(d.date()) if hasattr(d,'date') else str(d)[:10] for d in agg.index]
    fig, ax = plt.subplots(figsize=(10, 8))
    im = ax.imshow(data, cmap="YlGn", aspect="auto", vmin=0, vmax=1)
    ax.set_xticks([0,1,2]); ax.set_xticklabels(["D7","D30","D90"])
    ax.set_yticks(range(len(labels))); ax.set_yticklabels(labels, fontsize=8)
    ax.set_title("Cohort Retention Heatmap"); ax.set_xlabel("Retention Window"); ax.set_ylabel("Cohort Week")
    for i in range(len(labels)):
        for j in range(3):
            ax.text(j, i, f"{data[i,j]:.0%}", ha="center", va="center", color="white" if data[i,j]>0.6 else "black", fontsize=8)
    plt.colorbar(im, ax=ax, label="Retention Rate"); plt.tight_layout()
    plt.savefig(out_dir / "retention_heatmap.png", dpi=150, bbox_inches="tight"); plt.close()
    print(f"  Saved {out_dir}/retention_heatmap.png")


def plot_retention_by_plan(df, out_dir):
    pr = df.groupby("plan_type").agg({"cohort_size":"sum","active_d7":"sum","active_d30":"sum","active_d90":"sum"})
    for d in ["d7","d30","d90"]: pr[f"r_{d}"] = pr[f"active_{d}"] / pr["cohort_size"]
    colors = {"free":"#e74c3c","pro":"#3498db","enterprise":"#2ecc71"}
    fig, ax = plt.subplots(figsize=(10, 6))
    for plan in ["free","pro","enterprise"]:
        if plan in pr.index:
            ax.plot([0,7,30,90], [1.0, float(pr.loc[plan,"r_d7"]), float(pr.loc[plan,"r_d30"]), float(pr.loc[plan,"r_d90"])],
                    "o-", label=plan.title(), color=colors[plan], linewidth=2)
    ax.set_title("Retention Curves by Plan Type"); ax.set_xlabel("Days Since Signup"); ax.set_ylabel("Retention Rate")
    ax.set_ylim(0, 1.05); ax.yaxis.set_major_formatter(mticker.PercentFormatter(1.0)); ax.legend(); ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_dir / "retention_by_plan.png", dpi=150, bbox_inches="tight"); plt.close()
    print(f"  Saved {out_dir}/retention_by_plan.png")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-url", default=os.getenv("DATABASE_URL", DEFAULT_DB_URL))
    args = parser.parse_args()
    engine = create_engine(args.db_url)
    out_dir = Path(__file__).parent / "screenshots"
    out_dir.mkdir(exist_ok=True)

    print("Building fct_feature_adoption...")
    adf = build_feature_adoption(engine)
    print(f"  {len(adf):,} rows")
    top5 = adf.groupby("feature")["unique_users"].sum().nlargest(5)
    print(f"\n  Top 5 features:")
    for feat, cnt in top5.items(): print(f"    {feat:30s}  {cnt:>6,}")

    print("\nBuilding fct_retention_cohorts...")
    rdf = build_retention_cohorts(engine)
    print(f"  {len(rdf):,} rows")
    t = rdf.agg({"cohort_size":"sum","active_d7":"sum","active_d30":"sum","active_d90":"sum"})
    print(f"\n  Overall retention:")
    print(f"    D7:  {t['active_d7']/t['cohort_size']:.1%}")
    print(f"    D30: {t['active_d30']/t['cohort_size']:.1%}")
    print(f"    D90: {t['active_d90']/t['cohort_size']:.1%}")

    print("\nGenerating charts...")
    plot_feature_adoption(adf, out_dir)
    plot_retention_heatmap(rdf, out_dir)
    plot_retention_by_plan(rdf, out_dir)
    engine.dispose()
    print(f"\nDemo complete. Charts saved to {out_dir}/")


if __name__ == "__main__":
    main()
