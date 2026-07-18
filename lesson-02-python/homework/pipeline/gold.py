"""Gold stage — three analytics tables built from silver.

TODO (Завдання 4, 5, 6): реалізуйте три функції нижче.
Контракт: див. CONTRACTS.md → "gold repo_activity", "gold activity_per_minute",
"gold push_commits_by_repo". Усі лічильники приводьте до Int64 (.cast(pl.Int64)),
щоб схема результату була стабільною.

  * build_repo_activity:        кількість подій + кількість унікальних типів на repo
  * build_activity_per_minute:  кількість подій по хвилинах (.dt.truncate("1m"))
  * build_push_commits_by_repo: тільки PushEvent — кількість пушів і сума commit_count на repo
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from . import config


def build_repo_activity(silver: pl.DataFrame) -> pl.DataFrame:
    repo_activity = (
        silver.group_by("repo_name")
        .agg(
            pl.count().alias("event_count"),
            pl.col("event_type").n_unique().alias("distinct_event_types"),
        )
        .with_columns(
            pl.col("event_count").cast(pl.Int64),
            pl.col("distinct_event_types").cast(pl.Int64),
        )
        .sort("event_count", descending=True)
    )

    Path(config.GOLD_REPO_ACTIVITY).parent.mkdir(parents=True, exist_ok=True)
    repo_activity.write_parquet(config.GOLD_REPO_ACTIVITY)
    return repo_activity


def build_activity_per_minute(silver: pl.DataFrame) -> pl.DataFrame:
    minutes = (
        silver.with_columns(
            minute=pl.col("created_at").dt.truncate("1m")
        )
        .group_by("minute")
        .agg(pl.count().alias("event_count"))
        .with_columns(pl.col("event_count").cast(pl.Int64))
        .sort("minute")
    )

    Path(config.GOLD_ACTIVITY_PER_MINUTE).parent.mkdir(parents=True, exist_ok=True)
    minutes.write_parquet(config.GOLD_ACTIVITY_PER_MINUTE)
    return minutes


def build_push_commits_by_repo(silver: pl.DataFrame) -> pl.DataFrame:
    push = silver.filter(pl.col("event_type") == "PushEvent")

    push_commits = (
        push.group_by("repo_name")
        .agg(
            pl.count().alias("push_events"),
            pl.col("commit_count").sum().alias("total_commits"),
        )
        .with_columns(
            pl.col("push_events").cast(pl.Int64),
            pl.col("total_commits").cast(pl.Int64),
        )
    )

    Path(config.GOLD_PUSH_COMMITS).parent.mkdir(parents=True, exist_ok=True)
    push_commits.write_parquet(config.GOLD_PUSH_COMMITS)
    return push_commits
