import polars as pl
from pipeline import config

repo = pl.read_parquet(config.GOLD_REPO_ACTIVITY)
minutes = pl.read_parquet(config.GOLD_ACTIVITY_PER_MINUTE)
push = pl.read_parquet(config.GOLD_PUSH_COMMITS)

print('repo rows', repo.height)
print('repo sum', int(repo['event_count'].sum()))
print('repo sorted desc', repo['event_count'].to_list()[:5])

print('minutes rows', minutes.height)
print('minutes sum', int(minutes['event_count'].sum()))

print('push rows', push.height)
print('push total_commits', int(push['total_commits'].sum()))
