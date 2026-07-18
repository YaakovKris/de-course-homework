import polars as pl
from pipeline import config

print('--- Проверка BRONZE ---')
try:
    bronze = pl.read_parquet(config.BRONZE_FILE)
    print('Файл:', config.BRONZE_FILE)
    print('Строк:', bronze.height)
    print('Уникальных типов событий:', bronze['event_type'].n_unique())
    print('Null в created_at:', int(bronze['created_at'].null_count()))
    print('Schema:', {k: v for k, v in bronze.schema.items()})
except Exception as e:
    print('Ошибка при проверке bronze:', e)

print('\n--- Проверка SILVER ---')
try:
    silver = pl.read_parquet(config.SILVER_FILE)
    print('Файл:', config.SILVER_FILE)
    print('Строк:', silver.height)
    print('Уникальных типов событий:', set(silver['event_type'].unique()))
    for col in ('event_id', 'created_at', 'repo_name'):
        print(f"Null в {col}:", int(silver[col].null_count()))
    print('event_id уникальны:', silver['event_id'].n_unique() == silver.height)
except Exception as e:
    print('Ошибка при проверке silver:', e)

print('\n--- Проверка GOLD ---')
try:
    repo = pl.read_parquet(config.GOLD_REPO_ACTIVITY)
    minutes = pl.read_parquet(config.GOLD_ACTIVITY_PER_MINUTE)
    push = pl.read_parquet(config.GOLD_PUSH_COMMITS)
    print('repo_activity: rows=', repo.height, 'sum(event_count)=', int(repo['event_count'].sum()))
    print('activity_per_minute: rows=', minutes.height, 'sum(event_count)=', int(minutes['event_count'].sum()))
    print('push_commits_by_repo: rows=', push.height, 'sum(total_commits)=', int(push['total_commits'].sum()))
except Exception as e:
    print('Ошибка при проверке gold:', e)
