"""Bronze: landing-зона -> bronze.raw_events у Postgres. ЕТАП 1 — реалізуйте три місця з TODO.

Spark у local mode читає NDJSON із landing і дописує рядки у сховище через JDBC.

Контракт шару Bronze (повністю — SPEC.md, розділ 3):
  * читаємо з ЯВНОЮ схемою (ніякого inferSchema: схема — це контракт, а не здогадка);
  * `payload` лишається СИРИМ JSON-рядком: Bronze нічого не парсить і нічого не виправляє;
  * ідемпотентність за файлом: файл, який уже завантажено, вдруге не потрапляє;
  * атомарний append: або всі нові рядки в таблиці, або жодного.

    uv run python bronze_job.py
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from common import config
from common.spark import build_spark

# `payload` — вкладений обʼєкт, але оголошений як StringType: Spark віддасть його текст як є,
# без розбору (це і є контракт Bronze — payload лишається сирим JSON-рядком).
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=True),
        StructField("ride_id", StringType(), nullable=True),
        StructField("occurred_at", StringType(), nullable=True),
        StructField("source", StringType(), nullable=True),
        StructField("payload", StringType(), nullable=True),
    ]
)


def read_landing(spark: SparkSession, landing_dir: str) -> DataFrame:
    """Сирі події з landing плюс метадані ingestion.

    Читає `*.ndjson` із `landing_dir` (два рівні `dt=…/hour=…/`) з `EVENT_SCHEMA`. Hadoop-глоб
    `*.ndjson` сам відсіює `*.ndjson.tmp` (in-flight файли consumer-а) — суфікс не збігається.
    `dt=` і `hour=` у шляху — час запису файлу, а не бізнес-колонки: колонками не стають, лише
    `_source_file` (шлях відносно `landing_dir`) і `_ingested_at` (час цього запуску).
    """
    raw = spark.read.schema(EVENT_SCHEMA).json(f"{landing_dir}/dt=*/hour=*/*.ndjson")

    # input_file_name() повертає повний URI (file:///…); беремо останні три сегменти шляху
    # (dt=…/hour=…/part-….ndjson), щоб не залежати від того, де на диску лежить landing_dir.
    parts = F.split(F.regexp_replace(F.input_file_name(), r"\\", "/"), "/")
    n = F.size(parts)
    source_file = F.concat_ws(
        "/", F.element_at(parts, n - 2), F.element_at(parts, n - 1), F.element_at(parts, n)
    )

    return raw.select(
        "event_id",
        "event_type",
        "ride_id",
        F.to_timestamp("occurred_at").alias("occurred_at"),
        "source",
        "payload",
        source_file.alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )


def loaded_files(spark: SparkSession) -> set[str]:
    """Файли, які вже лежать у Bronze (ключ ідемпотентності). ДАНО."""
    query = f"(SELECT DISTINCT _source_file FROM {config.BRONZE_TABLE}) AS t"
    rows = spark.read.jdbc(config.JDBC_URL, query, properties=config.JDBC_PROPERTIES).collect()
    return {r["_source_file"] for r in rows}


def select_new(df: DataFrame, already_loaded: set[str]) -> DataFrame:
    """Лишає рядки з файлів, яких ще нема в Bronze. Порожній `already_loaded` — перший запуск."""
    if not already_loaded:
        return df
    return df.filter(~F.col("_source_file").isin(list(already_loaded)))


def write_bronze(df: DataFrame) -> None:
    """Append у `config.BRONZE_TABLE` ОДНІЄЮ транзакцією.

    Spark відкриває одне JDBC-з'єднання (= одну транзакцію) на партицію датафрейму: при кількох
    партиціях збій міг би лишити частину з них закомiченою, а частину — ні. `coalesce(1)` зводить
    запис до однієї партиції — одного з'єднання, — тож «або все, або нічого» гарантовано.
    """
    df.coalesce(1).write.mode("append").jdbc(
        config.JDBC_URL, config.BRONZE_TABLE, properties=config.JDBC_PROPERTIES
    )


def main() -> None:
    """ДАНО."""
    spark = build_spark("bronze-job")
    try:
        raw = read_landing(spark, str(config.LANDING_DIR))
        new = select_new(raw, loaded_files(spark)).cache()

        n_new = new.count()
        if n_new == 0:
            print("Bronze: нових файлів немає — пропускаю запис (ідемпотентно).")
        else:
            n_files = new.select("_source_file").distinct().count()
            write_bronze(new)
            print(f"Bronze: додано {n_new} подій із {n_files} файлів.")

        total = spark.read.jdbc(
            config.JDBC_URL,
            f"(SELECT count(*) AS n FROM {config.BRONZE_TABLE}) AS t",
            properties=config.JDBC_PROPERTIES,
        ).collect()[0]["n"]
        print(f"Bronze total: {total}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
