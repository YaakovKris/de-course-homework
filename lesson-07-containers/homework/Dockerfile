# Завдання 1 — Dockerfile для ingestor. Специфікація: SPEC.md → «Завдання 1».
#
# Один образ, одна стадія. Що має бути (деталі й бали — у SPEC.md):
#
#   * базовий образ python:3.12-slim з фіксованим тегом (не latest);
#   * PYTHONUNBUFFERED=1 — інакше логів завантаження не видно;
#   * non-root користувач (напр. appuser, uid 1000), USER — перед ENTRYPOINT;
#   * каталог /cache має належати цьому користувачу — туди монтується volume;
#   * залежності з app/requirements.txt ставляться ОКРЕМИМ шаром ПЕРЕД копіюванням
#     коду, з --no-cache-dir (правка коду не має перебудовувати pip install);
#   * ENTRYPOINT в exec-form запускає app/gh_ingest.py.
#
# Перевірте збірку:  docker build -t gh-ingest:latest .
# Скрипт app/gh_ingest.py — ДАНО, не редагуйте його.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY app/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN useradd --create-home --uid 1000 appuser \
	&& mkdir /cache \
	&& chown appuser:appuser /cache

COPY app /app

USER appuser

HEALTHCHECK CMD python -c "import psycopg"
ENTRYPOINT ["python", "gh_ingest.py"]
