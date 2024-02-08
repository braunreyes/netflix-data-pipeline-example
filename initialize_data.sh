#!/usr/bin/env bash

poetry run python src/create_data.py
docker exec -it spark-iceberg python /home/iceberg/scripts/load_data.py
poetry run python src/event_creator.py
poetry run dbt snapshot
poetry run dbt run