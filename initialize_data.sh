#!/usr/bin/env bash

poetry run python src/create_data.py
docker exec -it spark-iceberg python /home/iceberg/scripts/load_data.py