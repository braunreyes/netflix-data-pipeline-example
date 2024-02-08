from datetime import datetime
from pathlib import Path

from mimesis import Generic, Schema
from mimesis.locales import Locale

generic = Generic(Locale.EN, seed=0xFF)


def schema_definition():
    return {
        "user_id": generic.numeric.increment(),
        "user_name": generic.person.username(mask="U_d", drange=(100, 1000)),
        "date_of_birth": generic.person.birthdate(max_year=2002).strftime("%Y-%m-%d"),
        "email": generic.person.email(),
        "street_number": generic.address.street_number(),
        "street_name": generic.address.street_name(),
        "street_suffix": generic.address.street_suffix(),
        "state": generic.address.state(),
        "city": generic.address.city(),
        "zip_code": generic.address.zip_code(),
        "added_on": generic.datetime.datetime(start=2020, end=2024).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "updated_at": datetime.strptime("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"),
    }


def create_users_raw():
    file_name = "netflix_users_raw.csv"
    file_path = f"test_data/{file_name}"
    if not Path(file_path).is_file():
        print("creating users...")
        schema = Schema(schema=schema_definition, iterations=1000000)
        schema.create()
        schema.to_csv(file_path)  # type: ignore


def main():
    create_users_raw()


if __name__ == "__main__":
    main()
