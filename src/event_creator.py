import json
import random
import socket
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import duckdb
from confluent_kafka import Producer

conf = {
    "bootstrap.servers": "localhost:9092,localhost:9092",
    "client.id": socket.gethostname(),
}

producer = Producer(conf)


def main():
    user_ids = duckdb.sql(
        """
        select user_id from 'test_data/netflix_users_raw.csv'
    """
    ).fetchall()

    show_ids = duckdb.sql(
        """
        select show_id from 'test_data/netflix_titles_raw.csv'
    """
    ).fetchall()

    for _ in range(100000):
        event_dict = {
            "user_id": random.choice(user_ids)[0],
            "show_id": random.choice(show_ids)[0],
            "event_id": str(uuid4()),
            "event_type": "play",
            "user_device": random.choice(["mobile", "browser", "smart-tv"]),
            "event_timestamp": (
                (datetime.now(tz=timezone.utc) - timedelta(days=2))
                + timedelta(seconds=1)
            ).isoformat(),
        }
        event_data = json.dumps(event_dict)

        def acked(err, msg):
            if err is not None:
                print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
            else:
                pass

        producer.produce("netflix.play_events", value=event_data, callback=acked)

        # Wait up to 1 second for events. Callbacks will be invoked during
        # this method call if the message is acknowledged.
        producer.poll(1)


if __name__ == "__main__":
    main()
