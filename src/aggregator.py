import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


NETFLIX_TEST_DATA_PATH = os.getenv("NETFLIX_TEST_DATA_PATH", "test_data")


def event_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)  # type: ignore
    t_env.get_config().get_configuration().set_boolean(
        "python.fn-execution.memory.managed", True
    )

    create_kafka_source_ddl = """
            CREATE TABLE netflix_play_events(
                user_id BIGINT,
                show_id VARCHAR,
                event_id VARCHAR,
                event_type VARCHAR,
                netflix_play_events TIMESTAMP,
                user_device VARCHAR
                message_offset BIGINT PRIMARY KEY METADATA FROM 'offset'
            ) WITH (
              'connector' = 'kafka',
              'topic' = 'netflix.play_events',
              'properties.group.id' = 'testGroup',
              'properties.bootstrap.servers' = 'broker:29092',
              'scan.startup.mode' = 'latest-offset',
              'format' = 'json'
            )
            """

    create_kafka_sink_ddl = """
            CREATE TABLE device_type_aggregations(
                created_at TIMESTAMP,
                user_device VARCHAR,
                event_count BIGINT
            ) WITH (
              'connector' = 'kafka',
              'topic' = 'netflix.device_type_aggregations',
              'properties.bootstrap.servers' = 'broker:29092',
              'format' = 'json'
            )
    """

    data_insert_query = """
    INSERT INTO device_type_aggregations
    select window_time as group_timestamp,
            user_device,
            count(event_id) as event_count
        FROM netflix_play_events
        FROM TABLE(
            TUMBLE(TABLE netflix_play_events, DESCRIPTOR(netflix_play_events), INTERVAL '10' MINUTES));
        GROUP BY window_time, user_device
    """

    t_env.execute_sql(create_kafka_source_ddl)
    t_env.execute_sql(create_kafka_sink_ddl)
    t_env.execute_sql(data_insert_query)


if __name__ == "__main__":
    event_processing()
