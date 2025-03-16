from pyflink.table import TableEnvironment, EnvironmentSettings
from pyflink.table.window import Session, SessionWithGap
from pyflink.table.expressions import col, lit

def main():
    # 1. Initialize Table Environment
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # Optional: set the checkpointing or state backend if needed
    # t_env.get_config().set(...)

    # 2. Define source table for green-trips
    t_env.execute_sql("""
        CREATE TABLE green_trips (
            lpep_pickup_datetime TIMESTAMP(3),
            lpep_dropoff_datetime TIMESTAMP(3),
            PULocationID BIGINT,
            DOLocationID BIGINT,
            passenger_count BIGINT,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            WATERMARK FOR lpep_dropoff_datetime 
                AS lpep_dropoff_datetime - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'green_consumer',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

        # 3. Use a session window to count the streak of trips
    #    Then find the maximum session size per (pickup, dropoff).
    #    We'll produce a final dynamic table "session_results".
    t_env.execute_sql("""
        CREATE TABLE session_results AS
        WITH sessioned AS (
            SELECT
                PULocationID,
                DOLocationID,
                /* Session start and end for reference (optional) */
                SESSION_START(lpep_dropoff_datetime, INTERVAL '5' MINUTE) AS s_start,
                SESSION_END(lpep_dropoff_datetime, INTERVAL '5' MINUTE)   AS s_end,
                COUNT(*) AS session_size
            FROM green_trips
            GROUP BY
                SESSION(lpep_dropoff_datetime, INTERVAL '5' MINUTE),
                PULocationID,
                DOLocationID
        )
        SELECT
            PULocationID,
            DOLocationID,
            MAX(session_size) AS max_streak
        FROM sessioned
        GROUP BY PULocationID, DOLocationID
    """)

    # 4. Execute the pipeline
    #    "session_results" is a table sink, but to see the results
    #    in real time, you might need an actual sink connector 
    #    (e.g., printing to console or writing to another Kafka/Postgres table).
    print("Starting session window job...")
    t_env.execute("Session Window Job")

if __name__ == "__main__":
    main()
