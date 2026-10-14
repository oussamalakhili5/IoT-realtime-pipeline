-- Apply once before starting a run with
-- IOT_SPARK_WINDOW_PERSISTENCE_MODE=silo_summaries.
-- The new tables are isolated by topic and run_id; existing tables/data are
-- not altered.

CREATE TABLE IF NOT EXISTS iot_stream_batch_progress (
    topic text NOT NULL,
    run_id text NOT NULL,
    sink_name text NOT NULL,
    last_batch_id bigint NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (topic, run_id, sink_name)
);

CREATE TABLE IF NOT EXISTS iot_global_hourly_summaries (
    topic text NOT NULL,
    run_id text NOT NULL,
    hour_start timestamptz NOT NULL,
    hour_end timestamptz NOT NULL,
    accepted_event_count bigint NOT NULL DEFAULT 0,
    pressure_observation_count bigint NOT NULL DEFAULT 0,
    pressure_sum double precision,
    pressure_min double precision,
    pressure_max double precision,
    temperature_observation_count bigint NOT NULL DEFAULT 0,
    temperature_sum double precision,
    temperature_min double precision,
    temperature_max double precision,
    humidity_observation_count bigint NOT NULL DEFAULT 0,
    humidity_sum double precision,
    humidity_min double precision,
    humidity_max double precision,
    PRIMARY KEY (topic, run_id, hour_start)
);

CREATE TABLE IF NOT EXISTS iot_daily_sensor_summaries (
    topic text NOT NULL,
    run_id text NOT NULL,
    summary_date date NOT NULL,
    sensor_id text NOT NULL,
    location_key text NOT NULL,
    location_id text,
    latitude double precision,
    longitude double precision,
    accepted_event_count bigint NOT NULL DEFAULT 0,
    pressure_observation_count bigint NOT NULL DEFAULT 0,
    pressure_sum double precision,
    pressure_min double precision,
    pressure_max double precision,
    temperature_observation_count bigint NOT NULL DEFAULT 0,
    temperature_sum double precision,
    temperature_min double precision,
    temperature_max double precision,
    humidity_observation_count bigint NOT NULL DEFAULT 0,
    humidity_sum double precision,
    humidity_min double precision,
    humidity_max double precision,
    PRIMARY KEY (
        topic, run_id, summary_date, sensor_id, location_key
    )
);

CREATE INDEX IF NOT EXISTS iot_daily_sensor_summaries_location_idx
    ON iot_daily_sensor_summaries (topic, run_id, summary_date)
    INCLUDE (sensor_id, latitude, longitude);

CREATE TABLE IF NOT EXISTS iot_window_status_hourly (
    topic text NOT NULL,
    run_id text NOT NULL,
    hour_start timestamptz NOT NULL,
    normal_count bigint NOT NULL DEFAULT 0,
    anomaly_suspected_count bigint NOT NULL DEFAULT 0,
    insufficient_data_count bigint NOT NULL DEFAULT 0,
    other_count bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (topic, run_id, hour_start)
);

CREATE TABLE IF NOT EXISTS iot_sensor_window_anomalies (
    topic text NOT NULL,
    run_id text NOT NULL,
    sensor_id text NOT NULL,
    location_key text NOT NULL,
    location_id text,
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    latitude double precision,
    longitude double precision,
    measurement_count bigint NOT NULL,
    pressure_observation_count bigint NOT NULL,
    avg_pressure_pa double precision,
    min_pressure_pa double precision,
    max_pressure_pa double precision,
    pressure_range_pa double precision,
    pressure_variation_anomaly boolean NOT NULL,
    temperature_observation_count bigint NOT NULL,
    avg_temperature_c double precision,
    min_temperature_c double precision,
    max_temperature_c double precision,
    temperature_range_c double precision,
    temperature_variation_anomaly boolean NOT NULL,
    humidity_observation_count bigint NOT NULL,
    avg_humidity_pct double precision,
    min_humidity_pct double precision,
    max_humidity_pct double precision,
    humidity_range_pct double precision,
    humidity_variation_anomaly boolean NOT NULL,
    anomaly_status text NOT NULL CHECK (
        anomaly_status = 'anomaly_suspected'
    ),
    PRIMARY KEY (
        topic, run_id, sensor_id,
        location_key, window_start, window_end
    )
);

CREATE INDEX IF NOT EXISTS iot_sensor_window_anomalies_location_idx
    ON iot_sensor_window_anomalies (topic, run_id, window_start)
    INCLUDE (sensor_id, latitude, longitude);
