# Window persistence for the March 2026 run

## Current behavior

`build_windowed_metrics()` accepts events classified as `accepted`, then groups
them by topic, sensor, location and a 10-minute event-time window sliding every
5 minutes. The watermark delay is 10 minutes. The current query uses Spark
`update` output mode.

Each emitted row is upserted into `sensor_window_metrics` using
`(topic, sensor_id, window_start, window_end)` as the conflict key. Revisions
for the same window update that row. The writer stores the measurement count,
location, min/average/max readings, ranges, anomaly booleans and
`anomaly_status`; the Spark aggregate also computes population standard
deviations and per-measurement observation counts, but the existing PostgreSQL
writer does not store those fields. Every nonempty micro-batch is also written
to Silo under the historical topic/batch prefix.

That behavior is the default and remains unchanged. In the existing event
`silo` mode, classified raw events go to Silo and event details do not go to
PostgreSQL.

## Strategy B window-batch driver-memory update (2026-10-14)

In `silo_summaries` mode, `persist_window_metrics_batch()` now keeps detailed
window rows in the Spark DataFrame for the existing run-scoped Parquet write.
It computes hourly counts for `normal`, `anomaly_suspected`,
`insufficient_data`, and `other` in Spark, and collects only those compact
hour rows plus the full rows whose status is `anomaly_suspected`. A null or
unrecognized status is counted as `other`, matching the former Python helper.
The default `postgres` mode retains its existing full-row persistence path.

The Strategy B DataFrame is cached with `MEMORY_AND_DISK` while the hourly
aggregation, anomaly filter, and Parquet write consume it, then unpersisted in
a `finally` block. This avoids recomputing the window result for each action,
but introduces temporary executor memory and local-disk use. Empty batches
still claim PostgreSQL batch progress with empty summaries and do not write a
Parquet prefix. The Silo write still precedes the PostgreSQL summary
transaction, so there is no distributed transaction across those sinks.

This removes driver transfer of every detailed window row when computing the
status counts. The anomaly rows remain a possible driver-memory bottleneck:
Strategy B deliberately stores full suspected-anomaly details in PostgreSQL,
and their count is not yet profiled. Do not drop or sample them as a shortcut.
If measured anomaly cardinality is too large, assess a transaction-safe bulk
staging/COPY-and-merge design before changing the persistence contract.

The reported 100,000-event run had a roughly 137.5-second batch warning,
heartbeat timeouts, Py4J errors, and a `SparkContext was shut down` message.
The former full `batch_df.collect()` was an avoidable driver-transfer cost,
but the shutdown message means these observations alone do not identify it as
the cause of the interruption; JVM pauses, resource pressure, I/O, or an
operator stop remain possible. No comparable before/after workload was run
for this change, so no throughput improvement is claimed.

## Why full detailed PostgreSQL retention is risky

The profile contains 87,298,799 source rows and 5,156 sensors. A 10-minute
window sliding every 5 minutes has 8,929 possible starts that intersect March
2026: the March starts plus the single 2026-02-28 23:55 start that can contain
the earliest reported event at 2026-03-01 00:00:00. If each sensor has one
stable location per window, that gives 46,037,924
possible `(sensor_id, window_start)` slots. This is a conditional bound, not an
observed count: the actual Spark grouping also includes `location_id`,
`latitude` and `longitude`, so a sensor that changes location within a window
can produce multiple rows for that sensor/window. A conservative hard upper
bound from the profile is 174,597,598 window rows (`87,298,799 × 2`), since a
10-minute window sliding every five minutes can assign at most two windows to
one event. Neither value is an expected cardinality or a storage forecast.

`reports/dataset_profile.txt` has no sensor-window cardinality. Its distinct
sensor count and timestamp range cannot establish that number. The exact count
requires processing the source records with the producer's acceptance rules
and the full Spark grouping key. The read-only database check on 2026-10-13
found 272 existing `sensor_window_metrics` rows occupying 245,760 bytes; that
small sample is not suitable for extrapolating full-run storage.

The detailed accepted-event table is also intentionally omitted in the new
mode. At the measured storage scale for `sensor_events`, retaining all 87.3M
event rows could take tens of gigabytes; that is a rough scale illustration,
not a storage forecast.

## Strategy comparison and recommendation

| Strategy | PostgreSQL content | Strengths | Trade-offs |
| --- | --- | --- | --- |
| A | All detailed windows in Silo; anomaly windows and compact monitoring summaries in PostgreSQL | Lowest relational storage; anomalies remain directly queryable | “Compact monitoring” needs a deliberately chosen grain and can leave sensor-level trend/map questions to Silo queries |
| B | All detailed windows in Silo; hourly global summaries, daily per-sensor summaries, hourly window-status counts and anomaly windows in PostgreSQL | Keeps time-series panels, location maps, anomaly review and status distribution queryable in PostgreSQL | More summary tables and a small event-time delay for finalized window statuses |

Strategy B is implemented as an opt-in mode. Event summaries are bounded by
744 global hours and at most 159,836 sensor-days (`5,156 × 31`). Window-status
rows are bounded by 745 hour buckets because windows starting just before
March 1 can fall into the preceding hour. Suspected-anomaly rows are not known
from the reports; their conservative row bound is the 174,597,598 window-row
bound above. The exact anomaly count and physical table sizes still need
measurement before approving the full dataset. Since every accepted event can
contribute to at most two sliding windows, 174,597,598 is a conservative upper
bound for both detailed window rows and, separately, anomaly rows; it is not a
forecast.

The hourly and daily event summaries count each accepted source event once.
Hourly status rows count finalized 10-minute sensor windows, not raw events.
The 10-minute watermark means a status window is published after later event
timestamps advance the watermark. The final event-time tail may remain
unfinalized when a bounded producer stops; the raw events remain archived in
Silo for later recomputation. This is a known completion check for a finite
March replay.

There is a grain assumption to verify before the full run. The daily Spark
group includes `location_id`, `latitude` and `longitude`, while the PostgreSQL
daily primary key is only `(topic, run_id, summary_date, sensor_id)`; the
writer adds counts for rows that collide and replaces the location columns.
Likewise, the window aggregation groups by location fields, while the compact
anomaly table key identifies only topic/run/sensor/window. If a sensor has
multiple locations in a day or a window, daily map coordinates can represent
only the last upserted location, and multiple anomaly groups may collide. The
available profile does not prove that locations are stable at either grain.
Measure that condition or align the summary keys/grain before relying on maps
and per-location anomaly detail.

## Configuration and retry behavior

The new option is:

```text
IOT_SPARK_WINDOW_PERSISTENCE_MODE=postgres   # default, historical behavior
IOT_SPARK_WINDOW_PERSISTENCE_MODE=silo_summaries
```

`silo_summaries` requires `IOT_SPARK_EVENT_PERSISTENCE_MODE=silo`,
`IOT_SPARK_EVENT_SILO_RUN_ID`, and separate, non-default values for
`IOT_SPARK_ROUTING_CHECKPOINT_DIR` and
`IOT_SPARK_WINDOW_CHECKPOINT_DIR`. The run ID also scopes all new PostgreSQL
rows and the Silo window prefix. Use a fresh run ID whenever starting from new
checkpoints; reuse both the run ID and checkpoints only to resume that run.

Detailed window batches use:

```text
s3a://iot-data-lake/processed/window_metrics/
  topic=<topic>/run_id=<run-id>/batch_id=<20-digit-spark-batch-id>/
```

A retry of the same Spark batch overwrites only its deterministic
topic/run/batch path. Another batch or run has a different path. The writer
stores Silo first, then commits PostgreSQL output and the batch progress marker
in one transaction. A retried committed PostgreSQL batch is skipped; a failed
transaction rolls back both the progress marker and its summary writes.

The Silo overwrite is not atomic with PostgreSQL, and Spark's overwrite of a
batch prefix is not itself an atomic publish protocol. If a Silo write fails
partway through, that batch prefix may be incomplete until the batch retries
successfully. A retry rewrites only that same deterministic prefix; the
PostgreSQL transaction separately commits summaries and progress together.
There is no distributed transaction across Silo and PostgreSQL. Keep one active
consumer for a given topic/run/checkpoint pair.

The consumer configures the S3A endpoint and credentials but no S3A committer
was found in the current Spark configuration. The prefix scheme prevents a
batch from targeting another batch's path; it does not make an object-store
overwrite atomic or protect readers from seeing a partial prefix. Hadoop's
S3A guidance describes directory delete/rename as non-atomic and recommends
S3A committers for direct output-store writes. Choose and validate a committer
compatible with the Silo version before relying on batch output as an atomic
publish.

The existing event archive continues to use
`processed/event_records/topic=<topic>/run_id=<run-id>/batch_id=<id>`. No
existing Silo prefix or PostgreSQL table is altered by this code change.

Before enabling the new mode, apply
[`streaming_summary_schema.sql`](../sql/streaming_summary_schema.sql) to create
the new tables. This script only creates new objects with `IF NOT EXISTS`; it
does not modify the existing `sensor_events` or `sensor_window_metrics`
tables. The application does not run DDL automatically. After connecting with
the existing local secret handling (for example, a configured `.pgpass`), the
operator command is:

```powershell
psql -v ON_ERROR_STOP=1 -f .\sql\streaming_summary_schema.sql
```

## Read-only verification queries

Use a read-only database session when checking the results. Replace the topic
and run ID with the isolated run values.

```sql
BEGIN TRANSACTION READ ONLY;

SELECT count(*) AS detailed_event_rows
FROM sensor_events
WHERE topic = 'iot-sensor-events-silo-summaries-smoke-v2-20261013';

SELECT topic, run_id,
       count(*) AS hour_rows,
       sum(accepted_event_count) AS accepted_events
FROM iot_global_hourly_summaries
WHERE topic = 'iot-sensor-events-silo-summaries-smoke-v2-20261013'
  AND run_id = 'silo-summaries-smoke-v2-20261013'
GROUP BY topic, run_id;

SELECT topic, run_id,
       count(*) AS sensor_days,
       sum(accepted_event_count) AS accepted_events,
       count(*) FILTER (WHERE latitude IS NOT NULL AND longitude IS NOT NULL)
           AS rows_with_location
FROM iot_daily_sensor_summaries
WHERE topic = 'iot-sensor-events-silo-summaries-smoke-v2-20261013'
  AND run_id = 'silo-summaries-smoke-v2-20261013'
GROUP BY topic, run_id;

SELECT hour_start, normal_count, anomaly_suspected_count,
       insufficient_data_count, other_count
FROM iot_window_status_hourly
WHERE topic = 'iot-sensor-events-silo-summaries-smoke-v2-20261013'
  AND run_id = 'silo-summaries-smoke-v2-20261013'
ORDER BY hour_start;

SELECT window_start, sensor_id, location_id, latitude, longitude,
       anomaly_status, pressure_range_pa, temperature_range_c,
       humidity_range_pct
FROM iot_sensor_window_anomalies
WHERE topic = 'iot-sensor-events-silo-summaries-smoke-v2-20261013'
  AND run_id = 'silo-summaries-smoke-v2-20261013'
ORDER BY window_start, sensor_id;

SELECT relname AS table_name,
       pg_total_relation_size(relid) AS total_bytes
FROM pg_catalog.pg_statio_user_tables
WHERE relname IN (
    'iot_global_hourly_summaries',
    'iot_daily_sensor_summaries',
    'iot_window_status_hourly',
    'iot_sensor_window_anomalies',
    'iot_stream_batch_progress'
)
ORDER BY relname;

ROLLBACK;
```

Before and after a future smoke run, list the isolated prefix with an already
configured MinIO Client alias. Before the run, confirm both commands return no
objects. `IOT_SILO_MC_ALIAS` is the alias name only; do not echo credential
variables. `mc ls` is read-only:

```powershell
$topic = 'iot-sensor-events-silo-summaries-smoke-v2-20261013'
$runId = 'silo-summaries-smoke-v2-20261013'
if (-not $env:IOT_SILO_MC_ALIAS) { throw 'Set IOT_SILO_MC_ALIAS to a configured MinIO Client alias.' }
mc ls --recursive "$($env:IOT_SILO_MC_ALIAS)/iot-data-lake/processed/event_records/topic=$topic/run_id=$runId/"
mc ls --recursive "$($env:IOT_SILO_MC_ALIAS)/iot-data-lake/processed/window_metrics/topic=$topic/run_id=$runId/"
```

Check that each path has per-batch objects and `_SUCCESS`; use Spark with the
validated S3A classpath to read the Parquet schema and reconcile counts. Listing
alone does not prove Parquet readability or row counts.

## Grafana query changes

There are no provisioned dashboard JSON files in `dashboards/` yet, so no
existing panel query can be migrated here. New full-run panels should use:

- Global pressure, temperature and humidity series:
  `iot_global_hourly_summaries`; derive average as `*_sum / NULLIF(*_observation_count, 0)` and use `hour_start` as time.
- Sensor day trends and geomap locations:
  `iot_daily_sensor_summaries`; use `summary_date`, `sensor_id`, `latitude`, `longitude` and the same sum/count averages.
- Suspected anomaly table/count:
  `iot_sensor_window_anomalies`; filter by topic/run and `window_start`.
- Window status distribution:
  `iot_window_status_hourly`; chart its four count columns by `hour_start`.

Existing detailed-window panels that query `sensor_window_metrics` must switch
to these tables for a full run. The old table remains available for the
historical/default mode and existing smoke topics.

## Finite-run window finalization design

This has not been implemented. The live Kafka query is unbounded: an empty poll
does not prove that the March producer reached EOF. In append mode, Spark emits
an aggregate only after its watermark crosses the window end. With a
10-minute watermark, the latest event-time windows may therefore remain
unemitted at producer completion. Spark advances watermarks as input is
processed; it does not infer finite-source completion from a paused Kafka
topic.

Use a separate bounded finalization after producer completion is proven:

1. Confirm the producer process finished, all Kafka send futures were
   acknowledged, the producer progress position reached source EOF, and no
   producer will resume. Capture Kafka end offsets for every partition.
2. Wait until both Spark queries have committed through those offsets. Check
   their progress/checkpoint state and the relevant PostgreSQL batch progress
   records. A temporarily empty poll is not a completion signal.
3. Read the immutable Silo event archive for that topic/run as a bounded batch.
   Reuse the same JSON/schema/quality rules, keep only accepted events, and
   rebuild the 10-minute/5-minute windows without a watermark. This also
   restores late accepted events that the live watermark may have dropped.
4. Write finalized detailed windows to a new namespace such as
   `processed/window_metrics_finalized/topic=<topic>/run_id=<run-id>/finalization_id=<id>/`.
   Do not overwrite the streaming batch prefixes.
5. Recompute the window-status and anomaly outputs from that same complete
   batch. Store them in finalized-generation tables or add a
   `finalization_id` key with replace/upsert semantics. Do not add these counts
   to the live run's additive summary rows; a rerun must replace the same
   deterministic finalization generation, not count it twice.
6. Publish a completion marker only after the finalized Silo output and the
   PostgreSQL transaction have both been validated. There is no transaction
   spanning Silo and PostgreSQL, so a stable finalization ID plus read-only
   reconciliation and retry handling are required after partial failure.

The current tables and writers do not have this finalization-generation key,
so this procedure requires additive schema, writer and test work before it can
be used. A finalizer run twice must overwrite only its own deterministic Silo
generation and idempotently replace its PostgreSQL generation. If a finalizer
partially succeeds, retry or reconcile that same ID before marking completion;
never mix a new ID with partially committed output. Spark's live checkpoints
remain untouched throughout.

## Cardinality profiling without Kafka or database writes

The existing profile pass did not retain window keys, so an exact group count
cannot be reconstructed from its report. If a measurement is needed before the
full run, extend the existing CSV profiling pass rather than loading the file
into memory. For an approximate count, use a one-pass HyperLogLog over the
complete Spark grouping key `(topic, sensor_id, location_id, latitude,
longitude, window_start, window_end)` for accepted rows; a 16,384-register
sketch uses about 16 KiB and has roughly 0.8% nominal relative standard error.
This is an estimate, not an exact count. For an exact count, use an external
distinct-key store such as SQLite with the same key; its disk requirement could
be large and must be budgeted first. Either approach requires another
sequential 6.97 GB CSV read if run now. Neither scan nor database was run
during this audit.

## Current readiness snapshot (read-only checks on 2026-10-13)

This snapshot records local observations, not a full-run approval:

- C: had 35.61 GiB free. Keep at least 10 GiB free as an operational reserve;
  under that starting condition, at most about 25.61 GiB is available to the
  workload. This guardrail does not establish that the workload fits.
- PostgreSQL 18.6 database `iot_realtime_pipeline` occupied 33,216,191 bytes.
  `sensor_events` had 44,930 rows and occupied 16,433,152 bytes including
  indexes; `sensor_window_metrics` had 272 rows and occupied 245,760 bytes.
  The new Strategy B tables were absent. The schema SQL was inspected but not
  applied. PostgreSQL WAL was about 80 MiB at inspection; `max_wal_size` was
  configured to 1 GiB, which is a soft checkpoint limit rather than a hard
  cap. Temporary-file limits are unlimited. Peak WAL, Spark spill, and
  anomaly-table size are not forecast by these observations.
- The existing Kafka topic uses seven-day retention and has no topic byte cap;
  broker logs share C:. At 100 offsets per five-second trigger, the configured
  theoretical ceiling is 20 offsets/s and 87,298,799 rows require about 50.52
  days before overhead. A seven-day replay needs at least about 144.34 rows/s,
  before allowing for retries or lag. Retention was not changed.
- The local Spark JVM reported Spark 3.5.9 and Hadoop 3.3.4, but could not load
  `org.apache.hadoop.fs.s3a.S3AFileSystem`; matching S3A jars were not found in
  the inspected classpaths. The current Silo `s3a://` writer is blocked in
  this runtime. Hadoop requires `hadoop-aws` to match `hadoop-common` and also
  requires the AWS SDK bundle. No dependency was installed.
- The Docker API did not permit inspection of the MinIO volume, and a Silo
  prefix listing for the proposed smoke run did not complete. Silo capacity
  and candidate-prefix emptiness remain unverified.
- The environment was Python 3.14.7, PySpark/Spark 3.5.9 and Java 21. PySpark
  3.5.9 documents Python 3.8+ and Java 8, 11 or 17; Java 21 is outside that
  documented set. Spark startup succeeded. Python `tempfile` could create,
  write, read and clean a file directly under the selected scratch root, while
  the sandbox denied some newly created private temp subdirectories. A local
  Parquet test under the workspace first hit access denial; with a writable
  temp root it then failed in `NativeIO$Windows.access0`. The inherited test
  process had `HADOOP_HOME=C:\hadoop\bin` and omitted `C:\hadoop\bin` from
  `PATH`, while the user-level `HADOOP_HOME` was `C:\hadoop`. Explicitly
  setting the process `HADOOP_HOME`, `PATH` and Java `java.library.path` below
  let the JVM load Hadoop's Windows native library. The isolated Parquet test
  then passed, and the full 86-test unit suite passed. These are process-only
  values:

  ```powershell
  $scratch = Join-Path $env:LOCALAPPDATA 'Temp'
  $env:TEMP = $scratch
  $env:TMP = $scratch
  $env:SPARK_LOCAL_DIRS = $scratch
  $env:HADOOP_HOME = 'C:\hadoop'
  $env:PATH = "C:\hadoop\bin;$env:PATH"
  $env:JAVA_TOOL_OPTIONS = "-Djava.io.tmpdir=$scratch -Djava.library.path=C:\hadoop\bin"
  .\.venv\Scripts\python.exe -m unittest discover -s tests/unit -v
  ```

  This validates local tests only. It does not add S3A jars or prove Silo
  connectivity.
- A prior read-only Silo listing found
  `processed/event_records/topic=iot-sensor-events-march-silo-events-smoke-v1/run_id=event-silo-smoke-20261013/batch_id=00000000000000000001/`
  with `_SUCCESS` and one Parquet part (15,571 bytes), and the default window
  prefix
  `processed/window_metrics/topic=iot-sensor-events-march-silo-events-smoke-v1/batch_id=00000000000000000001/`
  with `_SUCCESS` and two parts (10,764 and 10,979 bytes). Their Parquet magic
  and footer lengths checked out, but schemas and row counts were not read, so
  they were not reconciled to the separate 100-event smoke test. The current
  default window path has no `run_id`; `silo_summaries` uses a run-scoped path.

At the time of this 2026-10-13 snapshot, S3A class loading and summary-table
DDL were unresolved. The following 2026-10-14 addendum supersedes those
specific observations. The full run remains gated on the other listed
capacity, throughput, sink-write, retention, and finite-run finalization
checks.

## Addendum: latest validated state (2026-10-14)

- The project configuration successfully reached the local Silo endpoint with
  a read-only Spark/S3A probe using Spark 3.5.9, Hadoop 3.3.4, and matching
  `hadoop-aws:3.3.4`. The historical event-record prefix listed and read as
  100 Parquet rows. This verifies read access only; Strategy B window writes,
  retries, and atomic visibility have not been integration-tested.
- The 8-event Strategy B smoke and the 10,000-event benchmark were reported
  successful. The 100,000-event run reported 100,000 archived events, 92,650
  accepted, 7,350 `review_required`, 200 event batches, and matching 92,650
  totals in the hourly and daily PostgreSQL event summaries. Its performance
  interruption remains under investigation as described above. These are
  existing-run observations, not a benchmark of the current code change.
- The Strategy B schema was absent in the last read-only PostgreSQL catalog
  inspection on 2026-10-13. The DDL has not been applied. Recheck the catalog
  and compare keys before proposing the create-only script; do not run it
  without explicit operator authorization.
- The optimized code and its regressions passed local unit tests on
  2026-10-14. The exact command/results are in `docs/CHATGPT_HANDOFF.md`.
  Those tests use synthetic inputs, mocks, or local Spark. They did not start
  Kafka, connect to PostgreSQL, write to Silo, or exercise production retries.
- No new streaming benchmark was started as part of this code change. A new
  benchmark requires a fresh topic, run ID, progress file, and both
  checkpoints, plus read-only absence checks immediately before execution.

## Isolated validation plan

The commands below are a plan only; they were not run. Verify the topic and
Silo topic/run prefix are absent with read-only listing first; those two
absence checks did not complete in this audit. The Kafka metadata check is:

```powershell
$topic = 'iot-sensor-events-silo-summaries-smoke-v2-20261013'
.\.venv\Scripts\python.exe -c "from kafka.admin import KafkaAdminClient; admin=KafkaAdminClient(bootstrap_servers='localhost:9092', request_timeout_ms=5000); print('topic_exists=', '$topic' in admin.list_topics()); admin.close()"
```

If `topic_exists=True` or either Silo prefix contains objects, select a new
date-stamped topic/run before continuing. Resolve the S3A classpath issue and
validate local Parquet output, then apply the additive summary schema to
the intended PostgreSQL database in a separately approved operation. Confirm
the input, progress file and checkpoint paths are new before starting. The
candidate input/progress/checkpoint paths below were absent in the read-only
check. If any later exists, choose a new date-stamped suffix; never reset or
overwrite a prior smoke run. Create the controlled ASCII CSV once, only if it
does not exist:

```powershell
$input = '.\data\interim\smoke_inputs\silo_summaries_smoke_v2_20261013.csv'
if (Test-Path -LiteralPath $input) { throw "Input already exists: $input" }
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $input) | Out-Null
@'
sensor_id;location;lat;lon;timestamp;pressure;temperature;humidity
30343;smoke-lab;33.5731;-7.5898;2026-03-01 00:00:00;1008.5;8;42
30343;smoke-lab;33.5731;-7.5898;2026-03-01 00:01:00;1009.0;9;45
30343;smoke-lab;33.5731;-7.5898;2026-03-01 00:02:00;1021.0;15;70
30343;smoke-lab;33.5731;-7.5898;2026-03-01 00:30:00;1010.0;4;47
30343;smoke-lab;33.5731;-7.5898;2026-03-01 00:31:00;1011.0;4;47
30343;smoke-lab;33.5731;-7.5898;2026-03-01 00:32:00;1012.0;4;47
30343;smoke-lab;33.5731;-7.5898;2026-03-01 01:00:00;1010.0;4;47
30343;smoke-lab;33.5731;-7.5898;2026-03-01 01:01:00;1011.0;4;47
30343;smoke-lab;33.5731;-7.5898;2026-03-01 01:02:00;1012.0;4;47
'@ | Set-Content -LiteralPath $input -Encoding Ascii
```

The first three records are intended to cross all three anomaly thresholds
with three observations; later timestamps advance the watermark so those
windows can finalize. Start the consumer in one PowerShell window:

```powershell
$topic = 'iot-sensor-events-silo-summaries-smoke-v2-20261013'
$runId = 'silo-summaries-smoke-v2-20261013'
$env:IOT_KAFKA_TOPIC = $topic
$env:IOT_SPARK_EVENT_PERSISTENCE_MODE = 'silo'
$env:IOT_SPARK_EVENT_SILO_RUN_ID = $runId
$env:IOT_SPARK_WINDOW_PERSISTENCE_MODE = 'silo_summaries'
$env:IOT_SPARK_ROUTING_CHECKPOINT_DIR = 'data/interim/checkpoints/spark_iot_silo_summaries_smoke_v2_20261013_routing'
$env:IOT_SPARK_WINDOW_CHECKPOINT_DIR = 'data/interim/checkpoints/spark_iot_silo_summaries_smoke_v2_20261013_windows'
$env:IOT_SPARK_MAX_OFFSETS_PER_TRIGGER = '3'
.\.venv\Scripts\python.exe -m src.streaming.kafka_spark_consumer
```

In a second window, publish this bounded sample to that topic with a new
progress file:

```powershell
$topic = 'iot-sensor-events-silo-summaries-smoke-v2-20261013'
.\.venv\Scripts\python.exe -m src.ingestion.kafka_producer `
  --input .\data\interim\smoke_inputs\silo_summaries_smoke_v2_20261013.csv `
  --topic $topic `
  --chunk-size 3 `
  --max-messages 9 `
  --progress-file .\data\interim\checkpoints\kafka_producer_silo_summaries_smoke_v2_20261013_progress.json
```

After publishing ends and the watermark has advanced, interrupt only the
isolated consumer and run the read-only verification queries above. A failed
smoke run should resume with the same topic, run ID, producer progress file and
Spark checkpoints. Do not reuse the run ID with new checkpoints. Verify that
`sensor_events` gains no detailed rows for the candidate topic, event Parquet
contains classified source records, window Parquet exists under the run-scoped
prefix, and PostgreSQL has compact hourly/daily/status and anomaly summaries.
The compatible S3A classpath is now validated for read access, but this does
not validate a Strategy B write. The summary schema remains uninstalled per
the last inspection and requires separate authorization. The topic, run ID,
input, progress-file, and checkpoint values in this 2026-10-13 procedure are
historical candidates; check them read-only and choose fresh values before any
new run. Retain benchmark artifacts as evidence; do not automate cleanup.

## Full-run procedure and gates

The full run remains gated. Before it, measure the exact window cardinality
with the profiling method above, check free space for PostgreSQL and Silo,
measure producer and Spark throughput, and verify Kafka retention is longer
than the maximum consumer lag. The default Spark cap of 100 offsets per
5-second trigger has a theoretical ceiling of 20 offsets/s; processing
87,298,799 source rows at that ceiling would take about 50.5 days before
overhead. The dataset average is about 32.6 rows/s across 31 days, so the
default trigger cap needs benchmarking and likely adjustment. These are rate
calculations, not a throughput test.

Once those checks pass, use a new topic, producer progress file, run ID and
both checkpoints. The sample names below are candidates only: verify each
topic/path is unused before starting and select a fresh suffix if any exists.
Start Spark first, then the producer; keep the same values to resume. Do not
reuse a run ID with new checkpoints because both Silo paths and the PostgreSQL
idempotency keys include that run ID.

```powershell
$topic = 'iot-sensor-events-march-full-v2-20261013'
$runId = 'march-full-v2-20261013'
$env:IOT_KAFKA_TOPIC = $topic
$env:IOT_SPARK_EVENT_PERSISTENCE_MODE = 'silo'
$env:IOT_SPARK_EVENT_SILO_RUN_ID = $runId
$env:IOT_SPARK_WINDOW_PERSISTENCE_MODE = 'silo_summaries'
$env:IOT_SPARK_ROUTING_CHECKPOINT_DIR = 'data/interim/checkpoints/spark_iot_kafka_routing_full_v2_20261013'
$env:IOT_SPARK_WINDOW_CHECKPOINT_DIR = 'data/interim/checkpoints/spark_iot_kafka_windows_full_v2_20261013'
$env:IOT_SPARK_MAX_OFFSETS_PER_TRIGGER = '1000' # only after a benchmark
.\.venv\Scripts\python.exe -m src.streaming.kafka_spark_consumer
```

```powershell
.\.venv\Scripts\python.exe -m src.ingestion.kafka_producer `
  --topic $topic `
  --chunk-size 1000 `
  --max-messages 90000000 `
  --progress-file .\data\interim\checkpoints\kafka_producer_march_full_v2_20261013_progress.json
```

These are operator procedures for a separately reviewed run; this audit did
not start the consumer, publish messages, apply DDL, or access Silo objects.
