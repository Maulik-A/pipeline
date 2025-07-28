CREATE TABLE fact_telemetry (
    pk_id STRING,
    event_id STRING,
    event_year STRING,
    event_code STRING,
    event_num STRING,
    session_id STRING,
    ts TIMESTAMP,
    drivernumber INT,
    rpm INT,
    speed INT,
    gear INT,
    throttle INT,
    breaks BOOLEAN,
    drs INT)
PARTITIONED BY (event_id) 
LOCATION 's3://telem-data/iceberg_tbl/fact_telemetry'
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet',
  'write_compression'='snappy',
  'optimize_rewrite_delete_file_threshold'='10'
)