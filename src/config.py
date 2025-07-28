import os
import boto3
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import (
    StringType,
    LongType,
    TimestampType
)

# Move this code to somewhere else
s3 = boto3.client(
    's3',
    aws_access_key_id = os.environ.get("aws_access_key"),
    aws_secret_access_key = os.environ.get("aws_secret"),
    region_name = os.environ.get("aws_region_name")
)

athena = boto3.client(
    'athena',
    aws_access_key_id = os.environ.get("aws_access_key"),
    aws_secret_access_key = os.environ.get("aws_secret"),
    region_name = os.environ.get("aws_region_name")
)

glue_bucket= os.environ.get('destination_glue_bucket')
table_location = f"s3://{glue_bucket}/iceberg_tbl" 
database_name = os.environ.get('glue_database')
table_name= os.environ.get("destination_table")
fact_table= os.environ.get("fact_tbl")

glue_catalog = load_catalog(
    'default',
    **{
        'client.access-key-id': os.environ.get("aws_access_key"),
        'client.secret-access-key': os.environ.get("aws_secret"),
        'client.region': os.environ.get("aws_region_name")
    },
    type='glue'
)

# expecetd columns in the final dataframe
cols = ['event_id','event_year','event_code','event_num','session_id','timeUtc','driverNumber','rpm','speed','gear','throttle','brake','drs']

# iceberg table schema
schema = Schema(
    NestedField(1, "event_id", StringType(), required=False),
    NestedField(2, "event_year", StringType(), required=False),
    NestedField(3, "event_code", StringType(), required=False),
    NestedField(4, "event_num", StringType(), required=False),
    NestedField(5, "session_id", StringType(), required=False),
    NestedField(6, "timeUtc", TimestampType(), required=False),
    NestedField(7, "driverNumber", LongType(), required=False),
    NestedField(8, "rpm", LongType(), required=False),
    NestedField(9, "speed", LongType(), required=False),
    NestedField(10, "gear", LongType(), required=False),
    NestedField(11, "throttle", LongType(), required=False),
    NestedField(12, "brake", LongType(), required=False),
    NestedField(13, "drs", LongType(), required=False)
)

