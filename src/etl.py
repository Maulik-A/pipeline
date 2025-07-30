import os
import boto3
import time
import pandas as pd
import pyarrow as pa
from botocore.exceptions import ClientError
import logging
from typing import List, Dict, Tuple, Any, Optional
from src.data_definition import F1TelemetryValidator
import src.config as config


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
handler.setFormatter(formatter)
if not logger.hasHandlers():
    logger.addHandler(handler)


def get_race_id(key_value: str) -> Dict[str, str]:
    """
    Extracts race and session metadata from the S3 key.
    Takes S3 key for the file as an argument and returns file metadata in dictionary.
    """
    try:
        file_name_with_extention = key_value.split("/")[-1]
    except IndexError as e:
        logger.error("Key does not contain a valid file path or prefix: %s", key_value)
        raise ValueError("Key does not contain a valid file path or prefix.") from e

    try:
        extention = file_name_with_extention.split(".")[1]
        if extention == "csv":
            file_name = file_name_with_extention.split(".")[0]
            event_id = file_name.split("_")[0]
            session_id = file_name.split("_")[1]
            # table_name = "stg_" + str(file_name[0])
        else:
            logger.error("Not a valid csv file provided in the key: %s", file_name_with_extention)
            raise ValueError("Not a valid csv file provided in the key")
    except Exception as e:
        logger.error("Filename must be in format '<event_id>_<session_id>.csv': %s", file_name_with_extention)
        raise ValueError("filename must be in format '<event_id>_<session_id>.csv") from e

    return {
        "file_name_with_extention": file_name_with_extention,
        "file_name": file_name,
        "extention": extention,
        "event_id": event_id,
        "event_year": event_id[:2],
        "event_num": event_id[2:5],
        'event_code': event_id[5:],
        "session_id": session_id
    }


def read_file(bucket_name: str, key_value: str, s3: object) -> pd.DataFrame:
    """
    Reads a CSV file from S3 into a DataFrame.
    Takes bucket and key along with s3 client object.
    Returns pandas dataframe. 
    """
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=key_value)
        logger.info('Reading %s file from s3...', key_value)
        df = pd.read_csv(obj['Body'])
        return df
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            logger.error("Key doesn't match. Please check the key value entered: %s", key_value)
            raise FileNotFoundError(f"Key not found: {key_value}") from ex
        else:
            logger.error("ClientError while reading from S3: %s", ex)
            raise
    except Exception as e:
        logger.error("Error reading file from S3: %s", e)
        raise


def validate_data(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Validates the DataFrame using F1TelemetryValidator.
    Takes dataframe and validates agaisnt data definition.
    """
    try:
        validator = F1TelemetryValidator()
        result = validator.validate_csv_data(df)
        logger.info("Validation result: %s", result)
        return result
    except Exception as e:
        logger.error("Error during data validation: %s", e)
        raise


def transform_data(df: pd.DataFrame, file_metadata: Dict[str, str]) -> pd.DataFrame:
    """
    Transforms the dataframe by adding metadata columns and fixing types.
    Returns dataframe
    """
    try:
        if len(file_metadata) == 8:
            df.insert(0, 'session_id', file_metadata['session_id'])
            df.insert(0, 'event_num', file_metadata['event_num'])
            df.insert(0, 'event_code', file_metadata['event_code'])
            df.insert(0, 'event_year', file_metadata['event_year'])
            df.insert(0, 'event_id', file_metadata['event_id'])
        else:
            logger.error("Incorrect metadata provided: %s", file_metadata)
            raise ValueError("Provide correct metadata")
        df['timeUtc'] = pd.to_datetime(df['timeUtc'], errors='coerce')
        logger.info("Data transformed and added metadata: %s", file_metadata)
        return df
    except Exception as e:
        logger.error("Error during data transfornation: %s", e)
        raise


def load_to_iceberg_table(df: pd.DataFrame, catalog_name: object, database_name: str, table_location: str) -> str:
    """
    Loads DataFrame to an Iceberg table (staging table).
    Checks if stg tables exists and delete it before writing data.
    Then overwrites the data. So, each run will have new copy of the table.
    """
    try:
        final_df = pa.Table.from_pandas(df)
        final_df = final_df.select(config.cols)
        # Fixing timestamp column
        final_df = final_df.set_column(
            final_df.schema.get_field_index("timeUtc"),
            "timeUtc",
            final_df.column("timeUtc").cast(pa.timestamp("ms"))
        )
        # Check if dataframe is not empty before writing to table
        if not df['event_id'].empty:
            event_id = df['event_id'][0]
            session_id = df['session_id'][0]
            table_name = "stg_" + str(event_id) + "_" + str(session_id)
            identifier = (database_name, table_name)
            logger.info('Data is ready to populate: %s', identifier)
        else:
            logger.error("No data available to load in table.")
            raise Exception("No data available to load in table.")

        #  Drop table if already exists and create new one 
        if catalog_name.table_exists(identifier):
            catalog_name.drop_table(identifier)
            logger.info("Dropped existing table: %s", identifier)
        logger.info('Writing data in iceberg table: %s', identifier)
        table = catalog_name.create_table(
            identifier=(database_name, table_name),
            schema=config.schema,
            location=table_location
        )
        # Writing to stg table
        if catalog_name.table_exists(identifier):
            table.overwrite(final_df)
            logger.info("Data ingested successfully into %s", identifier)
        return table_name
    except Exception as e:
        logger.error("Error loading data to Iceberg table: %s", e)
        raise

def load_sql_query(filepath: str) -> str:
    with open(filepath, 'r') as file:
        return file.read()

def merge_to_fact_table(
    athena_client: object,
    catalog: str,
    database: str,
    athena_output_bucket: str,
    src_table: str,
    dst_table: str
) -> None:
    """
    Merges stage data into the fact table using Athena.
    It takes athena clients and details for source and destination tables.
    Performs merge operation with fact table.
    If successful, dropping the source (staging table) in the end.       
    """
    client = athena_client

    try:
        sql_path = 'src/sql/merge_fact_table.sql'
        query_template = load_sql_query(sql_path)
        query = query_template.format(
            catalog=catalog,
            database=database,
            src_table=src_table,
            dst_table=dst_table
        )
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': athena_output_bucket
            }
        )
        logger.info("Started Athena query for merging tables: %s", response)
        query_execution_id = response['QueryExecutionId']

        while True:
            response = client.get_query_execution(QueryExecutionId=query_execution_id)
            state = response['QueryExecution']['Status']['State']
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(10)
        if state == 'SUCCEEDED':
            logger.info("Successfully added data in the fact table: %s", dst_table)
            try:
                client.delete_table(CatalogId=catalog, DatabaseName=database, Name=src_table)
                logger.info("Deleted staging table: %s", src_table)
            except Exception as e:
                logger.warning("Failed to delete staging table %s: %s", src_table, e)
        else:
            logger.error("Error in ingesting data into fact table. State: %s", state)
            raise Exception(f"Athena query failed with state: {state}")
    except Exception as e:
        logger.error("Error during merge to fact table: %s", e)
        raise 