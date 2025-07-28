import json
import urllib.parse
import boto3
import pandas as pd
import src.config as config
import src.data_definition
from src.etl import get_race_id, read_file, validate_data, transform_data, load_to_iceberg_table, merge_to_fact_table
import logging


# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
handler.setFormatter(formatter)
if not logger.hasHandlers():
    logger.addHandler(handler)


print('Loading main function')

def main(bucket, key):
    s3=config.s3
    athena=config.athena
    catalog_name = config.glue_catalog
    database_name = config.database_name
    table_location= config.table_location
    dst_table_name= config.fact_table

    try:
        logger.info('Starting ETL process for bucket: %s, key: %s', bucket, key)
        race_id = get_race_id(key)
        df = read_file(bucket_name=bucket, key_value=key, s3= s3)
        result = validate_data(df)
        if result["is_valid"]:
            transform_data(df, race_id)
            table = load_to_iceberg_table(df, catalog_name, database_name, table_location)
            logger.info("Data loaded in staging table: %s", table)
            merge_to_fact_table(athena, catalog_name, database_name, table_location, table, dst_table_name)
            logger.info("Data successfully merged in fact table: %s", dst_table_name)
        else:
            logger.error("Data validation failed")
            for error in result["errors"]:
                logger.error(error)
    except Exception as e:
        logger.exception("ETL process failed: %s", e)
        raise

if __name__ == "__main__":
    bucket = "add bucket name"
    key = "add/key/here.csv"
    try:
        main(bucket, key)
    except Exception as e:
        logger.error("Main execution failed: %s", e)


# def lambda_handler(event, context):
#     #print("Received event: " + json.dumps(event, indent=2))

#     # Get the object from the event and show its content type
#     bucket = event['Records'][0]['s3']['bucket']['name']
#     key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
#     try:
#         s3=config.s3
#         catalog_name = config.glue_catalog
#         database_name = config.database_name
#         table_location= config.table_location

#         race_id = get_race_id(key)

#         df = read_file(bucket_name=bucket, key_value=key, s3= s3)
#         result = validate_data(df)

#         if result["is_valid"]:
#             transform_data(df, race_id)
#             load_to_iceberg_table(df, catalog_name, database_name, table_location)
#             print("CSV schema and data is valid")
#         else:
#             print("data validation failed")
#             for error in result["errors"]:
#                 print(error)
#     except Exception as e:
#         print(e)
#         print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
#         raise e
              
