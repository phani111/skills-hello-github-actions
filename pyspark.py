"""
Script to compare data between an ORC file and a BigQuery table.
This addresses the issue caused by writers earlier than HIVE-4243 and compares the data with BigQuery.
"""

import logging
from typing import Dict, Any

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
INPUT_SCHEMA_PATH = '/mnt/data/file-BYqwZAcMxZWEPPbjZYBQmhdD'
INPUT_ORC_PATH = "path_to_orc_file"
BIGQUERY_PROJECT = "your-project-id"
BIGQUERY_DATASET = "your-dataset"
BIGQUERY_TABLE = "your-table"

# Type mapping
TYPE_MAPPING: Dict[str, Any] = {
    "STRING": StringType(),
    "INT": IntegerType(),
    "FLOAT": FloatType(),
    "DATE": DateType(),
}

def create_spark_session() -> SparkSession:
    """Create and return a Spark session."""
    return SparkSession.builder.appName("CompareORCandBigQuery").getOrCreate()

def read_schema_csv(file_path: str) -> pd.DataFrame:
    """Read the schema information from a CSV file."""
    try:
        return pd.read_csv(file_path, sep='|', encoding='ISO-8859-1', error_bad_lines=False)
    except Exception as e:
        logger.error(f"Error reading schema CSV: {e}")
        raise

def read_orc_file(spark: SparkSession, file_path: str):
    """Read an ORC file without a predefined schema."""
    try:
        return spark.read.orc(file_path)
    except Exception as e:
        logger.error(f"Error reading ORC file: {e}")
        raise

def apply_schema(df, schema_df: pd.DataFrame):
    """Apply the schema from the CSV to the Spark DataFrame."""
    # Rename columns
    for i, new_name in enumerate(schema_df['target_column_name']):
        df = df.withColumnRenamed(f"_c{i}", new_name)
    
    # Cast columns to correct data type
    for _, row in schema_df.iterrows():
        col_name = row['target_column_name']
        data_type = TYPE_MAPPING.get(row['source_datatype'], StringType())
        df = df.withColumn(col_name, col(col_name).cast(data_type))
    
    return df

def read_bigquery_table(spark: SparkSession, project: str, dataset: str, table: str):
    """Read data from a BigQuery table into a Spark DataFrame."""
    try:
        return (
            spark.read.format("bigquery")
            .option("project", project)
            .option("dataset", dataset)
            .option("table", table)
            .load()
        )
    except Exception as e:
        logger.error(f"Error reading BigQuery table: {e}")
        raise

def compare_dataframes(df1, df2):
    """Compare two DataFrames using subtract operation."""
    diff1 = df1.subtract(df2)
    diff2 = df2.subtract(df1)
    
    logger.info(f"Records in ORC but not in BigQuery: {diff1.count()}")
    logger.info(f"Records in BigQuery but not in ORC: {diff2.count()}")
    
    if diff1.count() > 0:
        logger.info("Sample of records in ORC but not in BigQuery:")
        diff1.show(5, truncate=False)
    
    if diff2.count() > 0:
        logger.info("Sample of records in BigQuery but not in ORC:")
        diff2.show(5, truncate=False)

def main():
    """Main function to orchestrate the comparison process."""
    spark = create_spark_session()

    schema_df = read_schema_csv(INPUT_SCHEMA_PATH)
    logger.info("Schema information loaded successfully")

    orc_df = read_orc_file(spark, INPUT_ORC_PATH)
    logger.info("ORC file read successfully")

    orc_df = apply_schema(orc_df, schema_df)
    logger.info("Schema applied successfully to ORC data")

    bq_df = read_bigquery_table(spark, BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_TABLE)
    logger.info("BigQuery table read successfully")

    logger.info("Comparing ORC and BigQuery data:")
    compare_dataframes(orc_df, bq_df)

if __name__ == "__main__":
    main()



Key changes and additions:

We've added a new function read_bigquery_table to read data from BigQuery into a Spark DataFrame.

We've added a compare_dataframes function that uses the subtract operation to find differences between the two DataFrames.

In the main function, we now:

Read the ORC file and apply the schema
Read the corresponding BigQuery table
Compare the two DataFrames
We've removed the write_orc_file function as we're no longer writing the data back to ORC.

We've added configuration variables for the BigQuery project, dataset, and table.

To use this script, you'll need to:

Ensure you have the necessary permissions to read from the BigQuery table.
Install and configure the BigQuery connector for Spark. You can do this by including the appropriate JAR file when starting your Spark session.
Set the correct values for BIGQUERY_PROJECT, BIGQUERY_DATASET, and BIGQUERY_TABLE in the configuration section.
This script will read both the ORC file and the BigQuery table, then compare them using the subtract operation. It will report the number of records that differ between the two sources and show a sample of these differences.



Try again with different context
Add context...
Avatar for phani111-rsqnq
 i have a json config to be passed to this pyspark code 

