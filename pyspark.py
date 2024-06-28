"""
Script to compare data between an ORC file and a BigQuery table.
This addresses the issue caused by writers earlier than HIVE-4243 and compares the data with BigQuery.
"""

import logging
from typing import Dict, Any

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, ArrayType
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
    "ARRAY<FLOAT>": ArrayType(FloatType()),
    # Add more mappings as needed
}

def create_spark_session() -> SparkSession:
    """Create and return a Spark session."""
    return SparkSession.builder.appName("CompareORCandBigQuery").getOrCreate()

def read_schema_csv(file_path: str) -> pd.DataFrame:
    """Read the schema information from a CSV file."""
    try:
        df = pd.read_csv(file_path, sep='|', encoding='ISO-8859-1', error_bad_lines=False)
        if df['isnullable'].dtype != bool:
            df['isnullable'] = df['isnullable'].astype(bool)
        return df
    except Exception as e:
        logger.error(f"Error reading schema CSV: {e}")
        raise

def get_data_type(type_string: str) -> Any:
    """Convert string data type to Spark data type, including array types."""
    if type_string.upper().startswith("ARRAY<"):
        inner_type = type_string[6:-1].strip().upper()
        return ArrayType(TYPE_MAPPING.get(inner_type, StringType()))
    return TYPE_MAPPING.get(type_string.upper(), StringType())

def create_spark_schema(schema_df: pd.DataFrame) -> StructType:
    """Create a Spark schema from the CSV schema information."""
    fields = []
    for _, row in schema_df.iterrows():
        field = StructField(
            name=row['src_column_name'],
            dataType=get_data_type(row['source_datatype']),
            nullable=row['isnullable']
        )
        fields.append(field)
    return StructType(fields)

def read_orc_file(spark: SparkSession, file_path: str, schema: StructType):
    """Read an ORC file with the provided schema."""
    try:
        return spark.read.schema(schema).orc(file_path)
    except Exception as e:
        logger.error(f"Error reading ORC file: {e}")
        raise

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

    spark_schema = create_spark_schema(schema_df)
    logger.info("Spark schema created successfully")

    orc_df = read_orc_file(spark, INPUT_ORC_PATH, spark_schema)
    logger.info("ORC file read successfully with applied schema")

    bq_df = read_bigquery_table(spark, BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_TABLE)
    logger.info("BigQuery table read successfully")

    logger.info("Comparing ORC and BigQuery data:")
    compare_dataframes(orc_df, bq_df)

if __name__ == "__main__":
    main()
