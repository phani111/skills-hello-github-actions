import sys
import logging
import os
from typing import List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, DecimalType, StringType, IntegerType
from pyspark.sql.functions import col

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session() -> SparkSession:
    """Create and return a Spark session optimized for ORC reading and BigQuery writing."""
    return SparkSession.builder \
        .appName("BigQuery ORC Writer") \
        .config("spark.sql.orc.impl", "native") \
        .config("spark.sql.orc.enableVectorizedReader", "true") \
        .config("spark.sql.broadcastTimeout", "600") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar") \
        .getOrCreate()

def read_orc_file(spark: SparkSession, file_path: str, schema: StructType) -> DataFrame:
    """Read an ORC file with the given schema."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"ORC file not found: {file_path}")
    try:
        return spark.read.schema(schema).orc(file_path)
    except Exception as e:
        logger.error(f"Error reading ORC file: {str(e)}")
        raise

def process_decimal_columns(df: DataFrame) -> DataFrame:
    """
    Process decimal columns to ensure compatibility with BigQuery NUMERIC types.
    Maintains the original precision and scale, which BigQuery will adjust during import.
    
    Parameters:
    df (DataFrame): The input DataFrame with potential decimal columns.
    
    Returns:
    DataFrame: The DataFrame with processed decimal columns.
    """
    for field in df.schema:
        if isinstance(field.dataType, DecimalType):
            try:
                # Maintain the original precision and scale
                original_precision = field.dataType.precision
                original_scale = field.dataType.scale
                
                # Recast the column to ensure it's treated as a decimal
                df = df.withColumn(field.name, col(field.name).cast(DecimalType(original_precision, original_scale)))
            except Exception as e:
                logger.error(f"Error processing column {field.name} with precision {original_precision} and scale {original_scale}: {str(e)}")
                # In case of error, keep the original column as is
    return df

def write_to_bigquery(df: DataFrame, project_id: str, dataset_id: str, table_id: str, gcs_temp_bucket: str,
                     partition_field: Optional[str] = None, 
                     cluster_fields: Optional[List[str]] = None, 
                     write_disposition: str = 'WRITE_APPEND') -> None:
    """Write the DataFrame to BigQuery using the Spark BigQuery connector."""
    try:
        bq_writer = df.write.format('bigquery') \
            .option('table', f'{project_id}:{dataset_id}.{table_id}') \
            .option('temporaryGcsBucket', gcs_temp_bucket) \
            .option('createDisposition', 'CREATE_IF_NEEDED') \
            .option('writeDisposition', write_disposition)

        if partition_field:
            bq_writer = bq_writer.option('partitionField', partition_field)
        if cluster_fields:
            bq_writer = bq_writer.option('clusterFields', ','.join(cluster_fields))

        bq_writer.mode('append').save()
        logger.info(f"Successfully wrote data to BigQuery table {project_id}:{dataset_id}.{table_id}")
    except Exception as e:
        logger.error(f"Error writing to BigQuery: {str(e)}")
        raise

def get_schema() -> StructType:
    """Return the schema for the ORC file."""
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("price", DecimalType(38, 18), True)  # Use high precision and scale
    ])

def main(orc_file_path: str, project_id: str, dataset_id: str, table_id: str, gcs_temp_bucket: str, 
         partition_field: Optional[str] = None, 
         cluster_fields: Optional[List[str]] = None) -> None:
    """Main function to read ORC file, process it, and write to BigQuery."""
    spark = create_spark_session()

    try:
        schema = get_schema()
        df = read_orc_file(spark, orc_file_path, schema)
        df = process_decimal_columns(df)
        write_to_bigquery(df, project_id, dataset_id, table_id, gcs_temp_bucket,
                          partition_field, cluster_fields)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    # Configuration (consider moving these to environment variables or a config file)
    ORC_FILE_PATH = os.environ.get("ORC_FILE_PATH", "path/to/your/orc/file")
    PROJECT_ID = os.environ.get("PROJECT_ID", "your-project-id")
    DATASET_ID = os.environ.get("DATASET_ID", "your-dataset-id")
    TABLE_ID = os.environ.get("TABLE_ID", "your-table-id")
    GCS_TEMP_BUCKET = os.environ.get("GCS_TEMP_BUCKET", "your-temporary-gcs-bucket")
    PARTITION_FIELD = os.environ.get("PARTITION_FIELD")
    CLUSTER_FIELDS = os.environ.get("CLUSTER_FIELDS", "").split(",") if os.environ.get("CLUSTER_FIELDS") else None

    main(ORC_FILE_PATH, PROJECT_ID, DATASET_ID, TABLE_ID, GCS_TEMP_BUCKET, 
         PARTITION_FIELD, CLUSTER_FIELDS)
