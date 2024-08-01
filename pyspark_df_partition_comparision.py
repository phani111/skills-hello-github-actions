from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
import pandas as pd
import argparse
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_column_types(df: DataFrame) -> pd.DataFrame:
    """Returns a DataFrame with column names and their data types."""
    column_types = []
    for column in df.columns:
        col_type = df.schema[column].dataType
        column_types.append((column, col_type, str(col_type)))
    return pd.DataFrame(column_types, columns=['column', 'type', 'type_str'])

def compare_and_cast_partitions(spark: SparkSession, path_2022: str, path_2023: str):
    """Compares data types of two partitions, casts 2022 to 2023 types if different."""
    try:
        df_2022 = spark.read.format("orc").load(path_2022)
        df_2023 = spark.read.format("orc").load(path_2023)

        logging.info("Successfully read partitions.")

        types_2022 = get_column_types(df_2022)
        types_2023 = get_column_types(df_2023)

        comparison = pd.merge(types_2022, types_2023, on='column', suffixes=('_2022', '_2023'), how='outer')

        for _, row in comparison.iterrows():
            column = row['column']
            type_2022 = row['type_2022']
            type_2023 = row['type_2023']

            if pd.isna(type_2022):
                logging.warning(f"Column '{column}' exists in 2023 but not in 2022. Skipping casting.")
                continue
            if pd.isna(type_2023):
                logging.warning(f"Column '{column}' exists in 2022 but not in 2023. Skipping casting.")
                continue

            if type_2022 != type_2023:
                logging.warning(f"Column '{column}' has different types: {type_2022} (2022) vs {type_2023} (2023). Casting 2022 to 2023 type.")
                try:
                    df_2022 = df_2022.withColumn(column, df_2022[column].cast(type_2023))
                except Exception as e:
                    logging.error(f"Error casting column '{column}': {str(e)}")
                    # Handle the error appropriately (e.g., skip casting, raise an exception)

        # Now df_2022 should have its columns cast to match df_2023's schema
        # You can proceed with your operations using the modified df_2022

    except FileNotFoundError as e:
        logging.error(f"File not found: {str(e)}")
    except PermissionError as e:
        logging.error(f"Permission denied: {str(e)}")
    except Exception as e:
        logging.error(f"Error comparing or casting partitions: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Compare and cast data types of two ORC partitions.")
    parser.add_argument("--path_2022", required=True, help="Path to the 2022 partition")
    parser.add_argument("--path_2023", required=True, help="Path to the 2023 partition")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("PartitionTypeComparison") \
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.0") \
        .getOrCreate()

    spark.conf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    spark.conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")

    compare_and_cast_partitions(spark, args.path_2022, args.path_2023)

    spark.stop()

if __name__ == "__main__":
    main()
