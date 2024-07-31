from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
import pandas as pd

def get_column_types(df: DataFrame) -> pd.DataFrame:
    """Returns a DataFrame with column names and their data types."""
    column_types = []
    for column in df.columns:
        col_type = df.schema[column].dataType
        column_types.append((column, str(col_type)))
    return pd.DataFrame(column_types, columns=['column', 'type'])

def compare_partition_types(spark: SparkSession, path_2022: str, path_2023: str):
    """Compares data types of two partitions and highlights differences."""
    try:
        df_2022 = spark.read.format("orc").load(path_2022)
        df_2023 = spark.read.format("orc").load(path_2023)

        print("Successfully read partitions.")

        types_2022 = get_column_types(df_2022)
        types_2023 = get_column_types(df_2023)

        comparison = pd.merge(types_2022, types_2023, on='column', suffixes=('_2022', '_2023'), how='outer')

        differences = []
        for _, row in comparison.iterrows():
            if row['type_2022'] != row['type_2023']:
                differences.append(f"Column '{row['column']}' has different types: {row['type_2022']} (2022) vs {row['type_2023']} (2023)")
            elif pd.isna(row['type_2022']):
                differences.append(f"Column '{row['column']}' exists in 2023 but not in 2022")
            elif pd.isna(row['type_2023']):
                differences.append(f"Column '{row['column']}' exists in 2022 but not in 2023")

        print("\nDifferences in data types between 2022 and 2023 partitions:")
        if differences:
            for diff in differences:
                print(diff)
        else:
            print("No differences in data types found.")

        comparison.to_csv('partition_type_comparison.csv', index=False)
        print("\nDetailed type comparison saved to 'partition_type_comparison.csv'")
    except Exception as e:
        print(f"Error comparing partitions: {str(e)}")

def main():
    spark = SparkSession.builder \
        .appName("PartitionTypeComparison") \
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.0") \
        .getOrCreate()

    spark.conf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    spark.conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")

    path_2022 = "gs://abc/abe_resul/year=2022"
    path_2023 = "gs://abc/abe_resul/year=2023"

    compare_partition_types(spark, path_2022, path_2023)

    spark.stop()

if __name__ == "__main__":
    main()
