from pyspark.sql import SparkSession
from pyspark.sql.functions import max, length, when, col, instr, lit
from pyspark.sql.types import DoubleType, IntegerType, LongType, FloatType, DecimalType, StringType

def analyze_columns(file_path):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Analyze ORC Data") \
        .getOrCreate()

    try:
        # Read ORC file
        df = spark.read.orc(file_path)

        # Show a sample of data
        print("Sample data:")
        df.show(5, truncate=False)

        # Analyze each column
        for column in df.columns:
            col_type = df.schema[column].dataType
            
            if isinstance(col_type, (DoubleType, FloatType, DecimalType)):
                # For numeric types that can have decimals
                analysis = df.select(
                    lit(column).alias("column_name"),
                    lit(str(col_type)).alias("data_type"),
                    max(when(col(column).isNotNull(),
                        length(col(column).cast("string").substr(1, instr(col(column).cast("string"), '.') - 1))
                    )).alias("max_precision"),
                    max(when(col(column).isNotNull(),
                        length(col(column).cast("string").substr(instr(col(column).cast("string"), '.') + 1, 100))
                    )).alias("max_scale")
                )
            elif isinstance(col_type, (IntegerType, LongType)):
                # For integer types
                analysis = df.select(
                    lit(column).alias("column_name"),
                    lit(str(col_type)).alias("data_type"),
                    max(when(col(column).isNotNull(), length(col(column).cast("string")))).alias("max_length"),
                    lit(None).alias("max_scale")
                )
            elif isinstance(col_type, StringType):
                # For string type
                analysis = df.select(
                    lit(column).alias("column_name"),
                    lit(str(col_type)).alias("data_type"),
                    max(length(col(column))).alias("max_length"),
                    lit(None).alias("max_scale")
                )
            else:
                # For other types
                analysis = df.select(
                    lit(column).alias("column_name"),
                    lit(str(col_type)).alias("data_type"),
                    lit(None).alias("max_length"),
                    lit(None).alias("max_scale")
                )

            print(f"\nAnalysis results for column '{column}':")
            analysis.show(truncate=False)

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    file_path = "gs://your_bucket/your_orc_file.orc"
    analyze_columns(file_path)
