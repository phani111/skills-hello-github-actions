from pyspark.sql import SparkSession
from pyspark.sql.functions import max, length, when, col, instr, lit, udf
from pyspark.sql.types import DoubleType, IntegerType, LongType, FloatType, DecimalType, StringType, StructType, StructField
from decimal import Decimal

def analyze_columns(file_path):
    spark = SparkSession.builder.appName("Analyze ORC Data").getOrCreate()

    try:
        df = spark.read.orc(file_path)
        print("Sample data:")
        df.show(5, truncate=False)

        def get_precision_scale(value):
            if value is None:
                return (None, None)
            d = Decimal(str(value)) # Convert to Decimal for accurate analysis
            return (len(str(d).strip('-').split('.')[0]), len(str(d).split('.')[-1])) 

        get_precision_scale_udf = udf(get_precision_scale, StructType([
            StructField("precision", IntegerType(), True),
            StructField("scale", IntegerType(), True)
        ]))

        for column in df.columns:
            col_type = df.schema[column].dataType

            if isinstance(col_type, (DoubleType, FloatType, DecimalType, IntegerType, LongType)):
                analysis = df.withColumn("precision_scale", get_precision_scale_udf(col(column)))\
                             .select(
                                 lit(column).alias("column_name"),
                                 lit(str(col_type)).alias("data_type"),
                                 max("precision_scale.precision").alias("max_precision"),
                                 max("precision_scale.scale").alias("max_scale")
                             )
            elif isinstance(col_type, StringType):
                analysis = df.select(
                    lit(column).alias("column_name"),
                    lit(str(col_type)).alias("data_type"),
                    max(length(col(column))).alias("max_length"),
                    lit(None).alias("max_scale")
                )
            else:
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
