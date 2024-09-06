from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import DecimalType

# Initialize Spark session
spark = SparkSession.builder.appName("ArrayDoubleToDecimal").getOrCreate()

# Sample DataFrame with array of doubles
data = [
    (1, [1.123456789, 2.987654321]),
    (2, [3.141592653, 2.718281828])
]

df = spark.createDataFrame(data, ["id", "double_array"])

# Show the original DataFrame
print("Original DataFrame:")
df.show(truncate=False)

# Use the built-in transform function to cast each element to Decimal(38,9)
df = df.withColumn("decimal_array", expr("transform(double_array, x -> CAST(x AS DECIMAL(38,9)))"))

# Show the result
print("DataFrame with decimal_array:")
df.select("id", "decimal_array").show(truncate=False)

# Stop the Spark session
spark.stop()
