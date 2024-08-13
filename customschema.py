from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DecimalType, BooleanType, DateType, TimestampType
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("CustomSchemaExample").getOrCreate()

def create_custom_schema(input_data):
    type_mapping = {
        'string': StringType(), 'int': IntegerType(), 'float': FloatType(),
        'boolean': BooleanType(), 'date': DateType(), 'timestamp': TimestampType()
    }
    
    def get_type(type_str):
        type_str = type_str.lower()
        if type_str.startswith(('numeric', 'decimal')):
            try:
                params = type_str[type_str.index('(') + 1 : type_str.index(')')].split(',')
                return DecimalType(int(params[0]), int(params[1])) if len(params) == 2 else DecimalType(38, 9)
            except:
                return DecimalType(38, 9)
        return type_mapping.get(type_str, StringType())

    if isinstance(input_data, str):  # If input is a file path
        df = pd.read_csv(input_data)
        return StructType([StructField(row['column_name'], get_type(row['data_type']), str(row['is_nullable']).lower() == 'true') 
                           for _, row in df.iterrows()])
    else:  # If input is a list
        return StructType([StructField(xs[0], get_type(xs[1]), xs[2].lower() == 'true') for xs in input_data])

# Example usage with a list:
mapping_input = [
    ["name", "string", "true"],
    ["age", "int", "false"],
    ["salary", "decimal(38,9)", "true"],
    ["hire_date", "date", "true"]
]

schema = create_custom_schema(mapping_input)

# Simulate reading data from a partitioned file where some partitions might be missing certain columns
data = [
    {"name": "Alice", "age": 30, "hire_date": "2021-01-01"},
    {"name": "Bob", "salary": 100000.00, "hire_date": "2020-05-15"}
]

# Create a DataFrame without specifying the schema to simulate reading from a partitioned file
df = spark.createDataFrame(data)

# Show the DataFrame without the custom schema
print("DataFrame without custom schema:")
df.show()

# Read the DataFrame with the custom schema
df_with_schema = spark.createDataFrame(df.rdd, schema)

# Show the DataFrame with the custom schema
print("DataFrame with custom schema:")
df_with_schema.show()

# Stop the Spark session
spark.stop()
