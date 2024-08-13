from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DecimalType, BooleanType, DateType, TimestampType
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("CustomSchemaExample").getOrCreate()

def create_custom_schema(df):
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

    # Create the schema
    schema = StructType([
        StructField(row['src_column_name'], get_type(row['source_datatype']), str(row['Isnullable']).lower() == 'true')
        for _, row in df.iterrows()
    ])
    
    return schema

# Example usage with a DataFrame:
data = {
    'src_column_name': ['name', 'age', 'salary', 'hire_date', 'tax_rate'],
    'target_column_name': ['name', 'age', 'salary', 'hire_date', 'tax_rate'],
    'source_datatype': ['string', 'int', 'decimal(38,9)', 'date', 'numeric(28,9)'],
    'Isnullable': ['true', 'false', 'true', 'true', 'true'],
    'iscolpresentindata': ['true', 'true', 'true', 'true', 'true'],
    'transformcols': ['', '', '', '', ''],
    'isrequired': ['true', 'true', 'true', 'true', 'true']
}

df = pd.DataFrame(data)

schema = create_custom_schema(df)
print("Schema from DataFrame:")
print(schema)

# Simulate reading data from a partitioned file where some partitions might be missing certain columns
data = [
    {"name": "Alice", "age": 30, "hire_date": "2021-01-01"},
    {"name": "Bob", "salary": 100000.00, "hire_date": "2020-05-15", "tax_rate": 0.15}
]

# Create a DataFrame without specifying the schema to simulate reading from a partitioned file
df_data = spark.createDataFrame(data)

# Show the DataFrame without the custom schema
print("DataFrame without custom schema:")
df_data.show()

# Read the DataFrame with the custom schema
df_with_schema = spark.createDataFrame(df_data.rdd, schema)

# Show the DataFrame with the custom schema
print("DataFrame with custom schema:")
df_with_schema.show()

# Stop the Spark session
spark.stop()
