from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DecimalType, BooleanType, DateType, TimestampType
import pandas as pd

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

schema_from_list = create_custom_schema(mapping_input)
print("Schema from list:")
print(schema_from_list)

# Example usage with a CSV file:
csv_file_path = 'path/to/your/schema_mapping.csv'
schema_from_csv = create_custom_schema(csv_file_path)
print("\nSchema from CSV:")
print(schema_from_csv)
