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
