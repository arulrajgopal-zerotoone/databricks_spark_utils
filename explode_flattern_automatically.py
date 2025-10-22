from pyspark.sql.functions import col,explode_outer,explode
from pyspark.sql.types import StructType,ArrayType
from pyspark.sql import DataFrame


def flatten_schema(df, prefix=""):
    flat_cols = []
    
    for field in df.schema.fields:
        col_name = f"{prefix}{field.name}" if prefix else field.name  # Maintain hierarchy in names
        
        if isinstance(field.dataType, StructType):  # If field is a StructType, recurse
            for sub_field in field.dataType.fields:
                flat_cols.append(col(f"{col_name}.{sub_field.name}").alias(f"{col_name}_{sub_field.name}"))
        else:
            flat_cols.append(col(col_name))
    
    return df.select(*flat_cols)


def recursive_flatten(df):
    while any(isinstance(field.dataType, StructType) for field in df.schema.fields):
        df = flatten_schema(df)
    
    return df

def explode_array(df,column_name):
    return df.withColumn(column_name, explode_outer(df[column_name]))



def explode_all_cols(source_df):
    processing_df = source_df

    for colm in source_df.schema.fields:
        if isinstance(colm.dataType, ArrayType):
            processing_df = explode_array(processing_df, colm.name)

    return processing_df


def flattern_and_explode(source_df):
    processing_df = source_df
    struct_array_present = True
    while True:
        if struct_array_present == False:
            break

        processing_df = recursive_flatten(processing_df)
        processing_df = explode_all_cols(processing_df)

        for i in processing_df.schema.fields:
            if isinstance(i.dataType, StructType) or isinstance(i.dataType, ArrayType):
                struct_array_present = True
                break
            else:
                struct_array_present = False
                
    return processing_df

            








