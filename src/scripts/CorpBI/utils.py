from pyspark.sql.types import *
from pyspark.sql.functions import col, trim, upper

def convertDataTypes(data_frame, integer_cols = [], long_cols = [], timestamp_cols = [], boolean_cols = [], double_cols = [], float_cols = [], date_cols=[]):
    for col_name in timestamp_cols:
        data_frame = data_frame.withColumn(col_name, col(col_name).cast(TimestampType()))

    for col_name in integer_cols:
        data_frame = data_frame.withColumn(col_name, col(col_name).cast(IntegerType()))

    for col_name in long_cols:
        data_frame = data_frame.withColumn(col_name, col(col_name).cast(LongType()))

    for col_name in boolean_cols:
        data_frame = data_frame.withColumn(col_name, col(col_name).cast(BooleanType()))

    for col_name in double_cols:
        data_frame = data_frame.withColumn(col_name, col(col_name).cast(DoubleType()))

    for col_name in float_cols:
        data_frame = data_frame.withColumn(col_name, col(col_name).cast(FloatType()))

    for col_name in date_cols:
        data_frame = data_frame.withColumn(col_name, col(col_name).cast(DateType()))

    return data_frame

def renameColumns(data_frame, mapping = {}):
    for source, destination in mapping.items():
        data_frame = data_frame.withColumnRenamed(source, destination)
    return data_frame

def cleanDataFrame(data_frame, to_upper_cols = []):
    for column_name in data_frame.columns:
        data_frame = data_frame.withColumn(column_name,trim(col(column_name)))
    for column_name in to_upper_cols:
        data_frame = data_frame.withColumn(column_name,upper(col(column_name)))
    return data_frame