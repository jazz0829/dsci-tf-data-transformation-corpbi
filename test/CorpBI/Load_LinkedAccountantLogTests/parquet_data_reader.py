from pyspark.sql.session import SparkSession

from test.Consts import CONSTS

def load_local_data_frames():
    spark = SparkSession.builder.master("local[10]")\
        .config("spark.driver.memory", "5G")\
        .getOrCreate()

    contractstatistics_raw_df = spark.read.parquet(CONSTS.LOCAL_PARQUETS_DESTINATION_FOLDER + 'raw_contractstatistics/*.parquet')
    
    accounts_df = spark.read.parquet(CONSTS.LOCAL_PARQUETS_DESTINATION_FOLDER + 'accounts/*.parquet')

    return contractstatistics_raw_df, accounts_df