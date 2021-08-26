from pyspark.sql.session import SparkSession

from test.Consts import CONSTS

def load_local_data_frames():
    spark = SparkSession.builder.master("local[10]")\
        .config("spark.driver.memory", "5G")\
        .getOrCreate()

    contracts_df = spark.read.parquet(CONSTS.LOCAL_PARQUETS_DESTINATION_FOLDER + 'contracts/*.parquet')
    
    accounts_df = spark.read.parquet(CONSTS.LOCAL_PARQUETS_DESTINATION_FOLDER + 'accounts/*.parquet')
    
    linkedaccountantlog_df = spark.read.parquet(CONSTS.LOCAL_PARQUETS_DESTINATION_FOLDER + 'linkedaccountantlog/*.parquet')

    return contracts_df, accounts_df, linkedaccountantlog_df