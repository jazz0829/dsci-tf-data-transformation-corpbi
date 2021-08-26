# Use Python2 to run this script!
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from test.Consts import CONSTS
from test.infrastructure.sql_data_reader import execute_sql_query
from pyspark.sql.session import SparkSession

def sql_to_parquet(table_name, output_folder = None):
    print("Downloading ::", table_name)
    driver_path = os.path.join(os.path.dirname(__file__), 'infrastructure' ,'mssql-jdbc-6.4.0.jre8.jar')
    spark = SparkSession.builder.master("local[6]")\
        .config("spark.driver.extraClassPath", driver_path)\
        .getOrCreate()

    jdbcDF = spark.read.format("jdbc")\
        .option("url", "jdbc:sqlserver://NLC1PRODCI01:1433;databasename=CustomerIntelligence;IntegratedSecurity=true")\
        .option("dbtable", table_name)
    df = jdbcDF.load()

    for colName in df.columns:
        ### Uncomment if raw data is being downloaded
        #df = df.withColumn(colName, col(colName).cast(StringType()))
        df = df.withColumnRenamed(colName, colName.lower())
    
    print("Saving parquets...")
    parquet_folder_path = os.path.join(CONSTS.LOCAL_PARQUETS_DESTINATION_FOLDER, output_folder)
    df.repartition(2).write.mode("overwrite").parquet(parquet_folder_path)
    df.printSchema()
    print("Saved ::", parquet_folder_path)

if __name__ == "__main__":
    sql_to_parquet("domain.linkedaccountantlog", "linkedaccountantlog")
    sql_to_parquet("domain.accounts", "accounts")
    sql_to_parquet("domain.contracts", "contracts")
    sql_to_parquet("raw.dw_contractstatistics", "raw_contractstatistics")