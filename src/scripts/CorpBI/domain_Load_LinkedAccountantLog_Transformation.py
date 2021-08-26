import sys
from utils import *
from pyspark.sql.functions import when, lit, count, col
from datetime import date, timedelta

def get_entrepreneur_contractstatistics(contractstatistics_raw_df):      
      entrepreneur_contractstatistics_df = contractstatistics_raw_df\
            .withColumn('environment', 
                  when(col('environment') == 'GB','UK')\
                  .otherwise(col('environment')))\
            .where((col('isaccountant') == 'Entrepreneur') 
                  & (col('meta_load_enddts').isNull())
                  & (col('meta_isdeleted') == False))\
            .select('environment',
                  col('code').alias('accountcode'),
                  'accountantorlinked',
                  'accountantcode')\
            .distinct()

      return entrepreneur_contractstatistics_df

def aggregate_accountant_links(entrepreneur_contractstatistics_df, accounts_df):      
      entrepreneur_contractstatistics_df = entrepreneur_contractstatistics_df.alias('cs')
      accounts_df = accounts_df.alias('ac')

      change_df = entrepreneur_contractstatistics_df\
            .join(accounts_df, (col("cs.environment") == col("ac.environment")) & (col("cs.accountcode") == col("ac.accountcode")), 'inner')\
            .where(
                  (col("ac.accountantorlinked") != col("cs.accountantorlinked"))
                  | (col("ac.accountantcode") != col("cs.accountantcode"))
            )\
            .select("cs.environment",
                  "cs.accountcode",
                  col("cs.accountantorlinked").alias("newaccountantorlinked"),
                  col("ac.accountantorlinked").alias("previousaccountantorlinked"),
                  col("cs.accountantcode").alias("newaccountantcode"),
                  col("ac.accountantcode").alias("previousaccountantcode"))
      return change_df

def format_as_logs(linked_accountant_df):
      yesterday = date.today() - timedelta(1)

      linkedaccountantlog_df = linked_accountant_df\
            .withColumn('date', lit(yesterday))\
            .withColumn('linkstatus',
                  when(
                        ((col('newaccountantorlinked') == True) & (col('previousaccountantorlinked') == False)),
                        'Accountant Linked')\
                  .when(
                        ((col('newaccountantorlinked') == False) & (col('previousaccountantorlinked') == True)),
                        'Accountant Link Removed')\
                  .when(
                        ((col('newaccountantorlinked') == True) & (col('previousaccountantorlinked') == True)
                        & (col('newaccountantcode') != col('previousaccountantcode'))),
                        'Accountant Change')
            )\
            .select("environment",
                  "accountcode",
                  "linkstatus",
                  "date",
                  "newaccountantcode",
                  "previousaccountantcode")
      return linkedaccountantlog_df

def prepare_data_frames(contractstatistics_raw_df, accounts_df):
      contractstatistics_raw_df = cleanDataFrame(contractstatistics_raw_df, ['environment', 'code', 'accountantcode'])
      contractstatistics_raw_df = convertDataTypes(
            data_frame = contractstatistics_raw_df,
            boolean_cols = ['meta_isdeleted', 'accountantorlinked'])
      accounts_df = cleanDataFrame(accounts_df, ['environment', 'accountcode', 'accountantcode'])
      accounts_df = convertDataTypes(data_frame = accounts_df, boolean_cols = ['accountantorlinked'])
      
      contractstatistics_raw_df.cache()
      accounts_df.cache()

      return contractstatistics_raw_df, accounts_df