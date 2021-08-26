from domain_Load_LinkedAccountantLog_Transformation import *
from utils import *

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from datetime import date

def load_data_frames(sys_argv):
      today = date.today()
      year = today.year
      month = today.month
      day = today.day

      filter = 'ingestionyear = ' + str(year) + ' and ingestionmonth = ' + str(month) + ' and ingestionday =' + str(day)
      args = getResolvedOptions(sys_argv, ['raw_db', 'domain_db', 'raw_contractstatistics_table',
                                    'domain_accounts_table', 'linkedaccountantlog_s3_destination'])
      glueContext = GlueContext(SparkContext.getOrCreate())

      contractstatistics_raw_df = glueContext.create_dynamic_frame.from_catalog(
            database=args['raw_db'],
            table_name=args['raw_contractstatistics_table'],
            push_down_predicate=filter).toDF()

      if contractstatistics_raw_df.count() > 0:
            accounts_df = glueContext.create_dynamic_frame.from_catalog(
                  database=args['domain_db'],
                  table_name=args['domain_accounts_table']).toDF()
            
      return contractstatistics_raw_df, accounts_df, args['linkedaccountantlog_s3_destination']

if __name__ == "__main__":
      contractstatistics_raw_df, accounts_df, linkedaccountantlog_s3_destination = load_data_frames(sys.argv)

      if contractstatistics_raw_df.count() == 0:
            sys.exit()

      contractstatistics_raw_df, accounts_df = prepare_data_frames(contractstatistics_raw_df, accounts_df)

      entrepreneur_contractstatistics_df = get_entrepreneur_contractstatistics(contractstatistics_raw_df)
      linked_accountant_df = aggregate_accountant_links(entrepreneur_contractstatistics_df, accounts_df)
      linkedaccountantlog_df = format_as_logs(linked_accountant_df)

      if linkedaccountantlog_df.count() > 0:
            linkedaccountantlog_df.repartition(1).write.mode("append").parquet(linkedaccountantlog_s3_destination)
