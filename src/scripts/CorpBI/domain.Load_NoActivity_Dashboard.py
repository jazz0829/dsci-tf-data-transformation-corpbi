import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from pyspark.sql.functions import col, lit, when, months_between, current_date, concat, trunc
from pyspark.sql.types import IntegerType
from awsglue.utils import getResolvedOptions
from datetime import datetime, date, timedelta

def load_noactivity_dashboard(config_calendar_df, config_activities_df, accountscontract_summary_df, activitydaily_df, accounts_df, users_df, s3_destination):
      config_calendar_df = config_calendar_df.withColumn("date", trunc("calendardate", "month"))
      date_since = config_calendar_df.where(months_between(current_date(), col('date')) <= 3).agg({"date": "min"}).first()['min(date)']
      date_current = date.today() - timedelta(days=1)

      activitydaily_df = activitydaily_df\
            .where((col('activityid') == 1) & (col('date') >= date_since))\
            .groupBy("environment", "userid", "date")\
            .agg({"quantity": "sum"})\
            .withColumnRenamed("sum(quantity)", "sumquantity")

      noactivity_dashboard_df = accountscontract_summary_df.alias('acs')\
                                    .join(accounts_df.alias('a'), col('acs.accountid') == col('a.accountid'), 'inner')\
                                    .withColumn('join_condition', when(col('acs.churned') == 1, col('acs.latestcommfinaldate')).otherwise(current_date()))\
                                    .join(
                                          config_calendar_df.alias('cc'), 
                                          col('cc.calendardate').between(date_since, date_current) & col('cc.calendardate').between(col('acs.firstcommstartdate'), col('join_condition')), 
                                          'inner'
                                    )\
                                    .join(users_df.alias('u'), col('a.accountid') == col('u.accountid'), 'inner')\
                                    .join(activitydaily_df.alias('ad'), col('ad.userid') == col('u.userid'), 'left')\
                                    .where(col('a.accountclassificationcode').isin('EOL', 'ACC', 'AC7', 'JB0', 'AC1', 'EO1','AC8'))\
                                    .groupBy("acs.accountid", "acs.accountcode", "acs.environment", "cc.calendardate")\
                                    .agg({"sumquantity": "sum"})\
                                    .withColumnRenamed("sum(sumquantity)", "pageviewquantity")\
                                    .withColumn("pageviewquantity", col('pageviewquantity').cast(IntegerType()))\
                                    .withColumn("activeornot", when(col('pageviewquantity').isNull(), lit(0)).otherwise(lit(1)))\
                                    .withColumn("enviroaccount", concat(col("acs.environment"), col("acs.accountcode")))

      noactivity_dashboard_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

if __name__ == "__main__":
      args = getResolvedOptions(sys.argv, [
            'domain_db', 
            'config_calendar_table', 
            'config_activities_table', 
            'domain_activitydaily_table', 
            'domain_accountscontract_summary_table', 
            'domain_accounts_table', 
            'domain_users_table', 
            's3_destination'
      ])

      glueContext = GlueContext(SparkContext.getOrCreate())

      config_calendar_df = glueContext.create_dynamic_frame.from_catalog(database=args['domain_db'], table_name=args['config_calendar_table']).toDF()
      config_activities_df = glueContext.create_dynamic_frame.from_catalog(database=args['domain_db'], table_name=args['config_activities_table']).toDF()
      accountscontract_summary_df = glueContext.create_dynamic_frame.from_catalog(database=args['domain_db'], table_name=args['domain_accountscontract_summary_table']).toDF()
      activitydaily_df = glueContext.create_dynamic_frame.from_catalog(database=args['domain_db'], table_name=args['domain_activitydaily_table']).toDF()
      accounts_df = glueContext.create_dynamic_frame.from_catalog(database=args['domain_db'], table_name=args['domain_accounts_table']).toDF()
      users_df = glueContext.create_dynamic_frame.from_catalog(database=args['domain_db'], table_name=args['domain_users_table']).toDF()

      load_noactivity_dashboard(config_calendar_df, config_activities_df, accountscontract_summary_df, activitydaily_df, accounts_df, users_df, args['s3_destination'])