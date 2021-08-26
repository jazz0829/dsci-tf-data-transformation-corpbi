import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from pyspark.sql.functions import when, lit, udf, col, concat, trim, sum, desc, to_timestamp, months_between, floor
from awsglue.utils import getResolvedOptions
from datetime import datetime
from pyspark.sql.window import Window
import pyspark.sql.functions as func
from pyspark.sql.types import IntegerType, StringType, BooleanType, DateType
import calendar
import math

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'start_month_index', 'end_month_index', 's3_destination'])

def add_months(d,x):
    newmonth = ((( d.month - 1) + x ) % 12 ) + 1
    newyear  = int(math.floor(d.year + ((( d.month - 1) + x ) / 12 )))
    days_in_month = calendar.monthrange(newyear, newmonth)[1]
    return datetime(newyear, newmonth, days_in_month)

# inputenddate = datetime(2018, 9, 30)
# inputstartdate = datetime(2018, 8, 31)
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
contracts_DF  = glueContext.create_dynamic_frame.from_catalog(database='customerintelligence', table_name='contracts').toDF()
accounts_df  = glueContext.create_dynamic_frame.from_catalog(database='customerintelligence', table_name='accounts').toDF()
contracts_DF = contracts_DF.withColumn( 'startdatets', contracts_DF['startdate'].cast(DateType()))
contracts_DF = contracts_DF.withColumn( 'finaldatets', contracts_DF['finaldate'].cast(DateType()))
contracts_DF = contracts_DF.withColumn( 'eventdatets', contracts_DF['eventdate'].cast(DateType()))
contracts_DF = contracts_DF.withColumn( 'year', contracts_DF['eventdate'].cast(DateType()))
contracts_DF = contracts_DF.withColumn( 'month', contracts_DF['eventdate'].cast(DateType()))
dateimenow = datetime.now()
start_date = add_months(dateimenow, -1)

start_range = int(args['start_month_index'])
end_range = int(args['end_month_index'])


for i in range(start_range, end_range, -1):
    inputenddate = add_months(start_date, i)
    inputstartdate = add_months(inputenddate, -1)
    domain_account_summary_weekly = contracts_DF.where(
        (contracts_DF['linepackage'] == 1.0) &
        (contracts_DF['startdatets'] <= inputenddate) &
        (contracts_DF['finaldatets'] >= inputstartdate) & (contracts_DF['eventdatets'] <= inputenddate))\
        .groupBy('environment', 'accountcode','itemcode','contracttype','contractnumber')\
        .agg({'quantity': 'sum'}).withColumnRenamed('sum(quantity)', 'quantity')

    domain_account_summary_weekly = domain_account_summary_weekly.alias('con')
    accounts_df = accounts_df.alias('acc')
    domain_account_summary_weekly = domain_account_summary_weekly.join(accounts_df,
                                                                       (domain_account_summary_weekly.accountcode == accounts_df.accountcode) &
                                                                       (domain_account_summary_weekly.environment == accounts_df.environment)).select('con.*','acc.accountid')
    domain_account_summary_weekly = domain_account_summary_weekly\
        .withColumn('row_num',
                    func.row_number().over(Window.partitionBy(domain_account_summary_weekly['environment'],domain_account_summary_weekly['accountcode']).orderBy(domain_account_summary_weekly['quantity'].desc(), domain_account_summary_weekly['contracttype'])))
    domain_account_summary_weekly = domain_account_summary_weekly.where(domain_account_summary_weekly['row_num'] == 1)
    domain_account_summary_weekly = domain_account_summary_weekly.withColumn('year', lit(inputenddate.year))
    domain_account_summary_weekly = domain_account_summary_weekly.withColumn('month', lit(inputenddate.month))
    domain_account_summary_weekly = domain_account_summary_weekly.withColumn('year', domain_account_summary_weekly['year'].cast(StringType()))
    domain_account_summary_weekly = domain_account_summary_weekly.withColumn('month', domain_account_summary_weekly['month'].cast(StringType()))

    domain_account_summary_weekly = domain_account_summary_weekly.withColumn('yearmonth', concat(lit(inputenddate.year),  lit(inputenddate.strftime('%m'))))
    domain_account_summary_weekly = domain_account_summary_weekly.withColumn('yearmonth', domain_account_summary_weekly['yearmonth'].cast(IntegerType()))


    totals_df = contracts_DF.where((contracts_DF['startdatets'] <= inputenddate) & (contracts_DF['finaldatets'] >= inputstartdate) & (contracts_DF['eventdatets'] <= inputenddate))
    totals_df = totals_df.withColumn('numberofavailableadmins', when((totals_df['itemcode'] != 'EOL9950') & (totals_df['eventtype'] <> 'CFC'), totals_df['numberofadministrations']).otherwise(lit(0)))
    totals_df = totals_df.withColumn('numberofarchivedadmins', when((totals_df['itemcode'] == 'EOL9950') & (totals_df['eventtype'] <> 'CFC'), totals_df['numberofadministrations']).otherwise(lit(0)))
    totals_df = totals_df.withColumn('numberofusers', when((totals_df['eventtype'] <> 'CFC'), totals_df['numberofusers']).otherwise(lit(0)))
    totals_df = totals_df.withColumn('numberofpayingusers', when((totals_df['valuepermonth'] <> 0), totals_df['numberofusers']).otherwise(lit(0)))
    totals_df = totals_df.withColumn('numberoffreeusers', when((totals_df['valuepermonth'] == 0), totals_df['numberofusers']).otherwise(lit(0)))
    totals_df = totals_df.withColumn('mrr',
                                     when(totals_df['eventtype'] <> 'CFC', totals_df['valuepermonth'])
                                     .otherwise(lit(0))).groupBy('environment', 'accountcode','contractnumber').agg({'mrr': 'sum', 'numberofavailableadmins' : 'sum', 'numberofarchivedadmins' : 'sum', 'numberofusers' : 'sum', 'numberofpayingusers' : 'sum', 'numberoffreeusers' : 'sum'})\
                                    .withColumnRenamed('sum(numberofarchivedadmins)', 'numberofarchivedadmins')\
                                    .withColumnRenamed('sum(numberofavailableadmins)', 'numberofavailableadmins')\
                                    .withColumnRenamed('sum(numberofpayingusers)', 'numberofpayingusers')\
                                    .withColumnRenamed('sum(numberoffreeusers)', 'numberoffreeusers')\
                                    .withColumnRenamed('sum(mrr)', 'mrr')\
                                    .withColumnRenamed('sum(numberofusers)', 'numberofusers')

    totals_df = totals_df.alias('totals')
    domain_account_summary_weekly = domain_account_summary_weekly.alias('das')
    domain_account_summary_weekly = domain_account_summary_weekly.join(totals_df,
                                                                       (domain_account_summary_weekly.accountcode == totals_df.accountcode) &
                                                                       (domain_account_summary_weekly.environment == totals_df.environment) &
                                                                       (domain_account_summary_weekly.contractnumber == totals_df.contractnumber) &
                                                                       (domain_account_summary_weekly.year == inputenddate.year) &
                                                                       (domain_account_summary_weekly.month == inputenddate.month), how='left')\
                                                                 .select('das.*','totals.numberofarchivedadmins','totals.numberofavailableadmins','totals.numberofpayingusers','totals.numberoffreeusers','totals.mrr','totals.numberofusers')


    lifetime_df = contracts_DF.where((contracts_DF['linepackage'] == 1.0) & (trim(contracts_DF['contracttype']) == 'C') & (contracts_DF['startdatets'] <= inputenddate) & (contracts_DF['inflowoutflow'] == 'Inflow')).groupBy('environment', 'accountcode').agg({'startdatets': 'min'}).withColumnRenamed('min(startdatets)', 'firstCommstartdate')
    lifetime_df = lifetime_df.withColumn('commerciallifetimemonths',floor(months_between(lit(inputenddate),lifetime_df['firstCommstartdate']))).drop('firstCommstartdate')
    lifetime_df = lifetime_df.alias('lifetime')
    domain_account_summary_weekly = domain_account_summary_weekly.drop('row_num').alias('das')
    domain_account_summary_weekly = domain_account_summary_weekly.join(lifetime_df,(domain_account_summary_weekly.environment == lifetime_df.environment) & (domain_account_summary_weekly.accountcode == lifetime_df.accountcode) & (domain_account_summary_weekly.year == inputenddate.year) & (domain_account_summary_weekly.month == inputenddate.month), how='left').select('das.*','lifetime.commerciallifetimemonths')


    churn_df = contracts_DF.where((contracts_DF['linepackage'] == 1.0) & (contracts_DF['startdatets'] <= inputenddate) & (contracts_DF['finaldatets'] >= inputstartdate))
    churn_df = churn_df.withColumn('churned', when((churn_df['finaldatets'] <= inputenddate) & ((churn_df['eventtype'] == 'CFC') | (churn_df['eventtype'] == 'TFC')) , lit(1)).otherwise(lit(0)))
    churn_df = churn_df.groupBy('environment', 'accountcode','contracttype','contractnumber').agg(
       {'churned': 'max'})
    churn_df = churn_df.withColumnRenamed('max(churned)', 'churned')
    churn_df = churn_df.alias('churn')
    domain_account_summary_weekly = domain_account_summary_weekly.alias('das')
    domain_account_summary_weekly = domain_account_summary_weekly.join(churn_df, (domain_account_summary_weekly.environment == churn_df.environment) & (domain_account_summary_weekly.accountcode == churn_df.accountcode) & (domain_account_summary_weekly.year == inputenddate.year) & (domain_account_summary_weekly.month == inputenddate.month) & (domain_account_summary_weekly.contractnumber == churn_df.contractnumber) & (domain_account_summary_weekly.contracttype == churn_df.contracttype), how='left').select('das.*','churn.churned')

    s3path= args['s3_destination'] +'/year='+ str(inputenddate.year) +'/month='+ str(inputenddate.month) + '/'
    domain_account_summary_weekly = domain_account_summary_weekly.drop("year", "month")
    domain_account_summary_weekly = domain_account_summary_weekly.withColumnRenamed('itemcode', 'packagecode')
    domain_account_summary_weekly.repartition(1).write.mode("overwrite").parquet(s3path)