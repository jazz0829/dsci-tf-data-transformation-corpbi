import sys
import pyspark.sql.functions as F
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, BooleanType, FloatType, DateType
from pyspark.sql.functions import when, lit, col, trim, lower, row_number, sum, max

args = getResolvedOptions(sys.argv, ['domain_db', 'contracts_table', 'linkedaccountantlog_table',
                                     'accountscontract_summary_table', 'accounts_table', 's3_destination'])

glueContext = GlueContext(SparkContext.getOrCreate())

contracts_import = glueContext.create_dynamic_frame.from_catalog(database=args['domain_db'],
                                                         table_name=args['contracts_table']).toDF()
accountant = glueContext.create_dynamic_frame.from_catalog(database=args['domain_db'],
                                                           table_name=args['linkedaccountantlog_table']).toDF()
accountcontract = glueContext.create_dynamic_frame.from_catalog(database=args['domain_db'],
                                                                table_name=args[
                                                                    'accountscontract_summary_table']).toDF()
acccounts = glueContext.create_dynamic_frame.from_catalog(database=args['domain_db'],
                                                          table_name=args['accounts_table']).toDF()

account_filter_condition = ((col('linkstatus') == 'Accountant Linked'))

accountant_log = accountant.filter(account_filter_condition)
accountant_log = accountant_log.withColumn("environment", trim(col("environment")))
accountant_log = accountant_log.select('environment', 'accountcode', 'linkstatus', 'date')
accountant_log = accountant_log.withColumn("row_num", row_number().over(
    Window.partitionBy('environment', 'accountcode', 'linkstatus').orderBy("date")))
logcond = ((col('row_num') == '1'))
accountant_log = accountant_log.filter(logcond)

account_contracts = accountcontract.select('environment', 'accountcode', 'firstcommstartdate', 'hadcommcontract',
                                           'datefirstlinkedtoaccountant')

acccounts = acccounts.select('accountcode', 'accountid', 'environment', 'entrepreneuraccountantlinked')

account_contracts = account_contracts.alias('base')
acccounts = acccounts.alias('second')
acccontjoin = account_contracts.join(acccounts, (account_contracts.accountcode == acccounts.accountcode)
                                     & (account_contracts.environment == acccounts.environment))\
                                        .select('base.*', 'second.entrepreneuraccountantlinked', 'second.accountid')

acccontjoin = acccontjoin.alias('base1')
accountant_log = accountant_log.alias('third')

base_frame_accounts = acccontjoin.join(accountant_log, (acccontjoin.accountcode == accountant_log.accountcode) & (
            acccontjoin.environment == accountant_log.environment), "left").select('base1.*', 'third.linkstatus',
                                                                                  'third.date')
base_frame_accounts = base_frame_accounts.withColumn("accountantlinked",
                                     when(col("entrepreneuraccountantlinked") == 'EntrepreneurWithAccountant',
                                          1).otherwise(0))
base_frame_accounts = base_frame_accounts.withColumn("datefirstlinkedtoaccountantz", when(
    (col("entrepreneuraccountantlinked") == 'EntrepreneurWithAccountant') & (
                col("firstcommstartdate") >= '2016-02-09') & (col("linkstatus") == 'Accountant Linked'), col("date"))
                                     .when((col("entrepreneuraccountantlinked") == 'EntrepreneurWithAccountant') & (
            col("firstcommstartdate") >= '2016-02-09') & (col("linkstatus").isNull()), col("firstcommstartdate"))
                                     .when((col("entrepreneuraccountantlinked") == 'EntrepreneurWithoutAccountant') & (
            col("firstcommstartdate") >= '2016-02-09') & (col("linkstatus") == 'Accountant Linked'), col("date"))
                                     .when(
    (col("firstcommstartdate") <= '2016-02-09') & (col("linkstatus") == 'Accountant Linked'), col("date")))


# setup the contracts table
cond1 = (col('inflowoutflow') == 'Inflow')
cond2 = (col('inflowoutflow') == 'Outflow') & (col('eventtype') == 'PRC')
contracts = contracts_import.filter(cond1 | cond2)

cond3 = (col('linepackage') == when(col("eventtype") == 'TN', 1)
         .when(col("eventtype") == 'TN', 1)
         .when(col("eventtype") == 'CN', 1)
         .when(col("eventtype") == 'CT', 1)
         .when(col("eventtype") == 'CDN', 1)
         .when(col("eventtype") == 'CUP', 1)
         .otherwise(0))

contracts = contracts.filter(cond3)

contracts = contracts.select('environment', 'accountcode', 'eventdate', 'eventtype', 'packagecode',
                                 'linepackage')

#create a columative sum of all features per eventdate
sum_features = contracts_import.select('environment', 'accountcode', 'eventdate', 'numberofusers', 'numberofadministrations',
                          'valuepermonth', 'packagecode')

sum_features = sum_features.withColumn("numberofusers", sum_features["numberofusers"].cast(IntegerType()))\
    .withColumn("numberofadministrations", sum_features["numberofadministrations"].cast(IntegerType()))\
    .withColumn("valuepermonth", sum_features["valuepermonth"].cast(IntegerType()))

sum_users = sum_features.groupBy('environment', 'accountcode', 'eventdate').agg(F.sum('numberofusers'))
sum_admins = sum_features.groupBy('environment', 'accountcode', 'eventdate').agg(F.sum('numberofadministrations'))
sum_mrr = sum_features.groupBy('environment', 'accountcode', 'eventdate').agg(F.sum('valuepermonth'))

sum_users = sum_users.withColumnRenamed("sum(numberofusers)", "numberofusers")
sum_admins = sum_admins.withColumnRenamed("sum(numberofadministrations)", "numberofadministrations")
sum_mrr = sum_mrr.withColumnRenamed("sum(valuepermonth)", "valuepermonth")

windowfunction = (Window.partitionBy('environment', 'accountcode').orderBy('eventdate'))
cumsum_users = sum_users.withColumn('numberofusers', F.sum('numberofusers').over(windowfunction))
cumsum_mrr = sum_mrr.withColumn('valuepermonth', F.sum('valuepermonth').over(windowfunction))
cumsum_admins = sum_admins.withColumn('numberofadministrations', F.sum('numberofadministrations').over(windowfunction))

contracts = contracts.alias('main')

cumsum_users = cumsum_users.alias('user')
cumsum_mrr = cumsum_mrr.alias('value')
cumsum_admins = cumsum_admins.alias('admin')

contractuser = contracts.join(cumsum_users, (contracts.accountcode == cumsum_users.accountcode) & (
            contracts.environment == cumsum_users.environment) & (contracts.eventdate == cumsum_users.eventdate),
                             "left_outer").select('main.*', 'user.numberofusers')

contractuser = contractuser.alias('main')

contractadmin = contractuser.join(cumsum_mrr, (contractuser.accountcode == cumsum_mrr.accountcode) & (
            contractuser.environment == cumsum_mrr.environment) & (contractuser.eventdate == cumsum_mrr.eventdate),
                            "left_outer").select('main.*', 'value.valuepermonth')

contractadmin = contractadmin.alias('main')

contractsmrr = contractadmin.join(cumsum_admins, (contractadmin.accountcode == cumsum_admins.accountcode) & (
            contractadmin.environment == cumsum_admins.environment) & (contractadmin.eventdate == cumsum_admins.eventdate),
                             "left_outer").select('main.*', 'admin.numberofadministrations')

contracts_sum = contractsmrr.dropDuplicates()

# Join maintable on contract tables

contracts_sum = contracts_sum.alias('mid')
base_frame_accounts = base_frame_accounts.alias('cont')
output_dataset = contracts_sum .join(base_frame_accounts, (contracts_sum .accountcode == base_frame_accounts.accountcode) & (
            contracts_sum .environment == base_frame_accounts.environment), "left_outer").select('mid.*',
                                                                                     'cont.datefirstlinkedtoaccountant',
                                                                                     'cont.datefirstlinkedtoaccountantz',
                                                                                     'cont.AccountantLinked',
                                                                                     'cont.accountid',
                                                                                     'cont.hadcommcontract')

# shaping the accountantlinked status per line
output_dataset = output_dataset.withColumn("accountantlinked", when(
    (col("AccountantLinked") == '1') & (col("datefirstlinkedtoaccountantz").isNull()), '1')
                                   .when(
    (col("AccountantLinked") == '1') & (col("datefirstlinkedtoaccountantz") <= col("eventdate")), "1")
                                   .when(
    (col("AccountantLinked") == '0') & (col("datefirstlinkedtoaccountantz").isNull()), "0")
                                   .when(
    (col("AccountantLinked") == '0') & (col("datefirstlinkedtoaccountantz") <= col("eventdate")), "1")
                                   .when(
    (col("AccountantLinked") == '1') & (col("datefirstlinkedtoaccountantz") >= col("eventdate")), "0")
                                   .when(
    (col("AccountantLinked") == '0') & (col("datefirstlinkedtoaccountantz") >= col("eventdate")), "0"))

# Creating a new line called Linked using the output dataframe
newline_linked = output_dataset.where(
    (col('eventdate') <= col('datefirstlinkedtoaccountant')) & (col("datefirstlinkedtoaccountant").isNotNull()) & (
                col("hadcommcontract") == '1'))

# Dataframe making sure that the newline_linked also is created
# for customers that link before their first commercial event
newline_non_commercial_link = base_frame_accounts.where((col("datefirstlinkedtoaccountant").isNotNull()) & (col("hadcommcontract") == '1'))
newline_non_commercial_link = newline_non_commercial_link.alias('B')
newline_linked = newline_linked.alias('new0')
newline_linked = newline_non_commercial_link.join(newline_linked,
                                                              (newline_linked.accountcode == newline_non_commercial_link.accountcode) &
                                                              (newline_linked.environment == newline_non_commercial_link.environment),
                                                                "leftouter").select('B.*', 'new0.eventdate', 'new0.packagecode',
                                                                'new0.numberofadministrations', 'new0.numberofusers',
                                                                'new0.valuepermonth', 'new0.eventtype')

newline_linked = newline_linked.withColumn("eventdate", when(
    (col("datefirstlinkedtoaccountant").isNotNull()) & (col("numberofadministrations").isNull()) &
    (col("numberofusers").isNull()) & (col("valuepermonth").isNull()),
    (col("datefirstlinkedtoaccountant"))))\
    .withColumn("numberofadministrations", when(
        (col("datefirstlinkedtoaccountant").isNotNull()) &
        (col("numberofadministrations").isNull()), "0").otherwise(
        col("numberofadministrations")))\
    .withColumn("numberofusers",when((col("datefirstlinkedtoaccountant").isNotNull()) &
                (col("numberofusers").isNull()), '0').otherwise(col("numberofusers")))\
    .withColumn("valuepermonth", when((col("datefirstlinkedtoaccountant").isNotNull()) &
                (col("valuepermonth").isNull()), '0').otherwise(col("valuepermonth")))

newline_linked = newline_linked.select('datefirstlinkedtoaccountantz', 'datefirstlinkedtoaccountant', 'eventdate', 'environment',
                          'accountcode', 'packagecode', 'eventtype', 'accountantlinked', 'numberofusers',
                          'numberofadministrations', 'valuepermonth', 'accountid')
newline_linked = newline_linked.filter(col('valuepermonth').isNotNull())

newline_linked = newline_linked.withColumn('eventtype', lit("LINK"))
newline_linked = newline_linked.sort('eventdate', ascending=False)
newline_linked = newline_linked.withColumn("row_num", row_number().over(
    Window.partitionBy('environment', 'accountcode').orderBy(col('eventdate'), col('valuepermonth'),
                                                             col('numberofadministrations'), col('numberofusers'),
                                                             col('eventtype'), col('packagecode').desc())))
newline_linked = newline_linked.filter(col("row_num") == '1')
newline_linked = newline_linked.groupBy('environment', 'accountcode', 'eventdate', 'packagecode', 'eventtype',
                          'accountid').agg(F.max('numberofusers'), F.max('numberofadministrations'), F.max('valuepermonth'),
                                           F.max('datefirstlinkedtoaccountantz'), F.max('datefirstlinkedtoaccountant'))

newline_linked = newline_linked.withColumnRenamed("max(numberofusers)", "numberofusers")\
    .withColumnRenamed("max(numberofadministrations)", "numberofadministrations")\
    .withColumnRenamed("max(valuepermonth)", "valuepermonth")\
    .withColumnRenamed("max(datefirstlinkedtoaccountantz)", "datefirstlinkedtoaccountantz")\
    .withColumnRenamed("max(datefirstlinkedtoaccountant)", "datefirstlinkedtoaccountant")

Overwrite_on_accountant_linked_date = \
    output_dataset.select('environment', 'accountcode', 'eventdate', 'packagecode',
                          'numberofadministrations','numberofusers', 'valuepermonth')
Overwrite_on_accountant_linked_date = Overwrite_on_accountant_linked_date.groupBy('environment', 'accountcode', 'eventdate').agg(F.max('numberofusers'), F.max('packagecode'),
                                                                                    F.max('numberofadministrations'), F.max('valuepermonth'))

Overwrite_on_accountant_linked_date = Overwrite_on_accountant_linked_date.withColumnRenamed("max(packagecode)", "packagecodenew")\
.withColumnRenamed("max(numberofadministrations)", "numberofadministrationsnew")\
.withColumnRenamed("max(numberofusers)", "numberofusersnew")\
.withColumnRenamed("max(valuepermonth)", "valuepermonthnew")\
.withColumnRenamed("eventdate", "eventdatetemp")

Overwrite_on_accountant_linked_date = Overwrite_on_accountant_linked_date.alias('tem')
newline_linked = newline_linked.alias('newl')
newline_linked = newline_linked.join(Overwrite_on_accountant_linked_date,\
    (Overwrite_on_accountant_linked_date.accountcode == newline_linked.accountcode) &
    (Overwrite_on_accountant_linked_date.environment == newline_linked.environment) &
    (Overwrite_on_accountant_linked_date.eventdatetemp == newline_linked.datefirstlinkedtoaccountantz), "left")\
    .select('newl.*', 'tem.packagecodenew', 'tem.numberofadministrationsnew',
            'tem.numberofusersnew', 'tem.valuepermonthnew','tem.eventdatetemp')

newline_linked = newline_linked.withColumn("delete", when(
    (col('packagecodenew').isNotNull()) & (col('datefirstlinkedtoaccountantz') == col('eventdatetemp')), 1)
                               .when(col('packagecodenew').isNull(), 1).otherwise(0))
newline_linked = newline_linked.filter(col('delete') == '1')

update1 = when((col("packagecodenew").isNotNull()) & (col('eventtype') == 'LINK'),
               col('packagecodenew')).otherwise(col('packagecode'))
update2 = when((col("numberofadministrationsnew").isNotNull()) & (col('eventtype') == 'LINK'),
               col('numberofadministrationsnew')).otherwise(col('numberofadministrations'))
update3 = when((col("packagecodenew").isNotNull()) & (col('eventtype') == 'LINK'),
               col('numberofusersnew')).otherwise(col('numberofusers'))
update4 = when((col("packagecodenew").isNotNull()) & (col('eventtype') == 'LINK'),
               col('valuepermonthnew')).otherwise(col('valuepermonth'))

newline_linked = newline_linked.withColumn("packagecode", update1)\
    .withColumn("numberofadministrations", update2)\
    .withColumn("numberofusers", update3)\
    .withColumn("valuepermonth", update4)

# newlines = newlines.filter((col('hadcommcontract')=='1') & (col("datefirstlinkedtoaccountant").isNotNull()))

newline_linked = newline_linked.withColumn("accountantlinked", lit(1))
newline_linked = newline_linked.select('datefirstlinkedtoaccountantz', 'environment', 'accountcode', 'packagecode',
                          'eventtype', 'accountantlinked', 'numberofusers', 'numberofadministrations',
                          'valuepermonth', 'accountid')

output_dataset = output_dataset.select('eventdate', 'environment', 'accountcode', 'packagecode', 'eventtype',
                               'accountantlinked', 'numberofusers', 'numberofadministrations', 'valuepermonth',
                               'accountid', 'linepackage')

output_dataset = output_dataset.drop(output_dataset.linepackage)

# Add the New linked line to the output output dataset

output_dataset = output_dataset.union(newline_linked)
output_dataset = output_dataset.withColumn("accountantlinked",
                                       when(col("eventtype") == 'TN', 0).otherwise(col('accountantlinked')))

# Overwite the package where wrong
overwrite_package = contracts_import.select('environment', 'accountcode', 'eventdate', 'inflowoutflow', 'linepackage',
                                    'itemcode')
overwrite_package = overwrite_package.filter((col('inflowoutflow') == 'Inflow') & (col("linepackage") == '1'))
overwrite_package = overwrite_package.withColumn("row_num", row_number().over(
    Window.partitionBy('environment', 'accountcode', 'eventdate').orderBy(col('eventdate').desc())))
overwrite_package = overwrite_package.filter(col("row_num") == '1')
overwrite_package = overwrite_package.withColumnRenamed("eventdate", "joindate")

output_dataset = output_dataset.alias('end')
overwrite_package = overwrite_package.alias('over')
output_dataset = output_dataset.join(overwrite_package, (output_dataset.accountcode == overwrite_package.accountcode) & (
            output_dataset.environment == overwrite_package.environment) & (overwrite_package.joindate < output_dataset.eventdate),
                         "left_outer").select('end.*', 'over.itemcode', 'over.joindate')

output_dataset = output_dataset.withColumn("packagecode",
                               when((col("packagecode").isNull()), col('itemcode')).otherwise(col("packagecode")))

output_dataset = output_dataset.drop(output_dataset.joindate)\
    .drop(output_dataset.itemcode)\
    .withColumn('packagecode', lower(col('packagecode')))\
    .withColumnRenamed("eventdate", "date")\
    .withColumnRenamed("accountantlinked", "accountantorlinked")\
    .withColumnRenamed("valuepermonth", "mrr")

output_dataset = output_dataset.withColumn("row_num", row_number().over(
    Window.partitionBy('environment', 'accountcode', 'date', 'eventtype')
    .orderBy(col('date'), col('mrr'), col('numberofadministrations'), col('numberofusers'),
             col('eventtype'), col('packagecode').desc())))

output_dataset = output_dataset.filter(col("row_num") == '1')\
    .dropDuplicates()\
    .drop(output_dataset.row_num)

output_dataset = output_dataset.withColumn('date', col('date').cast(DateType()))\
    .withColumn('accountantorlinked', col('accountantorlinked').cast(BooleanType()))\
    .withColumn('numberofusers', col('numberofusers').cast(IntegerType()))\
    .withColumn('numberofadministrations', col('numberofadministrations').cast(IntegerType()))\
    .withColumn('mrr', col('mrr').cast(FloatType()))

output_dataset.repartition(1).write.mode("overwrite").parquet(args['s3_destination'])
