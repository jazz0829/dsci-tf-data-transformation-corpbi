import sys
from utils import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from datetime import datetime, date
from pyspark.sql.window import Window
from pyspark.sql.functions import col, upper, trim, when, row_number

args = getResolvedOptions(sys.argv, ['raw_db', 'domain_db', 'domain_users_table', 'raw_contractstatistics_table', 'raw_appbillingproposal_table',
                                     'raw_monthlyappusage_table', 'sectors_s3_destination', 'subsectors_s3_destination',
                                     'resellers_s3_destination', 'accountants_s3_destination', 'raw_opportunities_table',
                                     'contracteventtypes_s3_destination', 'opportunities_s3_destination',
                                     'itemclassifications_s3_destination', 'appbillingproposal_s3_destination',
                                     'monthlyappusage_s3_destination','contracts_s3_destination', 'accountmanager_s3_destination'])

dateimenow = datetime.now()
year = dateimenow.year
month = dateimenow.month
day = dateimenow.day

filter = 'ingestionyear = ' + str(year) + ' and ingestionmonth = ' + str(month) + ' and ingestionday =' + str(day)

glueContext = GlueContext(SparkContext.getOrCreate())


def load_contracts(df):
    cancellationdate_compare = date(9999, 12, 31)
    selectedcolumns = ['environment', 'code', 'contractnumber', 'contracttype', 'startdate', 'enddate', 'finaldate',
                       'eventdate', 'inflowoutflow', 'itemcode', 'packagecode', 'linepackage', 'quantity',
                       'numberofadministrations', 'numberofusers', 'contractlinevalue', 'valuepermonth', 'valueperyear',
                       'contractlinevaluedc', 'valuepermonthdc', 'valueperyeardc', 'exchangerate', 'cancellationdate',
                       'itembasepricepermonth', 'itembasepriceperyear', 'name', 'eventyearmonth', 'eventyear',
                       'eventmonth', 'eventtype', 'eventdescription', 'item', 'itemdescription', 'itemtype']

    contracts_df = df.where((col('meta_load_enddts').isNull()) & (col('meta_isdeleted') == False))
    contracts_df = contracts_df.select(selectedcolumns)
    contracts_df = convertDataTypes(
        data_frame = contracts_df,
        date_cols = ['startdate', 'enddate', 'finaldate', 'eventdate', 'cancellationdate'],
        double_cols = ['contractlinevalue', 'valuepermonth', 'valueperyear', 'contractlinevaluedc', 'valuepermonthdc',
                       'valueperyeardc', 'exchangerate', 'itembasepricepermonth', 'itembasepriceperyear'],
        integer_cols = ['quantity', 'numberofadministrations', 'numberofusers', 'eventyearmonth', 'eventyear', 'eventmonth',
                        'linepackage']
    )
    contracts_df = contracts_df.withColumnRenamed("code", "accountcode")
    contracts_df = contracts_df.withColumn('contracteventtypecode', col('eventtype'))
    contracts_df = contracts_df.withColumn('cancellationdate', when(col('cancellationdate') != cancellationdate_compare,col('cancellationdate')))

    contracts_df.repartition(1).write.mode("overwrite").parquet(args['contracts_s3_destination'])

def load_subsectors(df):
    subsectors_df = df.where(
        (col('subsectorcode').isNotNull()) & (col('meta_isdeleted') == False) & (col('meta_load_enddts').isNull())) \
        .select('environment', 'subsectorcode', 'subsectordescription').distinct()
    subsectors_df.repartition(1).write.mode("overwrite").parquet(args['subsectors_s3_destination'])


def load_sectors(df):
    sectors_df = df.where(
        (df['sectorcode'].isNotNull()) & (col('meta_isdeleted') == False) & (col('meta_load_enddts').isNull())) \
        .select('environment', 'sectorcode', 'sectordescription').distinct()

    sectors_df.repartition(1).write.mode("overwrite").parquet(args['sectors_s3_destination'])


def load_resellers(df):
    resellers_df = df.where(
        (df['resellercode'].isNotNull()) & (col('meta_isdeleted') == False) & (col('meta_load_enddts').isNull())) \
        .select('environment', 'resellercode', 'resellername').distinct()

    resellers_df.repartition(1).write.mode("overwrite").parquet(args['resellers_s3_destination'])


def load_accountants(df):
    accountants_df = df.where(
        (df['accountantcode'].isNotNull()) & (col('meta_isdeleted') == False) & (col('meta_load_enddts').isNull())) \
        .select('environment', 'accountantcode', 'accountantname').distinct()

    accountants_df.repartition(1).write.mode("overwrite").parquet(args['accountants_s3_destination'])


def load_contracteventtypes(df):
    contracteventtypes_df = df.where((col('meta_isdeleted') == 0) & (col('meta_load_enddts').isNull())) \
        .select('environment', 'eventtype', 'eventdescription').distinct()
    contracteventtypes_df = contracteventtypes_df.withColumnRenamed('eventtype', 'contracteventtypecode') \
        .withColumnRenamed('eventdescription', 'contracteventdescription')

    contracteventtypes_df.repartition(1).write.mode("overwrite").parquet(args['contracteventtypes_s3_destination'])


def load_itemclassifications(df):
    df_1 = df.where((col('meta_isdeleted') == False) & (col('meta_load_enddts').isNull())) \
        .select('environment', 'itemcode', 'itemdescription').distinct()
    df_2 = df.where(
        (df['itemtype'].isNotNull()) & (col('meta_isdeleted') == False) & (col('meta_load_enddts').isNull())) \
        .select('itemcode', 'itemtype').groupBy('itemcode', 'itemtype').agg({'itemcode': 'count'})
    df_2 = df_2.withColumnRenamed('itemcode', 'code').withColumnRenamed('count(itemcode)', 'cn')

    w = Window.partitionBy(df_2['code']).orderBy(df_2['cn'])
    df_2 = df_2.withColumn("RowNum", row_number().over(w))
    df_3 = df_1.join(df_2, (df_1.itemcode == df_2.code) & (df_2.RowNum == 1), how='left') \
        .drop('code', 'RowNum', 'cn')

    df_3.repartition(1).write.mode("overwrite").parquet(args['itemclassifications_s3_destination'])


def load_app_billing_proposal():
    app_billing_df = glueContext.create_dynamic_frame.from_catalog(database=args['raw_db'],
                                                          table_name=args['raw_appbillingproposal_table'],
                                                          push_down_predicate=filter).toDF()

    if app_billing_df.count() > 0:
        upper_cols = ['item','environment']
        app_billing_df = cleanDataFrame(app_billing_df, to_upper_cols = upper_cols)
        app_billing_df = convertDataTypes(
        data_frame = app_billing_df,
        timestamp_cols = ['meta_load_enddts'],
        boolean_cols = ['meta_isdeleted']
        )
        app_billing_df = app_billing_df.withColumn('environment', when(col('environment') == 'GB', 'UK').otherwise(col('environment')))
        app_billing_df = app_billing_df.where((col('meta_load_enddts').isNull()) & (col('meta_isdeleted') == False))
        app_billing_df = app_billing_df.drop('ingestionyear', 'ingestionmonth', 'ingestionday')

        app_billing_df.repartition(1).write.mode("overwrite").parquet(args['appbillingproposal_s3_destination'])


def load_opportunities():
    op_df = glueContext.create_dynamic_frame.from_catalog(database=args['raw_db'],
                                                          table_name=args['raw_opportunities_table'],
                                                          push_down_predicate=filter).toDF()

    if op_df.count() > 0:
        upper_cols = ['country']
        op_df = cleanDataFrame(op_df, to_upper_cols = upper_cols)
        op_df = renameColumns(op_df, {
            'country': 'environment',
            'salesdescription': 'salestypedescription',
            'reasondescription': 'reasoncodedescription'
        })
        op_df = convertDataTypes(
            data_frame = op_df,
            timestamp_cols = ['meta_load_enddts'],
            boolean_cols = ['meta_isdeleted']
        )

        op_df = op_df.withColumn('environment', when(col('environment') == 'GB', 'UK').otherwise(col('environment')))
        op_df = op_df.where((col('meta_load_enddts').isNull()) & (col('meta_isdeleted') == False))
        op_df = op_df.select('id', 'environment', 'accountname', 'accountcode', 'actiondate', 'amountfc', 'amountdc',
                             'closedate', 'created', 'creator', 'modified', 'currency', 'name', 'opportunitystage',
                             'opportunitystagedescription', 'opportunitystatus', 'probability', 'owner', 'ratefc',
                             'ownerfullname', 'leadsource', 'leadsourcedescription', 'salestypedescription',
                             'reasoncode', 'reasoncodedescription', 'notes', 'project', 'projectcode', 'salestype',
                             'projectdescription', 'campaign', 'opportunitytype', 'opportunitytypedescription',
                             'channel', 'campaigndescription', 'channeldescription')

        op_df.repartition(1).write.mode("overwrite").parquet(args['opportunities_s3_destination'])


def load_monthlyappusage():
    appusage_df = glueContext.create_dynamic_frame.from_catalog(database=args['raw_db'],
                                                                table_name=args['raw_monthlyappusage_table'],
                                                                push_down_predicate=filter).toDF()
    if appusage_df.count() > 0:
        upper_cols = ['app','partner','usedbycustomer','id','entryid','environment']
        appusage_df = cleanDataFrame(appusage_df, to_upper_cols = upper_cols)
        appusage_df = convertDataTypes(
            data_frame = appusage_df,
            timestamp_cols = ['meta_load_enddts'],
            date_cols = ['firstappusagedate'],
            boolean_cols = ['isfreeconnection','meta_isdeleted']
        )
        appusage_df = appusage_df.withColumn('environment',
                                             when(col('environment') == 'GB', 'UK').otherwise(col('environment')))

        appusage_df = appusage_df.where((col('meta_load_enddts').isNull()) & (col('meta_isdeleted') == False))
        appusage_df = appusage_df.drop('meta_hash', 'meta_isdeleted', 'meta_lastupdateprocessid', 'meta_load_enddts',
                                       'IngestionDay', 'meta_load_startdts', 'meta_processid', 'meta_source',
                                       'IngestionYear', 'IngestionMonth')

        appusage_df.repartition(1).write.mode("overwrite").parquet(args['monthlyappusage_s3_destination'])

def load_accountmanager(contractstatistics_raw_df):
    users_df = glueContext.create_dynamic_frame.from_catalog(database = args['domain_db'], table_name=args['domain_users_table']).toDF()
    users_df = cleanDataFrame(users_df, ['environment'])

    contractstatistics_raw_df = convertDataTypes(
            data_frame = contractstatistics_raw_df,
            timestamp_cols = ['eventdate'],
            integer_cols = ['accountmanagercode'],
            boolean_cols = ['meta_isdeleted']
    )

    window_spec = Window.partitionBy(col('accountmanagercode')).orderBy(col('eventdate').desc())
    accountmanager_df = contractstatistics_raw_df.withColumn("row_num", row_number().over(window_spec))\
                        .where(col('meta_load_enddts').isNull() & (col('meta_isdeleted') == False) & (col('row_num') == 1) & col('accountmanagercode').isNotNull())\
                        .select('accountmanagercode', 'accountmanagername', 'businesslinecode', 'businesslinedescription', 'environment')\
                        .alias("cs")\
                        .join(users_df.alias("u"), 
                                (col("cs.environment") == col("u.environment")) & 
                                (col("cs.accountmanagername") == col("u.fullname")) & 
                                (col("cs.accountmanagercode") == col("u.hid")),
                            "left")\
                        .select(col("u.userid").alias("accountmanagerid"), "cs.*")

    accountmanager_df.repartition(1).write.mode("overwrite").parquet(args['accountmanager_s3_destination'])


def load_contract_statistics():
    contract_statistics_df = glueContext.create_dynamic_frame.from_catalog(database=args['raw_db'],
                                                           table_name=args['raw_contractstatistics_table'],
                                                         push_down_predicate=filter).toDF()

    upper_cols = ['account','environment']
    contract_statistics_df = cleanDataFrame(contract_statistics_df, to_upper_cols = upper_cols)
    contract_statistics_df = convertDataTypes(
        data_frame = contract_statistics_df,
        boolean_cols = ['meta_isdeleted']
    )
    contract_statistics_df.cache()

    if contract_statistics_df.count() > 0:
        contract_statistics_df = contract_statistics_df.\
            withColumn('environment', when(col('environment') == 'GB', 'UK').otherwise(col('environment')))

        load_contracts(contract_statistics_df)

        load_subsectors(contract_statistics_df)

        load_sectors(contract_statistics_df)

        load_resellers(contract_statistics_df)

        load_accountants(contract_statistics_df)

        load_contracteventtypes(contract_statistics_df)

        load_itemclassifications(contract_statistics_df)

        load_accountmanager(contract_statistics_df)


load_opportunities()
load_app_billing_proposal()
load_contract_statistics()
load_monthlyappusage()
