from utils import *

import sys
from pyspark.sql.functions import when, lit, count, trim, current_date, row_number, col, sum, max, \
    months_between, round
from pyspark.sql.window import Window
from pyspark.sql.types import DateType, IntegerType

# CONSTS
CONTRACT_TYPE_TRIAL = 'T'
CONTRACT_TYPE_COMMERCIAL = 'C'
CONTRACT_TYPE_PILOT = 'P'

EVENT_TYPE_NEW_TRIAL = 'TN'
EVENT_TYPE_PRICE_INCREASE = 'PI'

CONTRACT_CHANGE_TYPE_INFLOW = "Inflow"

# 1. FILL THE TABLE WITH ALL ACCOUNTS
def get_account_contracts(contracts_df, accounts_df):
    result_df = contracts_df\
        .join(accounts_df,
        (contracts_df.environment == accounts_df.environment) & 
        (contracts_df.accountcode == accounts_df.accountcode))\
            .groupby(
                contracts_df.environment,
                contracts_df.accountcode,
                accounts_df.accountid).count()
    result_df = result_df.drop('count')
    return result_df

# 2. SET DATES AND PACKAGE OF FIRST TRIAL
# Note: a customer may have multiple trial packages
def add_first_trail_package_data(contracts_df, result_df):
    window_spec = Window.partitionBy(contracts_df['accountcode'],contracts_df['environment'])\
        .orderBy(contracts_df['eventdate'])

    trialpackage_df = contracts_df.where(
            (col('contracttype') == CONTRACT_TYPE_TRIAL)
            & (col('linepackage') == 1)
            & ((col('eventtype') == EVENT_TYPE_NEW_TRIAL) | (col('eventtype') == EVENT_TYPE_PRICE_INCREASE))
            & (col('inflowoutflow') == CONTRACT_CHANGE_TYPE_INFLOW)
            & (col('eventdate') <= current_date()))\
        .withColumn("row_num", row_number().over(window_spec))
    trialpackage_df = trialpackage_df.filter(col("row_num") == 1)

    package_joined = contracts_df\
        .where(
            (col('contracttype') == CONTRACT_TYPE_TRIAL)
            & (col('linepackage') == 1)
            & ((col('eventtype') == EVENT_TYPE_NEW_TRIAL) | (col('eventtype') == EVENT_TYPE_PRICE_INCREASE))
            )\
        .groupBy("environment", "accountcode")\
        .agg({'startdate': 'min', 'finaldate': 'min'})\
        .join(trialpackage_df, ["accountcode", "environment"], 'left')

    package_joined = package_joined\
        .withColumnRenamed('min(startdate)', 'firsttrialstartdate')\
        .withColumnRenamed('min(finaldate)', 'firsttrialfinaldate')\
        .withColumnRenamed('itemcode', 'firsttrialpackage')
        
    package_joined = package_joined.select(
        "environment", "accountcode", "firsttrialstartdate", "firsttrialfinaldate", "firsttrialpackage")

    result_df = result_df.join(package_joined, ['accountcode', 'environment'], 'left').select('*')
    return result_df

# 3. SET DATES AND PACKAGE OF FIRST COMMERCIAL PACKAGE
def add_first_commercial_package_data(contracts_df, result_df):
    window_spec = Window.partitionBy(
        contracts_df['environment'], contracts_df['accountcode'])\
        .orderBy(contracts_df['eventdate'])

    firstcomm_package_df = contracts_df.where(
            (col('contracttype') == CONTRACT_TYPE_COMMERCIAL) & 
            (col('linepackage') == 1) & 
            (col('inflowoutflow') == CONTRACT_CHANGE_TYPE_INFLOW) &
            (col('eventdate') <= current_date()))\
        .withColumn("row_num", row_number().over(window_spec))
    firstcomm_package_df = firstcomm_package_df.filter(col("row_num") == 1)

    first_comm_package_joined = contracts_df.where(
            (col('contracttype') == CONTRACT_TYPE_COMMERCIAL) & 
            (col('linepackage') == 1))\
        .groupBy("environment", "accountcode")\
        .agg({'startdate': 'min', 'finaldate': 'min'})\
        .join(firstcomm_package_df, ["environment", "accountcode"], 'left')

    first_comm_package_joined = first_comm_package_joined\
        .withColumnRenamed('min(startdate)', 'firstcommstartdate')\
        .withColumnRenamed('min(finaldate)', 'firstcommfinaldate')\
        .withColumnRenamed('itemcode', 'firstcommpackage')

    first_comm_package_joined = first_comm_package_joined\
        .select("environment", "accountcode", "firstcommstartdate", "firstcommfinaldate", "firstcommpackage")
    result_df = result_df.join(first_comm_package_joined, ['environment', 'accountcode'], 'left').select('*')
    return result_df
  
# 4.SETS INDICATOR WHETHER ACCOUNT HAS HAD A TRIAL OR HAD A COMMERCIAL CONTRACT
def add_had_trail_or_commercial_indicators(result_df):
    result_df = result_df.withColumn('hadtrial',
                            when(col('firsttrialpackage').isNotNull(), lit(1)).otherwise(lit(0)))
    result_df = result_df.withColumn('hadcommcontract',
                            when(col('firstcommpackage').isNotNull(), lit(1)).otherwise(lit(0)))
    return result_df

# 5. SETS LATEST COMMERCIAL PACKAGE
def add_last_commercial_package(contracts_df, result_df):
    last_comm_package_df = contracts_df\
        .groupBy('environment', 'accountcode', 'itemcode', 'finaldate', 'contracttype', 'linepackage', 'eventdate')\
        .agg(sum('quantity'))

    last_comm_package_df = last_comm_package_df.where(
        (col('contracttype') == CONTRACT_TYPE_COMMERCIAL) &
        (col('linepackage') == 1) &
        (col('eventdate') <= current_date()))

    window_spec = Window.partitionBy("environment", "accountcode")\
            .orderBy(col("finaldate").desc(), col("sum(quantity)").desc())
    last_comm_package_df = last_comm_package_df.withColumn("row_number", row_number().over(window_spec))
    last_comm_package_df = last_comm_package_df\
        .filter(col("row_number") == 1)\
        .drop('row_number', 'sum(quantity)', 'finaldate', 'contracttype', 'linepackage', 'eventdate')

    last_comm_package_df = last_comm_package_df.withColumnRenamed('itemcode', 'latestcommpackage')

    commpackage_left_df = contracts_df.where(
            (col('contracttype') == CONTRACT_TYPE_COMMERCIAL) &
            (col('linepackage') == 1))\
        .groupBy('environment', 'accountcode').count()
    commpackage_left_df = commpackage_left_df.drop('count')

    package_joined = commpackage_left_df.join(last_comm_package_df, ['environment', 'accountcode'], 'left').select('*')

    package_joined = package_joined.withColumnRenamed('itemcode', 'latestcommpackage')
    package_joined = package_joined.drop('count')

    result_df = result_df.join(package_joined, ['environment', 'accountcode'], 'left').select('*')
    return result_df

# 6. SETS LATEST COMMERCIAL START AND FINAL DATES
def add_last_commercial_package_dates(contracts_df, result_df):
    latestcomm_df = contracts_df.where(
            (col('contracttype') == CONTRACT_TYPE_COMMERCIAL) &
            (col('linepackage') == 1) &
            (col('inflowoutflow') == CONTRACT_CHANGE_TYPE_INFLOW) &
            (col('eventdate') <= current_date()))\
        .groupBy('environment', 'accountcode', 'itemcode')\
        .agg({'startdate': 'max', 'finaldate': 'max'})
    latestcomm_df = latestcomm_df\
        .withColumnRenamed('max(startdate)', 'latestcommstartdate')\
        .withColumnRenamed('max(finaldate)', 'latestcommfinaldate')\
        .withColumnRenamed('itemcode', 'latestcommpackage')

    result_df = result_df.join(latestcomm_df, ['environment', 'accountcode', 'latestcommpackage'], 'left').select('*')
    return result_df

# 7. SETS LATEST TRIAL PACKAGE 
def add_last_trial_package(contracts_df, result_df):
    window_spec = Window.partitionBy('accountcode', 'environment')\
        .orderBy(col("finaldate").desc(), col("sum(quantity)").desc())

    trialpackage_df = contracts_df.where(
            ((col('eventtype') == EVENT_TYPE_NEW_TRIAL) | (col('eventtype') == EVENT_TYPE_PRICE_INCREASE)) &
            (col('contracttype') == CONTRACT_TYPE_TRIAL) &
            (col('linepackage') == 1) &
            (col('eventdate') <= current_date()))\
        .groupBy('environment', 'accountcode', 'itemcode', 'finaldate')\
        .agg(sum('quantity'))
    trialpackage_df = trialpackage_df.withColumn("row_number",
        row_number().over(window_spec))

    trialpackage_df = trialpackage_df\
        .withColumnRenamed('itemcode', 'latesttrialpackage')\
        .filter(col("row_number") == 1)\
        .drop('row_number', 'sum(quantity)', 'finaldate')

    trialpackage_left_df = contracts_df.where(
        (col('contracttype') == CONTRACT_TYPE_TRIAL) &
        (col('linepackage') == 1))\
        .groupBy('environment', 'accountcode').count()
    trialpackage_left_df = trialpackage_left_df.drop('count')

    trialpackage_df = trialpackage_left_df.join(trialpackage_df, ['environment', 'accountcode'], 'left').select('*')
    result_df = result_df.join(trialpackage_df, ['environment', 'accountcode'], 'left').select('*')
    return result_df

# 8. SETS LATEST TRIAL START AND FINAL DATES
def add_last_trial_package_dates(contracts_df, result_df):
    latesttrial_df = contracts_df.where(
        (col('eventdate') <= current_date()) &
        (col('inflowoutflow') == CONTRACT_CHANGE_TYPE_INFLOW) &
        (col('contracttype') == CONTRACT_TYPE_TRIAL) &
        (col('linepackage') == 1))\
            .groupBy('environment', 'accountcode', 'itemcode')\
            .agg({'startdate': 'max', 'finaldate': 'max'})
    latesttrial_df = latesttrial_df\
        .withColumnRenamed('max(startdate)', 'latesttrialstartdate')\
        .withColumnRenamed('max(finaldate)', 'latesttrialfinaldate')\
        .withColumnRenamed('itemcode', 'latesttrialpackage')

    result_df = result_df.join(latesttrial_df, ['environment', 'accountcode', 'latesttrialpackage'], 'left').select('*')
    return result_df

# 9. SETS THE COMMERCIAL LIFETIME OF THE CONTRACT IN MONTHS
def add_commercial_lifetime_of_contract_in_months(result_df):
    ### This has the potential problem of continuing to count time for customers who had a break in time between their first and latest contracts.
    result_df = result_df.withColumn('commlifetimemonths', 
        when(col('hadcommcontract') == 1,
            when(col('latestcommfinaldate') > current_date(),
                months_between(current_date(), col('firstcommstartdate')))
            .otherwise(
                months_between(col('latestcommfinaldate'), col('firstcommstartdate')))
        )
        .otherwise(lit(0))
    )
    result_df = result_df.withColumn('commlifetimemonths', result_df['commlifetimemonths'].cast(IntegerType()))
    # Dates could be null so months_between returns null
    result_df = result_df.fillna(0, subset=['commlifetimemonths'])
    
    return result_df

# 10. SETS THE LATEST MRR OF THE CONTRACT -- This only counts commercial contract types.
def add_mrr_of_latest_contract(contracts_df, result_df):
    mrr_df = contracts_df.where(
        (col('eventdate') <= current_date()) &
        (col('contracttype') == CONTRACT_TYPE_COMMERCIAL))\
            .groupBy('environment', 'accountcode')\
            .agg({'valuepermonth': 'sum'})
    mrr_df = mrr_df\
        .withColumnRenamed('sum(valuepermonth)', 'latestmrr')\
        .withColumn('latestmrr', round(col('latestmrr'), 2))

    result_df = result_df.join(mrr_df, ['environment', 'accountcode'], 'left').select('*')
    # This column in SQL is int, but during transformation it is float.
    result_df = result_df.withColumn('latestmrr', col('latestmrr').cast(IntegerType()))

    return result_df

# 11. SETS THE NUMBER OF AVAILABLE ADMINISTRATIONS, ARCHIVED ADMINISTRATIONS, AND USERS
def add_numbers_of_administrations(contracts_df, result_df):
    admin_df = contracts_df.where(
        (col('finaldate') >= current_date()) & (col('eventdate') <= current_date()))
    code_archived_administration = 'EOL9950'
    admin_df = admin_df.withColumn('latestnumberofavailableadmins',
        when(col('itemcode') != code_archived_administration,
            col('numberofadministrations'))\
        .otherwise(lit(0)))  # This line counts the sum of NumberOfAdministrations that are not archived.
    admin_df = admin_df.withColumn('latestnumberofarchivedadmins',
        when(col('itemcode') == code_archived_administration,
            col('numberofadministrations'))\
        .otherwise(lit(0)))  # Counts the number of archived administrations
    admin_df = admin_df.withColumn('latestnumberpayingusers',
        when(col('valuepermonth') != 0,
            col('numberofusers'))\
        .otherwise(lit(0)))  # Counts the number of customers with a ValuePerMonth value that is not 0
    admin_df = admin_df.withColumn('latestnumberfreeusers',
        when(col('valuepermonth') == 0,
            col('numberofusers'))
        .otherwise(lit(0)))  # Counts the number of customers with a ValuePerMonth value that is 0
    admin_df = admin_df.groupBy('environment', 'accountcode')\
        .agg(
        {'latestnumberofavailableadmins': 'sum',
        'latestnumberofarchivedadmins': 'sum',
        'numberofusers': 'sum',
        'latestnumberpayingusers': 'sum',
        'latestnumberfreeusers': 'sum'})

    admin_df = renameColumns(admin_df, {
        'sum(latestnumberofarchivedadmins)': 'latestnumberofarchivedadmins',
        'sum(latestnumberofavailableadmins)': 'latestnumberofavailableadmins',
        'sum(latestnumberpayingusers)': 'latestnumberpayingusers',
        'sum(latestnumberfreeusers)': 'latestnumberfreeusers',
        'sum(numberofusers)': 'latestnumberofusers'
    })
    admin_df = convertDataTypes(admin_df,
        integer_cols=['latestnumberfreeusers', 'latestnumberofarchivedadmins', 
        'latestnumberofavailableadmins', 'latestnumberofusers', 'latestnumberpayingusers'])

    result_df = result_df.join(admin_df, ['environment', 'accountcode'], 'left').select('*')
    return result_df

# 12. SETS INDICATOR FOR FULL CANCELLATION DATE
def add_full_cancelation_indicator(contracts_df, result_df):
    window_spec = Window.partitionBy("environment", "accountcode")\
        .orderBy(col("eventdate").desc(), col("contractnumber").desc())

    fullcancellation_df = contracts_df.where(
        (col('inflowoutflow') == CONTRACT_CHANGE_TYPE_INFLOW) &
        (col('linepackage') == 1))
    fullcancellation_df = fullcancellation_df\
        .withColumn('requestedfullcancellation', 
            when(col('cancellationdate').isNull(), lit(0)).otherwise(lit(1)))
    fullcancellation_df = fullcancellation_df.withColumn("row_number", row_number().over(window_spec))
    fullcancellation_df = fullcancellation_df\
        .withColumnRenamed('cancellationdate', 'fullcancellationrequestdate')\
        .filter(col("row_number") == 1)\
        .select('environment', 'accountcode', 'fullcancellationrequestdate', 'requestedfullcancellation')

    result_df = result_df.join(fullcancellation_df, ['environment', 'accountcode'], 'left').select('*')
    return result_df


# 13. SETS INDICATOR WHETHER ACCOUNT HAS CHURNED
def add_has_churned_indicator(contracts_df, result_df):
    churned_df = contracts_df.select('environment', 'accountcode', 'finaldate')\
        .groupBy('environment', 'accountcode')\
        .agg(max('finaldate').alias('maxfinaldate'))
    churned_df = churned_df.withColumn('churned', 
        when(col('maxfinaldate') > current_date(), lit(0)).otherwise(lit(1)))

    result_df = result_df.join(churned_df, ['environment', 'accountcode'], 'left').select('*')
    return result_df


# 14. SETS WHETHER CUSTOMER HAS AN ACCOUNTANT LINKED AND THE DATE THEY FIRST LINKED TO AN ACCOUNTANT
def add_linked_accountant_data(contracts_df, accounts_df, linkedaccountantlog_df, result_df):
    window_spec = Window.partitionBy("environment", "accountcode")\
        .orderBy(col("date").desc())

    accountant_linked_status = 'Accountant Linked'

    linkedaccountantlog_df = linkedaccountantlog_df.where(col('linkstatus') == accountant_linked_status)
    linkedaccountantlog_df = linkedaccountantlog_df.withColumn("row_number", row_number().over(window_spec))
    linkedaccountantlog_df = linkedaccountantlog_df\
        .filter(col('row_number') == 1)\
        .select('environment', 'accountcode', 'linkstatus', 'date')

    df_2 = result_df.join(accounts_df, ["environment", "accountcode"], "inner").select('*')

    df_2 = df_2.join(linkedaccountantlog_df, ['environment', 'accountcode'], 'left').select('*')

    df_2 = df_2.withColumn("inputdate", lit('2016-02-09'))
    df_2 = df_2.withColumn('inputdate', col('inputdate').cast(DateType()))

    df_2 = df_2.withColumn('accountantlinked',
        when(trim(col('entrepreneuraccountantlinked')) == 'EntrepreneurWithAccountant',
            lit(1)).otherwise(lit(0)))

    df_2 = df_2.withColumn('datefirstlinkedtoaccountant', 
        when(
            (trim(col('entrepreneuraccountantlinked')) == 'EntrepreneurWithAccountant') &
            (trim(col('firstcommstartdate')) >= col('inputdate')) &
            (trim(col('linkstatus')) == accountant_linked_status),
                col('date'))
        .when(
            (trim(col('entrepreneuraccountantlinked')) == 'EntrepreneurWithAccountant') & 
            (trim(col('firstcommstartdate')) >= col('inputdate')) &
            (trim(col('linkstatus')).isNull()),
                col('firstcommstartdate'))
        .when(
            (trim(col('entrepreneuraccountantlinked')) == 'EntrepreneurWithoutAccountant') & 
            (trim(col('firstcommstartdate')) >= col('inputdate')) &
            (trim(col('linkstatus')) == accountant_linked_status),
                col('date'))
        .otherwise(lit(None))
    )

    df_2 = df_2.withColumn('datefirstlinkedtoaccountant', col('datefirstlinkedtoaccountant').cast(DateType()))

    df_2 = df_2.groupBy('environment', 'accountcode')\
        .agg({'accountantlinked': 'max',
            'datefirstlinkedtoaccountant': 'max'})
    df_2 = df_2\
        .withColumnRenamed('max(accountantlinked)', 'accountantlinked')\
        .withColumnRenamed('max(datefirstlinkedtoaccountant)', 'datefirstlinkedtoaccountant')

    df_2 = df_2.select("environment", "accountcode", "accountantlinked", "datefirstlinkedtoaccountant")
    result_df = result_df.join(df_2, ['environment', 'accountcode'], 'left').select('*')
    return result_df

def prepare_data_frames(contracts_df, accounts_df, linkedaccountantlog_df):
    contracts_df = contracts_df\
        .withColumn('itemcode', upper(trim(col('itemcode'))))\
        .withColumn('environment', upper(trim(col('environment'))))\
        .withColumn('accountcode', trim(col('accountcode')))\
        .withColumn('contracttype', upper(trim(col('contracttype'))))\
        .withColumn('eventtype', upper(trim(col('eventtype'))))\
        .withColumn('inflowoutflow', trim(col('inflowoutflow')))
    
    accounts_df = accounts_df\
        .withColumn('accountcode', trim(col('accountcode')))\
        .withColumn('environment', upper(trim(col('environment'))))

    linkedaccountantlog_df = linkedaccountantlog_df\
        .withColumn('environment', upper(trim(col('environment'))))

    contracts_df.cache()
    accounts_df.cache()
    linkedaccountantlog_df.cache()

    return contracts_df, accounts_df, linkedaccountantlog_df