import sys
from utils import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.window import Window
from pyspark.sql.functions import when, sum, concat, lit, date_add, current_date, lag, row_number
from pyspark.sql.types import StringType, DateType

glueContext = GlueContext(SparkContext.getOrCreate())

def make_common_table(
    contracts_df,
    accountscontractsummary_df,
    contracteventtypes_df,
    itemclassifications_df):
    type_commercial = 'C'
    code_price_increase = 'PI'
    common_table_df = contracts_df\
        .join(accountscontractsummary_df,
            ((col("domain_contracts.environment") == col("domain_acs.environment")) &
            (col("domain_contracts.accountcode") == col("domain_acs.accountcode"))), 
            "inner")\
        .join(contracteventtypes_df,
            ((col("domain_contracts.environment") == col("domain_contracteventtypes.environment")) &
            (col("domain_contracts.eventtype") == col("domain_contracteventtypes.contracteventtypecode"))),
            "inner")\
        .join(itemclassifications_df,
            ((col("domain_contracts.environment") == col("domain_itemclassifications.environment")) &
            (col("domain_contracts.itemcode") == col("domain_itemclassifications.itemcode"))), 
            "inner")\
        .where(col("accountid").isNotNull())\
            .withColumn("linepackage_aggregated", 
                        when(col("domain_contracts.eventtype") == code_price_increase, 0)
                        .otherwise(col("domain_contracts.linepackage")))\
            .withColumn("quantity_aggregated", sum('domain_contracts.quantity').over(get_common_table_quatntity_partitioning()))\
            .withColumn("summrr", # maybe sum_mrr is a better name?
                            sum(when(col("domain_contracts.contracttype") == type_commercial, col("domain_contracts.valuepermonth"))
                                .otherwise(0))
                            .over(get_common_table_mrr_partitioning()))\
        .select(
            "domain_acs.accountid",
            "domain_contracts.eventdate",
            "domain_contracteventtypes.contracteventdescription",
            "domain_itemclassifications.itemdescription",
            "linepackage_aggregated",
            "quantity_aggregated",
            "summrr")

    common_table_df = common_table_df\
        .withColumnRenamed("linepackage_aggregated", "linepackage")\
        .withColumnRenamed("quantity_aggregated", "quantity")

    return common_table_df

def get_common_table_mrr_partitioning():
    # Calculates a cummulative MRR value
    partitioning = Window.partitionBy(
        col('domain_contracts.environment'),
        col('domain_contracts.accountcode'))\
        .orderBy(col('domain_contracts.eventdate'))
    return partitioning

def get_common_table_quatntity_partitioning():
    # Calculates the aggregate quantity change to the contract item (sum of inflow and outflow)
    partitioning = Window.partitionBy(
        col('domain_contracts.environment'),
        col('domain_contracts.accountcode'),
        col('domain_contracts.eventdate'),
        col('domain_contracteventtypes.contracteventdescription'),
        col('domain_itemclassifications.itemdescription'))\
        .orderBy(col('domain_contracts.eventdate'))
    return partitioning

def pick_customer_profile_contact_history_columns(common_table_df):
    customer_profile_contact_history_df = common_table_df\
        .withColumn("description", 
                    concat(col('contracteventdescription'), lit(' | '), col('itemdescription'), lit(' | Quantity '), col('quantity')))\
        .select(
            "accountid",
            "eventdate",
            "linepackage",
            "summrr",
            "description"
        )\
        .where(col("accountid").isNotNull())
    return customer_profile_contact_history_df

def add_every_customer_first_contract_change(
    customer_profile_contact_history_df,
    common_table_df):
    # This first UNION statement creates a date occurence before the change made in the Contracts table for the customer
    # This is so the line charts for Contract History have a period of time to connect a particular MRR value for
    first_contract_change_df = common_table_df\
        .withColumn("eventdate_aggregated", date_add(col("eventdate"), -1))\
        .withColumn("summrr_aggregated", lag(col("summrr"), 1, 0)
                                .over(Window.partitionBy("accountid").orderBy(col('eventdate'))))\
        .groupBy("accountid", "eventdate_aggregated", "summrr_aggregated")\
            .count() # ANSI-sql requiers an aggregation function but our query shouldn't use it
    
    first_contract_change_df = first_contract_change_df\
        .withColumn("linepackage_mocked", lit(None).cast(StringType()))\
        .withColumn("description_mocked", lit(None).cast(StringType()))\
        .withColumnRenamed("linepackage_mocked", "linepackage")\
        .withColumnRenamed("description_mocked", "description")\
        .withColumnRenamed("eventdate_aggregated", "eventdate")\
        .withColumnRenamed("summrr_aggregated", "summrr")

    first_contract_change_df = first_contract_change_df\
         .select(
            "accountid",
            "eventdate",
            "linepackage",
            "summrr",
            "description"
        )\
        .where(col("accountid").isNotNull())
        
    return customer_profile_contact_history_df\
        .union(first_contract_change_df)

def add_every_customer_current_mrr(
    customer_profile_contact_history_df,
    common_table_df):
    # This second UNION takes the latest MRR status for either a change being made in the future, or if there are no future changes, the current date
    # This is to give the line charts for Contract History a date to show the current MRR status 
    latest_row_partitioning = Window.partitionBy(col('accountid')).orderBy(col('eventdate').desc())
    latest_row_df = common_table_df\
        .withColumn("row_num", row_number().over(latest_row_partitioning))\
        .select(
            "accountid",
            "eventdate",
            "row_num",
            "summrr")
    today = current_date()
    current_mrr_df = latest_row_df\
        .withColumn("linepackage_mocked", lit(None).cast(StringType()))\
        .withColumn("description_mocked", lit(None).cast(StringType()))\
        .withColumn("eventdate_aggregated", when((col("eventdate") >= today), col("eventdate"))
                                    .otherwise(lit(today).cast(DateType())))\
        .select(
            "accountid",
            "eventdate_aggregated",
            "linepackage_mocked",
            "summrr",
            "description_mocked")\
        .where(col("accountid").isNotNull())

    current_mrr_df = current_mrr_df\
        .withColumnRenamed("linepackage_mocked", "linepackage")\
        .withColumnRenamed("description_mocked", "description")\
        .withColumnRenamed("eventdate_aggregated", "eventdate")\

    the_latest_record_index = 1
    current_mrr_df = current_mrr_df\
        .where(col('row_num') == the_latest_record_index)\
        .drop('row_num')

    return customer_profile_contact_history_df\
        .union(current_mrr_df)

def save_transformation_result(dataframe, s3_destination):
    dataframe.repartition(1).write.mode("overwrite").parquet(s3_destination)

def init_data_frames(args):
    domain_db = args['domain_db']

    contracts_df = glueContext.create_dynamic_frame.from_catalog(domain_db, args['contracts_domain_table']).toDF()
    contracts_df = cleanDataFrame(contracts_df, to_upper_cols=['environment']).alias("domain_contracts")
    
    accountscontractsummary_df = glueContext.create_dynamic_frame.from_catalog(domain_db, args['accountscontractsummary_domain_table']).toDF()
    accountscontractsummary_df = cleanDataFrame(accountscontractsummary_df, to_upper_cols=['environment']).alias("domain_acs")
   
    contracteventtypes_df = glueContext.create_dynamic_frame.from_catalog(domain_db, args['contracteventtypes_domain_table']).toDF()
    contracteventtypes_df = cleanDataFrame(contracteventtypes_df, to_upper_cols=['environment']).alias("domain_contracteventtypes")
    
    itemclassifications_df = glueContext.create_dynamic_frame.from_catalog(domain_db, args['itemclassifications_domain_table']).toDF()
    itemclassifications_df = cleanDataFrame(itemclassifications_df, to_upper_cols=['environment']).alias("domain_itemclassifications")

    return contracts_df, accountscontractsummary_df, contracteventtypes_df, itemclassifications_df

if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, [
        'domain_db', 'contracts_domain_table',
        'accountscontractsummary_domain_table', 'contracteventtypes_domain_table',
        'itemclassifications_domain_table',
        'customerprofile_contracthistory_s3_destination'])

    contracts_df, accountscontractsummary_df, contracteventtypes_df, itemclassifications_df\
         = init_data_frames(args)

    common_table_df = make_common_table(
        contracts_df,
        accountscontractsummary_df,
        contracteventtypes_df,
        itemclassifications_df)

    common_table_df.cache()

    customer_profile_contact_history_df =\
        pick_customer_profile_contact_history_columns(common_table_df)

    customer_profile_contact_history_df =\
        add_every_customer_first_contract_change(customer_profile_contact_history_df, common_table_df)
    
    customer_profile_contact_history_df =\
        add_every_customer_current_mrr(customer_profile_contact_history_df, common_table_df)

    save_transformation_result(
        customer_profile_contact_history_df,
        args['customerprofile_contracthistory_s3_destination'])