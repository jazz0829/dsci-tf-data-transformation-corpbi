from domain_Load_AccountsContract_Summary_Transformation import *

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

def load_data_frames(sys_argv):
    args = getResolvedOptions(sys_argv,
        ['domain_db', 'contracts_domain_table', 'accounts_domain_table',
        'linkedaccountantlog_domain_table', 's3_destination'])

    domain_db = args['domain_db']
    glueContext = GlueContext(SparkContext.getOrCreate())
    
    accounts_df = glueContext.create_dynamic_frame.from_catalog(
        database=domain_db, table_name=args['accounts_domain_table']).toDF()
    
    linkedaccountantlog_df = glueContext.create_dynamic_frame.from_catalog(
        database=domain_db, table_name=args['linkedaccountantlog_domain_table']).toDF()
    
    contracts_df = glueContext.create_dynamic_frame.from_catalog(
        database=domain_db, table_name=args['contracts_domain_table']).toDF()

    return contracts_df, accounts_df, linkedaccountantlog_df, args['s3_destination']

if __name__ == "__main__":
    contracts_df, accounts_df, linkedaccountantlog_df, accountscontract_summary_s3_destination = load_data_frames(sys.argv)

    contracts_df, accounts_df, linkedaccountantlog_df = prepare_data_frames(contracts_df, accounts_df, linkedaccountantlog_df)

    result_df = get_account_contracts(contracts_df, accounts_df)
    result_df = add_first_trail_package_data(contracts_df, result_df)
    result_df = add_first_commercial_package_data(contracts_df, result_df)
    result_df = add_had_trail_or_commercial_indicators(result_df)
    result_df = add_last_commercial_package(contracts_df, result_df)
    result_df = add_last_commercial_package_dates(contracts_df, result_df)
    result_df = add_last_trial_package(contracts_df, result_df)
    result_df = add_last_trial_package_dates(contracts_df, result_df)
    result_df = add_commercial_lifetime_of_contract_in_months(result_df)
    result_df = add_mrr_of_latest_contract(contracts_df, result_df)
    result_df = add_numbers_of_administrations(contracts_df, result_df)
    result_df = add_full_cancelation_indicator(contracts_df, result_df)
    result_df = add_has_churned_indicator(contracts_df, result_df)
    result_df = add_linked_accountant_data(contracts_df, accounts_df, linkedaccountantlog_df, result_df)

    # Save results to parquet files
    result_df.repartition(1).write.mode("overwrite").parquet(accountscontract_summary_s3_destination)