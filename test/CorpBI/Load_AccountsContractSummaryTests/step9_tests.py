import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
import unittest

from parquet_data_reader import load_local_data_frames
from original_stored_procedure_sql import OriginalStoredProcedureSQL
from test.infrastructure.sql_data_reader import execute_sql_query
from test.infrastructure.data_asserter import assert_data_frames

from src.scripts.CorpBI.domain_Load_AccountsContract_Summary_Transformation import *
import pandas

class Step9_Load_AccountsContractSummary_Test(unittest.TestCase):
    def test_step_is_executed_correctly(self):
        # Arrange
        sql_data = execute_sql_query(OriginalStoredProcedureSQL.STEP_9_SQL)
        expected_data = sql_data
        expected_data['AccountID'] = expected_data['AccountID'].str.strip().str.upper()
        expected_data['AccountCode'] = expected_data['AccountCode'].astype(str).str.strip()
        expected_data['Environment'] = expected_data['Environment'].str.strip().str.upper()
        expected_data = expected_data.sort_values(by=['AccountID', 'AccountCode'])

        # Act
        print("Applying domain transformation")
        aws_data = self.execute_transformation_step()
        aws_data = aws_data.toPandas()
        aws_data = aws_data.sort_values(by=['accountid', 'accountcode'])

        # Assert
        self.assertEqual(len(aws_data.axes[0]), len(expected_data.axes[0]), 'Rows count mismatches')
        self.assertEqual(len(aws_data.axes[1]), len(expected_data.axes[1]), 'Column count mismatches')
        print('Initial check passed')
        
        assert_data_frames(aws_data, expected_data)
        print('Per column check passed')
        print('Test completed.')


    def execute_transformation_step(self):
        contracts_df, accounts_df, linkedaccountantlog_df = load_local_data_frames()

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

        return result_df

if __name__ == "__main__":
    unittest.main()