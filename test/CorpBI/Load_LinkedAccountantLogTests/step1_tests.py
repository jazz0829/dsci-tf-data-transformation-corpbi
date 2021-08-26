import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
import unittest

from parquet_data_reader import load_local_data_frames
from original_stored_procedure_sql import OriginalStoredProcedureSQL
from test.infrastructure.sql_data_reader import execute_sql_query
from test.infrastructure.data_asserter import assert_data_frames

from src.scripts.CorpBI.domain_Load_LinkedAccountantLog_Transformation import *
import pandas

class Step1_Load_LinkedAccountantLog_Test(unittest.TestCase):
    def test_step_is_executed_correctly(self):
        # Arrange
        sql_data = execute_sql_query(OriginalStoredProcedureSQL.STEP_1_SQL)
        # sql_data.to_csv('sql_data.csv', encoding='utf-8', index=False)
        # sql_data = pandas.read_csv('sql_data.csv')
        # sql_data.fillna(value=pandas.np.nan, inplace=True)
        expected_data = sql_data
        expected_data['AccountantOrLinked'] = expected_data['AccountantOrLinked'].astype(bool)        
        expected_data = expected_data.sort_values(by=['AccountCode', 'AccountantCode', 'Environment'])

        # Act
        print("Applying domain transformation")
        aws_data = self.execute_transformation_step()
        aws_data = aws_data.toPandas()
        # aws_data.to_csv('aws_data.csv', encoding='utf-8', index=False)
        # aws_data = pandas.read_csv('aws_data.csv')
        aws_data['accountantcode'] = aws_data['accountantcode'].astype(str).str.strip()        
        # aws_data.fillna(value=pandas.np.nan, inplace=True)
        aws_data = aws_data.sort_values(by=['accountcode', 'accountantcode', 'environment'])

        # Assert
        self.assertEqual(len(aws_data.axes[0]), len(sql_data.axes[0]), 'Rows count mismatches')
        self.assertEqual(len(aws_data.axes[1]), len(sql_data.axes[1]), 'Column count mismatches')
        print('Initial check passed')
        assert_data_frames(aws_data, expected_data)
        print('Per column check passed')
        print('Test completed.')

    def execute_transformation_step(self):
        contractstatistics_raw_df, accounts_df = load_local_data_frames()
        
        contractstatistics_raw_df, accounts_df = prepare_data_frames(contractstatistics_raw_df, accounts_df)

        result_df = get_entrepreneur_contractstatistics(contractstatistics_raw_df)

        return result_df

if __name__ == "__main__":
    unittest.main()