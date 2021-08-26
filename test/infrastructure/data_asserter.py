from approvaltests.approvals import verify_file
from approvaltests.reporters.generic_diff_reporter_factory import GenericDiffReporterFactory
from approvaltests.file_approver import FileApprover
from approvaltests.string_writer import StringWriter

import tempfile
import numpy as np

def assert_data_frames(aws_data, expected_sql_data):
    reporter = GenericDiffReporterFactory().get_first_working()
    approver = FileApprover()

    for col in expected_sql_data:
        expected_sql_data[col] = expected_sql_data[col].astype(str)
    for col in aws_data:
        aws_data[col] = aws_data[col].astype(str)

    expected_data_columns = expected_sql_data.apply('\n'.join, axis=0)
    actual_columns = aws_data.apply('\n'.join, axis=0)

    for sql_column_name in expected_data_columns._info_axis:
        sql_column_values = expected_data_columns[sql_column_name]
        writer = StringWriter(sql_column_values)
        expected_file = tempfile.gettempdir() + '\SQL - ' + sql_column_name
        writer.write_received_file(expected_file)
        
        aws_column_name = sql_column_name.lower()
        aws_column_values = actual_columns[aws_column_name]
        writer = StringWriter(aws_column_values)
        actual_file = tempfile.gettempdir() + '\PRQ - ' + aws_column_name
        writer.write_received_file(actual_file)

        approver.verify_files(expected_file, actual_file, reporter)