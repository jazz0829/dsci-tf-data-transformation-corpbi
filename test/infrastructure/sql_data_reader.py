import pyodbc
import pandas

def execute_sql_query(sql_query):
    print('Establishing SQL connection')
    sql_conn = pyodbc.connect('Driver={SQL Server};'
                    'Server=NLC1PRODCI01;'
                    'Database=CustomerIntelligence;'
                    'Trusted_Connection=yes;')
    print('Querying SQL...')
    df = pandas.read_sql(sql_query, sql_conn)
    sql_conn.close()
    print('SQL connection closed')
    return df