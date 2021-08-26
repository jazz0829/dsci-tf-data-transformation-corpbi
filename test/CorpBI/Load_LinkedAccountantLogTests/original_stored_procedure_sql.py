import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from test.Consts import Constant

class OriginalStoredProcedureSQL(Constant):
    STEP_1_SQL = """
        SELECT
            DISTINCT
				(CASE WHEN Environment = 'GB' THEN 'UK'
                ELSE RTRIM(Environment)
                END) AS Environment
            , RTRIM(LTRIM(Code)) AS AccountCode
            , AccountantOrLinked
            , RTRIM(LTRIM(AccountantCode)) AS AccountantCode
        FROM [raw].[DW_ContractStatistics]
        WHERE 
            IsAccountant = 'Entrepreneur'		 -- Only selects Entrepreneurs
            AND META_LOAD_ENDDTS IS NULL		 -- If field is NULL then this means it is an old record
            AND META_ISDELETED = 0				 -- If equal to 1 then this row is one that should be deleted
    """

    STEP_2_SQL = """
    SELECT
        ContractStatistics.Environment
        , ContractStatistics.AccountCode
        , ContractStatistics.AccountantOrLinked AS NewAccountantOrLinked
        , Accounts.AccountantOrLinked AS PreviousAccountantOrLinked
        , ContractStatistics.AccountantCode AS NewAccountantCode
        , Accounts.AccountantCode AS PreviousAccountantCode
    FROM
    (
        """+STEP_1_SQL+"""
    ) ContractStatistics
    INNER JOIN [CustomerIntelligence].domain.Accounts
        ON Accounts.Environment = ContractStatistics.Environment
        AND RTRIM(LTRIM(Accounts.AccountCode)) = ContractStatistics.AccountCode
    WHERE
	(
		ContractStatistics.AccountantOrLinked <> Accounts.AccountantOrLinked
		OR ContractStatistics.AccountantCode <> RTRIM(LTRIM(Accounts.AccountantCode))
	)
    """

    STEP_3_SQL = """
    SELECT
        Environment
        , AccountCode
        , (CASE
            WHEN NewAccountantOrLinked = 1 AND PreviousAccountantOrLinked = 0 THEN 'Accountant Linked'
            WHEN NewAccountantOrLinked = 0 AND PreviousAccountantOrLinked = 1 THEN 'Accountant Link Removed'
            WHEN NewAccountantOrLinked = 1 AND PreviousAccountantOrLinked = 1 AND NewAccountantCode <> PreviousAccountantCode THEN 'Accountant Change'
            END) AS LinkStatus		-- This describes the type of change made to the linked accountant.
        , CONVERT(DATE, (GETDATE() - 1)) AS [Date]
        , NewAccountantCode
        , PreviousAccountantCode

    FROM
        (
        """+STEP_2_SQL+"""
        ) Change
    """