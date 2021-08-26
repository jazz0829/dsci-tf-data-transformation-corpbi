import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from test.Consts import Constant

class OriginalStoredProcedureSQL(Constant):
    STEP_1_SQL = """
			SELECT step1_C.Environment, step1_C.AccountCode, step1_A.AccountID
			FROM domain.Contracts AS step1_C
			JOIN domain.Accounts step1_A
			ON step1_A.AccountCode=step1_C.AccountCode AND step1_A.Environment=step1_C.Environment
			WHERE step1_A.AccountCode<>'162697700000000000' AND step1_C.AccountCode<>'162697700000000000'
			GROUP BY
		        step1_C.Environment,
				step1_C.AccountCode,
				step1_A.AccountID
        """

    STEP_2_SQL = """
        SELECT ACS.Environment, ACS.AccountCode, ACS.AccountID,
            FTP.StartDate as FirstTrialStartDate
            ,FTP.Package as FirstTrialPackage
            ,FTP.FinalDate as FirstTrialFinalDate
        FROM

        (""" + STEP_1_SQL + """) ACS

        LEFT JOIN
            (
                SELECT 
                    step2_C.Environment,
                    step2_C.AccountCode,
                    MIN(step2_C.StartDate) AS StartDate, --this sets the earliest date of an account's Trial contract type for the FirstTrialStartDate
                    MIN(step2_C.FinalDate) AS FinalDate,
                    
                    --this sub-query selects the first occurence of the Trial package type for the FirstTrialPackage
                    (	SELECT TOP 1 UPPER(ItemCode)
                        FROM [domain].[Contracts]
                        WHERE 
                            Environment = step2_C.Environment
                            AND AccountCode = step2_C.AccountCode
                            AND ContractType = 'T'				--this selects only Trial contract types
                            AND LinePackage = 1					--this selects only the row in the Contracts table where there is a change to the package
                            AND (EventType = 'TN' OR EventType = 'PI')	-- this selects only the event type 'New Trial'
                            AND InflowOutflow = 'Inflow'		--this selects only the row with an inflow to the contract
                            AND EventDate <= GETDATE()		--this ignores changes to contracts that are occurring in the future
                        ORDER BY EventDate 
                    ) AS Package

                FROM [domain].[Contracts] step2_C
                WHERE
                    step2_C.ContractType = 'T'
                    AND step2_C.LinePackage = 1
                    AND (step2_C.EventType = 'TN' OR step2_C.EventType = 'PI')
                GROUP BY
                    step2_C.Environment,
                    step2_C.AccountCode
            ) FTP

            ON ACS.Environment = FTP.Environment AND ACS.AccountCode = FTP.AccountCode
    """

    STEP_3_SQL = """
     SELECT STEP2.Environment, STEP2.AccountCode, STEP2.AccountID,
            STEP2.FirstTrialStartDate,
            STEP2.FirstTrialPackage,
            STEP2.FirstTrialFinalDate,
            FCP.StartDate AS FirstCommStartDate
	        ,FCP.FinalDate AS FirstCommFinalDate
	        ,FCP.Package AS FirstCommPackage

        FROM

        (""" + STEP_2_SQL + """) STEP2

        LEFT JOIN
        (
            SELECT 
                step3_C.Environment, 
                step3_C.AccountCode,
                MIN(step3_C.StartDate) AS StartDate,
                MIN(step3_C.FinalDate) AS FinalDate,

                --this sub-query selects the first occurence of the Commercial package type for the FirstCommPackage	
                (
                    SELECT TOP 1 UPPER(ItemCode)
                    FROM [domain].[Contracts]
                    WHERE 
                        Environment = step3_C.Environment
                        AND AccountCode = step3_C.AccountCode
                        AND ContractType = 'C'			--this selects only the Commercial contract types
                        AND LinePackage = 1				--this selects only the row in the Contracts table where there is a change to the package
                        AND InflowOutflow = 'Inflow'	--this selects only the row with an inflow to the contract
                        AND EventDate <= GETDATE()		--this ignores changes to contracts that are occurring in the future
                    ORDER BY EventDate
                ) AS Package
                
            FROM [domain].[Contracts] step3_C
            WHERE 
                step3_C.ContractType = 'C'
                AND step3_C.LinePackage = 1
            GROUP BY 
                step3_C.Environment,
                step3_C.AccountCode

        ) FCP

        ON STEP2.Environment = FCP.Environment AND STEP2.AccountCode = FCP.AccountCode
    """

    STEP_4_SQL = """
     SELECT *,
        CASE WHEN FirstTrialPackage IS NOT NULL THEN 1 ELSE 0 END AS HadTrial,
        CASE WHEN FirstCommPackage IS NOT NULL THEN 1 ELSE 0 END AS HadCommContract
    FROM(
    """ + STEP_3_SQL + """
    ) AS STEP3
    """

    STEP_5_SQL = """
         SELECT STEP4.Environment, STEP4.AccountCode, STEP4.AccountID,
            STEP4.FirstTrialStartDate, STEP4.FirstTrialPackage, STEP4.FirstTrialFinalDate,
            STEP4.FirstCommStartDate, STEP4.FirstCommFinalDate, STEP4.FirstCommPackage,
            STEP4.HadTrial, STEP4.HadCommContract
            ,LCP.Package AS LatestCommPackage

        FROM

        (""" + STEP_4_SQL + """) STEP4

        LEFT JOIN
        (
            SELECT
            step5_C.Environment,
            step5_C.AccountCode,
            (SELECT TOP 1 UPPER(ItemCode)
                FROM [domain].[Contracts]
                WHERE
                    Environment = step5_C.Environment
                    AND AccountCode = step5_C.AccountCode
                    AND ContractType = 'C'			--this selects only the Commercial contract types
                    AND LinePackage = 1				--this selects only the row in the Contracts table where there is a change to the package
                    AND EventDate <= GETDATE()		--this ignores changes to contracts that are occurring in the future
                GROUP BY 
                    Environment,
                    AccountCode,
                    UPPER(ItemCode),
                    FinalDate
                ORDER BY 
                    FinalDate DESC,					-- first this orders by the latest FinalDate of contracts
                    SUM(Quantity) DESC				-- this then orders by packages (itemcode) with a positive quantity, so downgraded packages are not selected. 
            ) AS Package
            FROM
                [domain].[Contracts] step5_C
            WHERE
                step5_C.ContractType = 'C'
                AND	step5_C.LinePackage  = 1
            GROUP BY
                Environment,
                AccountCode
        ) LCP
        
        ON STEP4.Environment = LCP.Environment AND STEP4.AccountCode = LCP.AccountCode
    """

    STEP_6_SQL = """
         SELECT STEP5.Environment, STEP5.AccountCode, STEP5.AccountID,
            STEP5.FirstTrialStartDate, STEP5.FirstTrialPackage, STEP5.FirstTrialFinalDate,
            STEP5.FirstCommStartDate, STEP5.FirstCommFinalDate, STEP5.FirstCommPackage,
            STEP5.HadTrial, STEP5.HadCommContract,
            STEP5.LatestCommPackage
            , LCPD.StartDate AS LatestCommStartDate
            , LCPD.FinalDate AS LatestCommFinalDate

        FROM

        (""" + STEP_5_SQL + """) STEP5

        LEFT JOIN
        (
            SELECT 
                step6_C.Environment,
                step6_C.AccountCode,
                MAX(step6_C.StartDate) AS StartDate,
                MAX(step6_C.FinalDate) AS FinalDate,
                UPPER(step6_C.ItemCode) AS ItemCode
                    
                FROM [domain].[Contracts] step6_C
                WHERE
                    step6_C.ContractType = 'C'
                    AND	step6_C.LinePackage  = 1
                    AND InflowOutflow = 'Inflow'
                    AND EventDate <= GETDATE()			--ignores StartDates and FinalDates that are related to contract changes occurring in the future
                GROUP BY 
                    step6_C.Environment,
                    step6_C.AccountCode,
                    UPPER(step6_C.ItemCode)
        ) LCPD
        
        ON STEP5.Environment = LCPD.Environment AND STEP5.AccountCode = LCPD.AccountCode
            AND STEP5.LatestCommPackage = LCPD.ItemCode
    """

    STEP_7_SQL = """
         SELECT STEP6.Environment, STEP6.AccountCode, STEP6.AccountID,
            STEP6.FirstTrialStartDate, STEP6.FirstTrialPackage, STEP6.FirstTrialFinalDate,
            STEP6.FirstCommStartDate, STEP6.FirstCommFinalDate, STEP6.FirstCommPackage,
            STEP6.HadTrial, STEP6.HadCommContract,
            STEP6.LatestCommPackage, STEP6.LatestCommStartDate, STEP6.LatestCommFinalDate,
            LTP.Package AS LatestTrialPackage

        FROM

        (""" + STEP_6_SQL + """) STEP6

        LEFT JOIN
        (
            SELECT 
                step7_C.Environment,
                step7_C.AccountCode,
                (SELECT TOP 1 UPPER(ItemCode)
                    FROM [domain].[Contracts] 
                    WHERE 
                        Environment = step7_C.Environment 
                        AND AccountCode = step7_C.AccountCode 
                        AND ContractType = 'T'			--this selects only the Trial contract types
                        AND LinePackage = 1				--this selects only the row in the Contracts table where there is a change to the package
                        AND (EventType = 'TN'	-- this selects only the event type 'New Trial'
                        OR EventType = 'PI')
                        AND EventDate <= GETDATE()		--this ignores changes to contracts that are occurring in the future		
                    GROUP BY 
                        Environment,
                        AccountCode,
                        UPPER(ItemCode),
                        FinalDate
                    ORDER BY 
                        FinalDate DESC,					-- first this orders by the latest FinalDate of contracts
                        SUM(Quantity) DESC				-- this then orders by packages (itemcode) with a positive quantity, so downgraded packages are not selected. 
                ) AS Package
            FROM
                [domain].[Contracts] step7_C
            WHERE 
                step7_C.ContractType = 'T' 
                AND	step7_C.LinePackage = 1
            GROUP BY
                Environment,
                AccountCode
        ) LTP
        
        ON STEP6.Environment = LTP.Environment AND STEP6.AccountCode = LTP.AccountCode
    """

    STEP_8_SQL = """
         SELECT STEP7.Environment, STEP7.AccountCode, STEP7.AccountID,
            STEP7.FirstTrialStartDate, STEP7.FirstTrialPackage, STEP7.FirstTrialFinalDate,
            STEP7.FirstCommStartDate, STEP7.FirstCommFinalDate, STEP7.FirstCommPackage,
            STEP7.HadTrial, STEP7.HadCommContract,
            STEP7.LatestCommPackage, STEP7.LatestCommStartDate, STEP7.LatestCommFinalDate,
            STEP7.LatestTrialPackage            
            , LTPD.StartDate AS LatestTrialStartDate
            , LTPD.FinalDate AS LatestTrialFinalDate

        FROM

        (""" + STEP_7_SQL + """) STEP7

        LEFT JOIN
        (
            SELECT 
                step8_C.Environment,
                step8_C.AccountCode,
                MAX(step8_C.StartDate) AS StartDate,
                MAX(step8_C.FinalDate) AS FinalDate,
                UPPER(step8_C.ItemCode) AS ItemCode
                    
                FROM [domain].[Contracts] step8_C
                WHERE
                    step8_C.ContractType = 'T'
                    AND	step8_C.LinePackage  = 1
                    AND InflowOutflow = 'Inflow'
                    AND EventDate <= GETDATE()			--ignores StartDates and FinalDates that are related to contract changes occurring in the future
                GROUP BY 
                    step8_C.Environment,
                    step8_C.AccountCode,
                    UPPER(step8_C.ItemCode)
        ) LTPD
        
        ON STEP7.Environment = LTPD.Environment AND STEP7.AccountCode = LTPD.AccountCode
            AND STEP7.LatestTrialPackage = LTPD.ItemCode
    """

    STEP_9_SQL = """
        SELECT *,
            CASE WHEN HadCommContract = 1 THEN (CASE WHEN LatestCommFinalDate > GETDATE() THEN DATEDIFF(MONTH, FirstCommStartDate, GETDATE()) ELSE DATEDIFF(MONTH, FirstCommStartDate, LatestCommFinalDate) END) ELSE 0 END AS CommLifetimeMonths
        FROM(
        """ + STEP_8_SQL + """
        ) AS STEP8
    """

    STEP_10_SQL = """
         SELECT STEP9.Environment, STEP9.AccountCode, STEP9.AccountID,
            STEP9.FirstTrialStartDate, STEP9.FirstTrialPackage, STEP9.FirstTrialFinalDate,
            STEP9.FirstCommStartDate, STEP9.FirstCommFinalDate, STEP9.FirstCommPackage,
            STEP9.HadTrial, STEP9.HadCommContract,
            STEP9.LatestCommPackage, STEP9.LatestCommStartDate, STEP9.LatestCommFinalDate,
            STEP9.LatestTrialPackage, STEP9.LatestTrialStartDate, STEP9.LatestTrialFinalDate,
            STEP9.CommLifetimeMonths,
            CAST(LCMRR.LatestMRR AS INT) AS LatestMRR

        FROM

        (""" + STEP_9_SQL + """) STEP9

        LEFT JOIN
        (
            SELECT 
                step10_C.Environment, 
                step10_C.AccountCode, 
                ROUND(SUM(step10_C.ValuePerMonth),2) AS LatestMRR
            FROM [domain].[Contracts] step10_C
            WHERE
                EventDate <= GETDATE()		--this ignores changes to contracts that are occurring in the future
                AND ContractType = 'C'
            GROUP BY 
                step10_C.Environment,
                step10_C.AccountCode
        ) LCMRR
        
        ON STEP9.Environment = LCMRR.Environment AND STEP9.AccountCode = LCMRR.AccountCode
    """

    STEP_11_SQL = """
        SELECT STEP10.Environment, STEP10.AccountCode, STEP10.AccountID,
            STEP10.FirstTrialStartDate, STEP10.FirstTrialPackage, STEP10.FirstTrialFinalDate,
            STEP10.FirstCommStartDate, STEP10.FirstCommFinalDate, STEP10.FirstCommPackage,
            STEP10.HadTrial, STEP10.HadCommContract,
            STEP10.LatestCommPackage, STEP10.LatestCommStartDate, STEP10.LatestCommFinalDate,
            STEP10.LatestTrialPackage, STEP10.LatestTrialStartDate, STEP10.LatestTrialFinalDate,
            STEP10.CommLifetimeMonths,
            STEP10.LatestMRR,
            ADM.LatestNumberOfAvailableAdmins,
            ADM.LatestNumberOfArchivedAdmins,
            ADM.LatestNumberOfUsers,
            ADM.LatestNumberPayingUsers,
            ADM.LatestNumberFreeUsers
        FROM

        (""" + STEP_10_SQL + """) STEP10

        LEFT JOIN
        (
            SELECT 
                step11_C.Environment, 
                step11_C.AccountCode,
                SUM(CASE WHEN ItemCode != 'EOL9950' THEN NumberOfAdministrations ELSE 0 END) AS LatestNumberOfAvailableAdmins,		--EOL9950 is the ItemCode for an archived administration. This line counts the sum of NumberOfAdministrations that are not archived. 
                SUM(CASE WHEN ItemCode = 'EOL9950' THEN NumberOfAdministrations ELSE 0 END) AS LatestNumberOfArchivedAdmins,  -- Counts the number of archived administrations 
                SUM(NumberOfUsers) AS LatestNumberOfUsers,
                SUM(CASE WHEN ValuePerMonth <> 0 THEN NumberOfUsers ELSE 0 END) AS LatestNumberPayingUsers,					-- Counts the number of customers with a ValuePerMonth value that is not 0
                SUM(CASE WHEN ValuePerMonth = 0 THEN NumberOfUsers ELSE 0 END) AS LatestNumberFreeUsers						-- Counts the number of customers with a ValuePerMonth value that is 0

            FROM [domain].[Contracts] step11_C
            WHERE
                EventDate <= GETDATE()		--this ignores changes to contracts that are occurring in the future
                AND FinalDate >= GETDATE()
            GROUP BY 
                step11_C.Environment, 
                step11_C.AccountCode
        ) ADM
        
        ON STEP10.Environment = ADM.Environment AND STEP10.AccountCode = ADM.AccountCode
    """

    STEP_12_SQL = """
        SELECT STEP11.Environment, STEP11.AccountCode, STEP11.AccountID,
            STEP11.FirstTrialStartDate, STEP11.FirstTrialPackage, STEP11.FirstTrialFinalDate,
            STEP11.FirstCommStartDate, STEP11.FirstCommFinalDate, STEP11.FirstCommPackage,
            STEP11.HadTrial, STEP11.HadCommContract,
            STEP11.LatestCommPackage, STEP11.LatestCommStartDate, STEP11.LatestCommFinalDate,
            STEP11.LatestTrialPackage, STEP11.LatestTrialStartDate, STEP11.LatestTrialFinalDate,
            STEP11.CommLifetimeMonths,
            STEP11.LatestMRR,
            STEP11.LatestNumberOfAvailableAdmins, STEP11.LatestNumberOfArchivedAdmins,STEP11.LatestNumberOfUsers, STEP11.LatestNumberPayingUsers, STEP11.LatestNumberFreeUsers,
            FCD.RequestedFullCancellation,
            FCD.FullCancellationRequestDate
        FROM

        (""" + STEP_11_SQL + """) STEP11

        LEFT JOIN
        (
            SELECT  
                Environment
                ,AccountCode
                ,FullCancellationRequestDate
                ,RequestedFullCancellation 
                FROM (
                    SELECT  
                        ROW_NUMBER () OVER (PARTITION BY Environment, AccountCode order by EventDate desc, ContractNumber desc) as #ROW
                        ,Environment
                        ,AccountCode
                        ,CancellationDate as FullCancellationRequestDate
                        ,CASE WHEN CancellationDate IS NULL THEN 0 ELSE 1 END AS RequestedFullCancellation
                    FROM [domain].[Contracts]
                    WHERE 
                        LinePackage = 1
                        AND InflowOutflow = 'Inflow'
                ) AS SUB where #ROW = 1
        ) FCD
        
        ON STEP11.Environment = FCD.Environment AND STEP11.AccountCode = FCD.AccountCode
    """
    STEP_13_SQL = """
        SELECT STEP12.Environment, STEP12.AccountCode, STEP12.AccountID,
            STEP12.FirstTrialStartDate, STEP12.FirstTrialPackage, STEP12.FirstTrialFinalDate,
            STEP12.FirstCommStartDate, STEP12.FirstCommFinalDate, STEP12.FirstCommPackage,
            STEP12.HadTrial, STEP12.HadCommContract,
            STEP12.LatestCommPackage, STEP12.LatestCommStartDate, STEP12.LatestCommFinalDate,
            STEP12.LatestTrialPackage, STEP12.LatestTrialStartDate, STEP12.LatestTrialFinalDate,
            STEP12.CommLifetimeMonths,
            STEP12.LatestMRR,
            STEP12.LatestNumberOfAvailableAdmins, STEP12.LatestNumberOfArchivedAdmins, STEP12.LatestNumberOfUsers, STEP12.LatestNumberPayingUsers, STEP12.LatestNumberFreeUsers,
            STEP12.RequestedFullCancellation, STEP12.FullCancellationRequestDate,
            CHD.Churned
        FROM

        (""" + STEP_12_SQL + """) STEP12

        LEFT JOIN
        (
            SELECT
                Environment
                , AccountCode
                , (CASE WHEN MaxFinalDate >= GETDATE() THEN 0 ELSE 1 END) AS Churned
                FROM
                (
                    SELECT 
                        Environment
                        , AccountCode
                        , MAX(FinalDate) AS MaxFinalDate
                    FROM 
                        [domain].[Contracts]
                    GROUP BY
                        Environment,
                        AccountCode
                ) AS FinalDate
        ) CHD

        ON STEP12.Environment = CHD.Environment AND STEP12.AccountCode = CHD.AccountCode
    """

    STEP_14_SQL = """
        SELECT STEP13.Environment, STEP13.AccountCode, STEP13.AccountID,
            STEP13.FirstTrialStartDate, STEP13.FirstTrialPackage, STEP13.FirstTrialFinalDate,
            STEP13.FirstCommStartDate, STEP13.FirstCommFinalDate, STEP13.FirstCommPackage,
            STEP13.HadTrial, STEP13.HadCommContract,
            STEP13.LatestCommPackage, STEP13.LatestCommStartDate, STEP13.LatestCommFinalDate,
            STEP13.LatestTrialPackage, STEP13.LatestTrialStartDate, STEP13.LatestTrialFinalDate,
            STEP13.CommLifetimeMonths,
            STEP13.LatestMRR,
            STEP13.LatestNumberOfAvailableAdmins, STEP13.LatestNumberOfArchivedAdmins, STEP13.LatestNumberOfUsers, STEP13.LatestNumberPayingUsers, STEP13.LatestNumberFreeUsers,
            STEP13.RequestedFullCancellation, STEP13.FullCancellationRequestDate,
            STEP13.Churned,
            LA.AccountantLinked,
            LA.DateFirstLinkedToAccountant
        FROM

        (""" + STEP_13_SQL + """) STEP13

        LEFT JOIN
        (
            SELECT
                ACS.Environment
                , ACS.AccountCode
                , MAX(CASE WHEN EntrepreneurAccountantLinked = 'EntrepreneurWithAccountant' THEN 1 ELSE 0 END) AS AccountantLinked
                , MAX(CASE 
                    WHEN EntrepreneurAccountantLinked = 'EntrepreneurWithAccountant' AND FirstCommStartDate >= '2016-02-09' AND LinkStatus = 'Accountant Linked' THEN [Date]
                    WHEN EntrepreneurAccountantLinked = 'EntrepreneurWithAccountant' AND FirstCommStartDate >= '2016-02-09' AND LinkStatus IS NULL THEN FirstCommStartDate 		
                    WHEN EntrepreneurAccountantLinked = 'EntrepreneurWithoutAccountant' AND FirstCommStartDate >= '2016-02-09' AND LinkStatus = 'Accountant Linked' THEN [Date]		-- This includes the DateFirstLinkedToAccountant for customers that have since removed the link
                    ELSE NULL END
                    ) AS DateFirstLinkedToAccountant					-- This only looks at commercial contracts. There are trial accounts that link to an accountant, but these tend to be the student accounts, or trials accounts that link to the Exacdemy Accountant
            FROM (""" + STEP_13_SQL + """) ACS
                
            INNER JOIN [domain].[Accounts]
                ON ACS.Environment = Accounts.Environment
                AND ACS.AccountCode = Accounts.AccountCode
            
            LEFT JOIN	
            (
                SELECT 
                    Environment
                    , AccountCode
                    , LinkStatus
                    , [Date]
                    , ROW_NUMBER() OVER(PARTITION BY Environment, AccountCode ORDER BY [Date]) AS RowNumber
                FROM
                    [domain].[LinkedAccountantLog]
                WHERE
                    LinkStatus = 'Accountant Linked'
            ) LAL
            ON ACS.Environment = LAL.Environment
                AND ACS.AccountCode = LAL.AccountCode
                AND RowNumber = 1								-- RowNumber 1 is the first date that the customer linked to an accountant. Customers can change between accountants, but we are most interested in when they first link

            GROUP BY
                ACS.Environment,
                ACS.AccountCode
        ) LA

        ON STEP13.Environment = LA.Environment AND STEP13.AccountCode = LA.AccountCode
    """