import sys
from utils import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import when, lit, udf, col, concat, trim, row_number, upper, lower
from awsglue.utils import getResolvedOptions
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, BooleanType, TimestampType, StringType
import hashlib

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'raw_db', 'raw_contractstatistics_corpbi_table',
                                     'raw_accounts_table', 's3_destination'])


def account_hash(account_id):
    return hashlib.sha1('{' + account_id.lower() + '}').hexdigest().decode("hex").encode("base64").lower()


def blank_key_fields(chamber_of_commerce, vatnumber, postcode, sectorcode, compsizecode, bustypecode):
    blank_count = 0
    if chamber_of_commerce is None:
        blank_count += 1
    if vatnumber is None:
        blank_count += 1
    if postcode is None:
        blank_count += 1
    if sectorcode is None:
        blank_count += 1
    if compsizecode is None:
        blank_count += 1
    if bustypecode is None:
        blank_count += 1
    return blank_count


blank_keyfields_udf = udf(blank_key_fields, IntegerType())
account_hash_udf = udf(lambda id: account_hash(id), StringType())

dateimenow = datetime.now()
year = dateimenow.year
month = dateimenow.month
day = dateimenow.day

filter = 'ingestionyear = ' + str(year) + ' and ingestionmonth = ' + str(month) + ' and ingestionday =' + str(day)

glueContext = GlueContext(SparkContext.getOrCreate())

contractstatistics_df = glueContext.create_dynamic_frame.from_catalog(database=args['raw_db'], table_name=args[
    'raw_contractstatistics_corpbi_table'], push_down_predicate=filter).toDF()
accounts_df = glueContext.create_dynamic_frame.from_catalog(database=args['raw_db'], table_name=args['raw_accounts_table']).toDF()

contractstatistics_df = cleanDataFrame(contractstatistics_df, to_upper_cols = ['environment'])

contractstatistics_df = convertDataTypes(
    data_frame = contractstatistics_df,
    timestamp_cols = ['lastmodified'],
    boolean_cols = ['meta_isdeleted']
)

window_spec =  Window.partitionBy(col('environment'), col('code')).orderBy(col('lastmodified').desc())

contractstatistics_df = contractstatistics_df.where((col('meta_load_enddts').isNull()) & (col('meta_isdeleted') == False) & (col('code') != '162697700000000000'))
contractstatistics_df = contractstatistics_df.withColumn("row_num", row_number().over(window_spec))
contractstatistics_df = contractstatistics_df.where(col('row_num') == 1)
raw_contractstatistics_cols = ['environment', 'code', 'account', 'name', 'isaccountant',
                               'accountantorlinked', 'entrepreneuraccountantlinked', 'accountantcode',
                               'chamberofcommerce', 'vatnumber', 'postcode', 'city', 'state',
                               'country', 'sectorcode', 'subsectorcode', 'compsizecode', 'bustypecode',
                               'accountclassificationcode', 'accountmanagercode', 'accountmanageraccountantcode',
                               'resellercode', 'leadsourcecode', 'blocked']

contractstatistics_df = contractstatistics_df.select(raw_contractstatistics_cols)
accounts_cols = ['id','dunsnumber', 'syscreated', 'sysmodified', 'sbicode', 'databaseid', 'maincontact', 'maindivision',
                 'isanonymized', 'leadpurpose', 'anonymisationsource', 'invoiceaccount']

accounts_df = accounts_df.select(accounts_cols)
accounts_df = accounts_df.withColumnRenamed('id','account')
accounts_df = accounts_df.withColumnRenamed('invoiceaccount','invoicedebtor')

domain_accounts = contractstatistics_df.join(accounts_df, 'account', 'left')
if domain_accounts.count() > 0 :
    domain_accounts = domain_accounts.withColumn('environment',
                                                 when(col('environment') == 'GB', 'UK')
                                                 .otherwise(col('environment')))
    domain_accounts = domain_accounts.withColumn(
        'numberofblankkeyfields',
        blank_keyfields_udf(col('chamberofcommerce'), col('vatnumber'), col('postcode'), col('sectorcode'), col('compsizecode'), col('bustypecode')))

    domain_accounts = domain_accounts.withColumn("accountidhash", account_hash_udf('account').substr(1, 28))
    domain_accounts = domain_accounts.withColumnRenamed('code', 'accountcode')
    domain_accounts = domain_accounts.withColumnRenamed('account', 'accountid')

    domain_accounts = convertDataTypes(
        data_frame = domain_accounts,
        timestamp_cols = ['syscreated','sysmodified'],
        boolean_cols = ['blocked','accountantorlinked'],
        integer_cols = ['maindivision','isanonymized','anonymisationsource','accountmanagercode','accountmanageraccountantcode']
    )

    domain_accounts = domain_accounts.withColumn('customerprofileurl', concat(lit('https://app.powerbi.com/groups/me/apps/ef46f53e-fb11-443c-8c44-c648fa633c42/reports/3ac72a9a-8c9b-4b9c-93dd-1d70ebd9946c/ReportSection1?filter=Accounts%252FAccountID%20eq%20%27'), col('accountid'), lit('%27')))
    domain_accounts = domain_accounts.withColumn("eolcardurl",
                                                 when(col('environment') == 'NL', concat(lit('https://start.exactonline.nl/docs/LicCustomerCard.aspx?AccountID=%7b'), lower(col('accountid')), lit('%7d&_Division_=1')))
                                                 .when(col('environment') == 'BE', concat(lit('https://start.exactonline.be/docs/LicCustomerCard.aspx?AccountID=%7b'), lower(col('accountid')), lit('%7d&ControlAccountVatNumber=False&ControlAccountCocNumber=False&DoPurchaseAccountData=False&_Division_=1')))
                                                 .when(col('environment') == 'FR', concat(lit('https://start.exactonline.fr/docs/LicCustomerCard.aspx?AccountID=%7b'), lower(col('accountid')), lit('%7d&_Division_=1')))
                                                 .when(col('environment') == 'UK', concat(lit('https://start.exactonline.co.uk/docs/LicCustomerCard.aspx?AccountID=%7b'), lower(col('accountid')), lit('%7d&_Division_=1')))
                                                 .when(col('environment') == 'US', concat(lit('https://start.exactonline.com/docs/LicCustomerCard.aspx?Action=0&AccountID=%7b'), lower(col('accountid')), lit('%7d&ControlAccountVatNumber=False&ControlAccountCocNumber=False&DoPurchaseAccountData=False&_Division_=1')))
                                                 .when(col('environment') == 'ES', concat(lit('https://start.exactonline.es/docs/LicCustomerCard.aspx?AccountID=%7b'), lower(col('accountid')), lit('%7d')))
                                                 .when(col('environment') == 'DE', concat(lit('https://start.exactonline.de/docs/LicCustomerCard.aspx?AccountID=%7b'), lower(col('accountid')), lit('%7d&_Division_=1')))
                                                 .otherwise(lit('%7d')))
    domain_accounts.repartition(1).write.mode("overwrite").parquet(args['s3_destination'])
