import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import split, col, substring, initcap, when

## Initial configuration
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read tables from Glue Catalog
loan_df = glueContext.create_dynamic_frame.from_catalog(database="tfm_db", table_name="kaggle_loan_applications_csv")
transactions_df = glueContext.create_dynamic_frame.from_catalog(database="tfm_db", table_name="kaggle_transactions_csv")

# Convert to DataFrames Spark
loan_spark_df = loan_df.toDF()
transactions_spark_df = transactions_df.toDF()

# ---------------------------------------------------------------------------------------------------------------------------------------------------
# TRANSFORMATIONS
# Separate customer information into a different CSV
customers_spark_df = loan_spark_df.select(
    "customer_id", 
    "employment_status", 
    "monthly_income", 
    "cibil_score", 
    "existing_emis_monthly", 
    "debt_to_income_ratio", 
    "property_ownership_status", 
    "residential_address", 
    "applicant_age", 
    "gender", 
    "number_of_dependents"
).dropDuplicates(["customer_id"])

# Delete those columns from loan_applications (except customer_id)
loan_spark_fix_df = loan_spark_df.drop(
    "employment_status", 
    "monthly_income", 
    "cibil_score", 
    "existing_emis_monthly", 
    "debt_to_income_ratio", 
    "property_ownership_status", 
    "residential_address", 
    "applicant_age", 
    "gender", 
    "number_of_dependents")

# Separate information from "residential_address" to connect it to locations.csv
# Get PINcode from address_city and delete address_city and residential_address
address_split = split(customers_spark_df["residential_address"], ', ')

customers_spark_fix_df = customers_spark_df \
    .withColumn("address_number", address_split.getItem(0)) \
    .withColumn("address_street", address_split.getItem(1)) \
    .withColumn("address_city", address_split.getItem(2)) \
    .withColumn("address_PINcode", substring(col("address_city"), -6, 6)) \
    .drop("address_city") \
    .drop("residential_address")
    
# Separate transaction_location and remove the city part, we are left with state to connect it with locations.csv
location_split = split(transactions_spark_df["transaction_location"], ', ')

transactions_spark_fix_df = transactions_spark_df \
    .withColumn("location_city", location_split.getItem(0)) \
    .withColumn("location_state", initcap(location_split.getItem(1))) \
    .drop("location_city") \
    .drop("transaction_location")

# Change types
loan_spark_fix_df = loan_spark_fix_df \
    .withColumn("fraud_flag", col("fraud_flag").cast("boolean")) \
    .withColumn("application_date", col("application_date").cast("date"))
    
transactions_spark_fix_df = transactions_spark_fix_df \
    .withColumn("transaction_date", col("transaction_date").cast("date")) \
    .withColumn("is_international_transaction", col("is_international_transaction").cast("boolean")) \
    .withColumn("transaction_status", when(col("transaction_status") == "Failed", 0) \
                                    .otherwise(1) \
                                    .cast("boolean")) \
    .withColumn("fraud_flag", col("fraud_flag").cast("boolean")) \


# ---------------------------------------------------------------------------------------------------------------------------------------------------

# Convert to DynamicFrame
customers_dyf = DynamicFrame.fromDF(customers_spark_fix_df, glueContext, "customers_dyf")
loan_dyf = DynamicFrame.fromDF(loan_spark_fix_df, glueContext, "loan_dyf")
transactions_dyf = DynamicFrame.fromDF(transactions_spark_fix_df, glueContext, "transactions_dyf")

# Define base path
base_path = "s3://sonia-uned-tfm-bucket/processed-data/"

# Clean folders before writing
s3 = boto3.resource('s3')
bucket = s3.Bucket('sonia-uned-tfm-bucket')

def empty_folder(prefix):
    objects_to_delete = [{'Key': obj.key} for obj in bucket.objects.filter(Prefix=prefix)]
    if objects_to_delete:
        bucket.delete_objects(Delete={'Objects': objects_to_delete})

empty_folder("processed-data/customers/")
empty_folder("processed-data/loan_applications/")
empty_folder("processed-data/transactions/")
empty_folder("processed-data/customers_csv/")

# Write in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=customers_dyf,
    connection_type="s3",
    connection_options={"path": f"{base_path}customers/"},
    format="parquet"
)

glueContext.write_dynamic_frame.from_options(
    frame=loan_dyf,
    connection_type="s3",
    connection_options={"path": f"{base_path}loan_applications/"},
    format="parquet"
)

glueContext.write_dynamic_frame.from_options(
    frame=transactions_dyf,
    connection_type="s3",
    connection_options={"path": f"{base_path}transactions/"},
    format="parquet"
)

job.commit()