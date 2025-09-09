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

# Read table from Glue Catalog
locations_df = glueContext.create_dynamic_frame.from_catalog(database="tfm_db", table_name="other_locations_csv")

# Convert to DataFrames Spark
locations_spark_df = locations_df.toDF()

# ---------------------------------------------------------------------------------------------------------------------------------------------------
# TRANSFORMATIONS
# Calculation of latitude and longitude centroid for each state
centroids_df = locations_spark_df \
    .groupBy("statename") \
    .agg({"latitude": "avg", "longitude": "avg"}) \
    .withColumnRenamed("avg(latitude)", "centroid_latitude") \
    .withColumnRenamed("avg(longitude)", "centroid_longitude")
    
# Change uppercase format from statename to initcap in locations
locations_spark_fix_df = locations_spark_df \
    .withColumn("statename", initcap(col("statename")))

# ---------------------------------------------------------------------------------------------------------------------------------------------------

# Convert each to DynamicFrame
locations_dyf = DynamicFrame.fromDF(locations_spark_fix_df, glueContext, "locations_dyf")
centroids_dyf = DynamicFrame.fromDF(centroids_df, glueContext, "centroids_dyf")

# Define base path
base_path = "s3://sonia-uned-tfm-bucket/processed-data/"

# Clean folders before writing
s3 = boto3.resource('s3')
bucket = s3.Bucket('sonia-uned-tfm-bucket')

def empty_folder(prefix):
    objects_to_delete = [{'Key': obj.key} for obj in bucket.objects.filter(Prefix=prefix)]
    if objects_to_delete:
        bucket.delete_objects(Delete={'Objects': objects_to_delete})

empty_folder("processed-data/locations/")
empty_folder("processed-data/centroids/")

# Writing each dataframe to Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=centroids_dyf,
    connection_type="s3",
    connection_options={"path": f"{base_path}centroids/"},
    format="parquet"
)

glueContext.write_dynamic_frame.from_options(
    frame=locations_dyf,
    connection_type="s3",
    connection_options={"path": f"{base_path}locations/"},
    format="parquet"
)

job.commit()
