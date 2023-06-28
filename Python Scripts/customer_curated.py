import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1687776164522 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://charlieudacitybucket/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1687776164522",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Join
Join_node1687776392173 = Join.apply(
    frame1=CustomerTrusted_node1687776164522,
    frame2=AccelerometerLanding_node1,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1687776392173",
)

# Script generated for node Drop Fields
DropFields_node1687776581312 = DropFields.apply(
    frame=Join_node1687776392173,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1687776581312",
)

# Script generated for node CustomerCurated
CustomerCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1687776581312,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://charlieudacitybucket/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node3",
)

job.commit()
