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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1687941973537 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1687941973537",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1",
)

# Script generated for node Join
Join_node1687942174127 = Join.apply(
    frame1=StepTrainerTrusted_node1,
    frame2=AccelerometerTrusted_node1687941973537,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node1687942174127",
)

# Script generated for node Drop PII
DropPII_node1687943304450 = DropFields.apply(
    frame=Join_node1687942174127,
    paths=["user"],
    transformation_ctx="DropPII_node1687943304450",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropPII_node1687943304450,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://charlieudacitybucket/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node3",
)

job.commit()
