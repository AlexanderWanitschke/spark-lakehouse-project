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

# Script generated for node customer trusted
customertrusted_node1709146560413 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customertrusted_node1709146560413",
)

# Script generated for node accelerometer landing
accelerometerlanding_node1709146572113 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometerlanding_node1709146572113",
)

# Script generated for node inner join on email/user
innerjoinonemailuser_node1709146587987 = Join.apply(
    frame1=customertrusted_node1709146560413,
    frame2=accelerometerlanding_node1709146572113,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="innerjoinonemailuser_node1709146587987",
)

# Script generated for node drop customer fields
dropcustomerfields_node1709147743488 = DropFields.apply(
    frame=innerjoinonemailuser_node1709146587987,
    paths=["email", "phone"],
    transformation_ctx="dropcustomerfields_node1709147743488",
)

# Script generated for node accelerometer trusted
accelerometertrusted_node1709146797165 = glueContext.write_dynamic_frame.from_catalog(
    frame=dropcustomerfields_node1709147743488,
    database="stedi",
    table_name="accelerometer_trusted",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
    },
    transformation_ctx="accelerometertrusted_node1709146797165",
)

job.commit()

