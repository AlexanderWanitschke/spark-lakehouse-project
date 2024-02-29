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

# Script generated for node customer curated
customercurated_node1709149694285 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="customercurated_node1709149694285",
)

# Script generated for node step trainer landing
steptrainerlanding_node1709149698899 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="steptrainerlanding_node1709149698899",
)

# Script generated for node right join
rightjoin_node1709149740091 = Join.apply(
    frame1=steptrainerlanding_node1709149698899,
    frame2=customercurated_node1709149694285,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="rightjoin_node1709149740091",
)

# Script generated for node Drop customer fields
Dropcustomerfields_node1709149849377 = DropFields.apply(
    frame=rightjoin_node1709149740091,
    paths=[],
    transformation_ctx="Dropcustomerfields_node1709149849377",
)

# Script generated for node step trainer trusted
steptrainertrusted_node1709149861056 = glueContext.write_dynamic_frame.from_catalog(
    frame=Dropcustomerfields_node1709149849377,
    database="stedi",
    table_name="step_trainer_trusted",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
    },
    transformation_ctx="steptrainertrusted_node1709149861056",
)

job.commit()

