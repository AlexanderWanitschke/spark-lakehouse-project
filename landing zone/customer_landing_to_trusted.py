import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer landing
customerlanding_node1709139664503 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing",
    transformation_ctx="customerlanding_node1709139664503",
)

# Script generated for node keep where share with research not null
SqlQuery0 = """
select * from myDataSource where sharewithresearchasofdate is not null;
"""
keepwheresharewithresearchnotnull_node1709140365462 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": customerlanding_node1709139664503},
    transformation_ctx="keepwheresharewithresearchnotnull_node1709140365462",
)

# Script generated for node customer trusted
customertrusted_node1709139737872 = glueContext.write_dynamic_frame.from_catalog(
    frame=keepwheresharewithresearchnotnull_node1709140365462,
    database="stedi",
    table_name="customer_trusted",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
    },
    transformation_ctx="customertrusted_node1709139737872",
)

job.commit()

