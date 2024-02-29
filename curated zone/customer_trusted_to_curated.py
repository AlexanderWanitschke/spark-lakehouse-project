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

# Script generated for node keep only distinct customers and their data
SqlQuery0 = """
select distinct(customername), email, phone, birthday,serialnumber,registrationdate, lastupdatedate,sharewithresearchasofdate, sharewithpublicasofdate from myDataSource;
"""
keeponlydistinctcustomersandtheirdata_node1709148988433 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": innerjoinonemailuser_node1709146587987},
    transformation_ctx="keeponlydistinctcustomersandtheirdata_node1709148988433",
)

# Script generated for node customer curated
customercurated_node1709148808004 = glueContext.write_dynamic_frame.from_catalog(
    frame=keeponlydistinctcustomersandtheirdata_node1709148988433,
    database="stedi",
    table_name="customer_curated",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
    },
    transformation_ctx="customercurated_node1709148808004",
)

job.commit()

