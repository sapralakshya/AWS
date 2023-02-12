import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
cardf=glueContext.create_dynamic_frame.from_catalog(
    database="test_json_db",
    table_name="inbox",
    transformation_ctx="s3_car_new_json_data"
    )
Cardf=cardf.toDF()
cardynamodf=glueContext.create_dynamic_frame.from_options(
    connection_type="dynamodb",
    connection_options={"dynamodb.input.tableName" : "test_data_pipeline_table","dynamodb.throughput.read.percent" : "1.0","dynamodb.splits" : "100"}
    )
newdynamodf=cardynamodf.toDF()
resultdf=None
if newdynamodf.count()!=0:
    print("dynamodb table is not empty")
    newDynamoDfRenamed = newdynamodf.select("car_name").withColumnRenamed("car_name","new_car_name")
    joineddf=Cardf.join(newDynamoDfRenamed,Cardf.car_name == newDynamoDfRenamed.car_name,"left")
    print("Number of records after join = ", joineddf.count())
    resultdf=joineddf.filter("new_car_name is null")
    resultdf.drop("new_car_name")
else:
    print("dynamodb table is empty")
    resultdf=Cardf
resultdynamicdf=DynamicFrame.fromDF(resultdf,glueContext,"resultdf")
try:
    glueContext.write_dynamic_frame_from_options(
            frame=resultdynamicdf,
            connection_type="dynamodb",
            connection_options={"dynamodb.output.tableName": "test_data_pipeline_table",
                "dynamodb.throughput.write.percent": "1.0"
            }
        )
    print("Data write in dynamodb is successful")
except Exception as err:
    print("Error is:", str(err))
job.commit()
