import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
# Additional Lib for Gender Mapping
from itertools import chain
from pyspark.sql.functions import create_map, lit
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "acg-mlspec-userdata", table_name = "put_record_python_kinesis_cfn", transformation_ctx = "datasource0")

# Gender Mapping Transformation 
df = datasource0.toDF()
gender_dict = { 'male': 1, 'female':0 }
mapping_expr = create_map([lit(x) for x in chain(*gender_dict.items())])
df = df.withColumn('gender', mapping_expr[df['gender']])
datasource_transformed = DynamicFrame.fromDF(df, glueContext, "datasource0")

applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("first", "string", "first", "string"), ("last", "string", "last", "string"), ("age", "int", "age", "int"), ("gender", "string", "gender", "string"), ("latitude", "double", "latitude", "double"), ("longitude", "double", "longitude", "double")], transformation_ctx = "applymapping1")

datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": "s3://mlspec-acg-output"}, format = "csv", transformation_ctx = "datasink2")
job.commit()