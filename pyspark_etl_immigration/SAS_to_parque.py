
from pyspark.sql import SparkSession
spark = SparkSession.builder.\
config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
.enableHiveSupport().getOrCreate()
df_spark =spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')

#write to parquet
df_spark.write.parquet("s3://immigration-us-1/sas_data")

#test 
df_spark=spark.read.parquet("s3://immigration-us-1/sas_data")
df_spark.head(1)