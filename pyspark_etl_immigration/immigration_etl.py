from pyspark.sql import SparkSession
import  pyspark.sql.functions as F
from pyspark.sql.functions import col, split, udf
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql.functions import desc
import datetime as dt

def date_converter(x):
    try:
        temp = dt.datetime(1960, 1, 1).date()
        return temp + dt.timedelta(days=int(x))
    except:
        return None

    
def immigration_etl(source="s3://immigration-us-1/sas_data",
                    destination="s3://immigration-us-1/sas_data_ready_to_use",
                    country_dict="s3://immigration-us-1/raw_data/country_dict.csv",
                    visa_dict="s3://immigration-us-1/raw_data/visa_dict.csv"
                   ):
    # Local udfs
    
    spaceDeleteUDF = udf(lambda s: s.replace(" ", ""), Str())
    ampesandDeleteUDF = udf(lambda s: s.replace("'", ""), Str())
    udf_to_datetime_sas = udf(lambda x: date_converter(x), DateType())
   
    Schema_country = R([
        Fld("id",Str()),
        Fld("country",Str())
        ])
    Schema_Visa = R([
        Fld("id",Str()),
        Fld("Visa_Type",Str())
        ])

    df_spark=spark.read.parquet(source)
    # Only immigrants from air 
    immigrants=df_spark.where(F.col("i94mode")==1)
    immigrants=immigrants.select("cicid", "i94yr", "i94mon", "i94cit", "i94res", "i94port", "arrdate", "i94visa",  "biryear", "gender", "visatype", "airline")
    country_dict=spark.read.csv(country_dict, header=True, mode="DROPMALFORMED", sep="=",schema=Schema_country)
    
    immigrants = immigrants.join(country_dict, immigrants.i94cit == country_dict.id,how='right') 
    immigrants=immigrants.withColumnRenamed("country", "cit_country")
    immigrants=immigrants.drop('id','i94cit')
    
    immigrants = immigrants.join(country_dict, immigrants.i94res == country_dict.id,how='right') 
    immigrants=immigrants.withColumnRenamed("country", "res_country")
    immigrants=immigrants.drop('id','i94res')
    
    visa_dict=spark.read.csv(visa_dict, header=False, mode="DROPMALFORMED",sep="=", schema=Schema_Visa)
    immigrants = immigrants.join(visa_dict, immigrants.i94visa == visa_dict.id,how='right') 
    immigrants=immigrants.drop('id','i94visa')
    
    immigrants=immigrants.withColumn("arrdate", udf_to_datetime_sas("arrdate"))
    
    immigrants = immigrants.withColumn("cicid", immigrants["cicid"].cast(IntegerType()))
    immigrants = immigrants.withColumn("i94yr", immigrants["i94yr"].cast(IntegerType()))
    immigrants = immigrants.withColumn("biryear", immigrants["biryear"].cast(IntegerType()))
    immigrants = immigrants.withColumn("i94mon", immigrants["i94mon"].cast(IntegerType()))
    
    immigrants.show(10) 
    immigrants.write.parquet(destination)
   
    
immigration_etl()   