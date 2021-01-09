# Databricks notebook source
# DBTITLE 1,Summary of Assignment

"""
1.Download the dataset from (https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page), there are two ways to achieve this :
  i. through webcrawling, which enables used to go to a perticular website and look for a perticular content.
  ii. by providing directly the url in databricks noteboook to supply the location of the file(from and FTP server, or other location)
Once downloaded the data should be stored in one of the stroge on cloud, be it blob, or ADLS in rawfile format.
2. Load this rawdata into pyspark dataframes and process according to ETL rules supplied in the assignment.
3. Store the results of the transformation into a location preferably deltatable and supply it through the rest end point to the end user.
"""


# COMMAND ----------

# DBTITLE 1,Defining Schema
from pyspark.sql.types import StructField, StructType, StringType, IntegerType,TimestampType,BooleanType,DoubleType

yellow_taxi_schema = StructType([
  StructField("VendorID",        IntegerType(), True)
  ,StructField("tpep_pickup_datetime", TimestampType(), True)
  ,StructField("tpep_dropoff_datetime",TimestampType(),True)
  ,StructField("passenger_count", IntegerType(),True)
  ,StructField("trip_distance", DoubleType(),True)
  ,StructField("RatecodeID", IntegerType(),True)
  ,StructField("store_and_fwd_flag", StringType(),True)
  ,StructField("PULocationID", IntegerType(), True)
  ,StructField("DOLocationID", IntegerType(), True)
  ,StructField("payment_type", IntegerType(), True)
  ,StructField("fare_amount", DoubleType(), True)
  ,StructField("extra", DoubleType(), True)
  ,StructField("mta_tax", DoubleType(), True)
  ,StructField("tip_amount", DoubleType(), True)
  ,StructField("tolls_amount", DoubleType(), True)
  ,StructField("improvement_surcharge", DoubleType(), True)
  ,StructField("total_amount", DoubleType(), True)
  ,StructField("congestion_surcharge", DoubleType(), True)
])

# COMMAND ----------

r_df =spark.read.format("csv").options(header='true').load('/FileStore/tables/yellow_tripdata_2020_01.csv') 
df = spark.read.format("csv").options(header='true').schema(yellow_taxi_schema).load('/FileStore/tables/yellow_tripdata_2020_01.csv')
df = df.dropDuplicates()
df.createOrReplaceTempView("yellowTaxiData")
r_df.createOrReplaceTempView("RawView")

# COMMAND ----------

# MAGIC %sql
# MAGIC --I would like to calculate the difference of date in hours to calculate the speed of a taxi during the ride in miles per hour(MPH)
# MAGIC 
# MAGIC select tpep_pickup_datetime as long_pickup_time
# MAGIC         ,tpep_dropoff_datetime
# MAGIC         ,cast(datediff(tpep_dropoff_datetime,tpep_pickup_datetime)/60 as decimal(2,2)) as trip_duration
# MAGIC         ,trip_distance 
# MAGIC from yellowTaxiData
# MAGIC order by 1,2 
# MAGIC limit 10;

# COMMAND ----------

df.write.format("delta").save("/TaxiData")
spark.sql("CREATE TABLE YellowTaxi USING DELTA LOCATION '/TaxiData'")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from RawView order by 2,3

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, DoubleType,IntegerType
data2 = [
    (15.3,2,2020),
    (5.0 ,12,2020),
    (14.3,4,2020),
    (2.88,1,2020)
    
  ]

schema = StructType([ StructField("tipAmount", DoubleType(), True),StructField("Month", IntegerType(), True),StructField("Year", IntegerType(), True)
                     ])
sd = spark.createDataFrame(data = data2,schema = schema)
sd.createOrReplaceTempView("sampleView")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from sampleView order by 2,1

# COMMAND ----------


