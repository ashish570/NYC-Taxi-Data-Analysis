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
from pyspark.sql.types import StructField, StructType, StringType, IntegerType,TimestampType,BooleanType,DecimalType
yellow_taxi_schema = StructType([
  StructField("VendorID",        IntegerType(), True)
  ,StructField("tpep_pickup_datetime", TimestampType(), True)
  ,StructField("tpep_dropoff_datetime",TimestampType(),True)
  ,StructField("passenger_count", IntegerType(),True)
  ,StructField("trip_distance", IntegerType(),True)
  ,StructField("RatecodeID", IntegerType(),True)
  ,StructField("store_and_fwd_flag", BooleanType(),True)
  ,StructField("PULocationID", IntegerType(), True)
  ,StructField("DOLocationID", IntegerType(), True)
  ,StructField("payment_type", IntegerType(), True)
  ,StructField("fare_amount", IntegerType(), True)
  ,StructField("extra", IntegerType(), True)
  ,StructField("mta_tax", IntegerType(), True)
  ,StructField("tip_amount", DecimalType(), True)
  ,StructField("tolls_amount", IntegerType(), True)
  ,StructField("improvement_surcharge", IntegerType(), True)
  ,StructField("total_amount", IntegerType(), True)
  ,StructField("congestion_surcharge", IntegerType(), True)
])


# COMMAND ----------

# DBTITLE 1,Loading dataframes and transform
r_df =spark.read.format("csv").options(header='true').load('/FileStore/tables/yellow_tripdata_2020_05.csv') 
df = spark.read.format("csv").options(header='true').schema(yellow_taxi_schema).load('/FileStore/tables/yellow_tripdata_2020_05.csv')
df = df.dropDuplicates()
df.createOrReplaceTempView("yellowTaxidata")
r_df.createOrReplaceTempView("RawView")

# COMMAND ----------


df.write.format("delta").save("/TaxiData")
spark.sql("CREATE TABLE YellowTaxi USING DELTA LOCATION '/TaxiData'")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from yellowTaxidata  order by 1 limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from RawView  order by 1 limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from YellowTaxi

# COMMAND ----------


