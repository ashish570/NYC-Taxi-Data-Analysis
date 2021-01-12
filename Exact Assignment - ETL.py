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

# DBTITLE 1,Downloading files to local storage in RawZone
# MAGIC %scala
# MAGIC import sys.process._
# MAGIC 
# MAGIC var base_url :String = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-"
# MAGIC var rawFileLoc = "dbfs:/FileStore/tables/rawFiles/2020/yellow_tripdata_2020"
# MAGIC var baseFileName = "yellow_tripdata_2020-"
# MAGIC for (month <- 1 to 6)
# MAGIC     {
# MAGIC         var monthStr :String= month.toString
# MAGIC         if (monthStr.length == 1)
# MAGIC           {
# MAGIC             monthStr = ("0".toString).concat(monthStr)
# MAGIC           }
# MAGIC         
# MAGIC     var download_cmd = "wget -P /tmp "+base_url + monthStr + ".csv"
# MAGIC       //Downloading the files using below command for each month  
# MAGIC       download_cmd !!
# MAGIC     var localpath="file:/tmp/"+baseFileName+monthStr + ".csv"
# MAGIC     var filePath = rawFileLoc+"-"+monthStr+".csv" 
# MAGIC     dbutils.fs.cp(localpath, filePath)
# MAGIC     }

# COMMAND ----------

# DBTITLE 1,Required Library imports
from pyspark.sql.types import StructField, StructType, StringType, IntegerType,TimestampType,BooleanType,DecimalType,LongType,DoubleType
from pyspark.sql.functions import round,year,to_timestamp


# COMMAND ----------

# DBTITLE 1,Defining Schema
yellow_taxi_schema = StructType([
  StructField("VendorID",        IntegerType(), True)
  ,StructField("tpep_pickup_datetime", TimestampType(), True)
  ,StructField("tpep_dropoff_datetime",TimestampType(),True)
  ,StructField("passenger_count", IntegerType(),True)
  ,StructField("trip_distance", DoubleType(),True)
  ,StructField("RatecodeID", IntegerType(),True)
  ,StructField("store_and_fwd_flag", BooleanType(),True)
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

dbutils.fs.rm('FileStore/tables/processedFiles/2020',True)

# COMMAND ----------

# DBTITLE 1,Loading dataframes and transformations
#reading yellowTaxi Data files into a dataframe
df = spark.read.format("csv").options(header='true').schema(yellow_taxi_schema).load('/FileStore/tables/rawFiles/2020/*')

# dropping duplicates
df = df.dropDuplicates()

# finding out datediffernce in hours for pickup and drop off date
datediff_df = df.withColumn("hours",(df["tpep_dropoff_datetime"].cast(LongType()) - df["tpep_pickup_datetime"].cast(LongType())))
datediff_df = datediff_df.withColumn("hours_diff",round(datediff_df["hours"]/3600,2))
datediff_df = datediff_df.withColumn("Year",year(datediff_df.tpep_dropoff_datetime))
datediff_df = datediff_df.filter(datediff_df.Year =='2020')
datediff_df.persist()
datediff.count()

#creating tempview for analysis
datediff_df.createOrReplaceTempView("yellowTaxidata")

#Writing dataframe to parquet
#datediff_df.write.mode("append").parquet("/FileStore/tables/processedFiles/2020/yellow_tripdata.parquet")

# COMMAND ----------

datediff_df.count()

# COMMAND ----------

ddf = spark.read.format("csv").options(header='true').schema(yellow_taxi_schema).load('/FileStore/tables/rawFiles/2020/*')
ddf.dropDuplicates()
ddf = ddf.withColumn("year",year(ddf.tpep_pickup_datetime))
display(ddf)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tpep_pickup_datetime
# MAGIC 	,tpep_dropoff_datetime
# MAGIC 	,hours_diff
# MAGIC 	,trip_distance
# MAGIC 	,DOLocationID
# MAGIC 	,total_amount
# MAGIC     ,
# MAGIC FROM yellowTaxidata
# MAGIC WHERE 1 = 1
