from fraser.bootstrap import Bootstrap
from pyspark.sql.types import StringType
from pyspark.sql.functions import desc, lit, udf, corr, when, lower
from pyspark.mllib.stat import Statistics
from pyspark.ml.stat import Correlation


b = Bootstrap()
spark = b.get_spark()

q42019_loc = "s3a://tonyfraser/g/Divvy_Trips_2019_Q4.csv"
q12020_loc = "s3a://tonyfraser/g/Divvy_Trips_2020_Q1.csv"

q42019 = spark.read.option("header", True).option("inferSchema", True).csv(q42019_loc)
q42019.printSchema() 
# root
#  |-- trip_id: integer (nullable = true)
#  |-- start_time: timestamp (nullable = true)
#  |-- end_time: timestamp (nullable = true)
#  |-- bikeid: integer (nullable = true)
#  |-- tripduration: string (nullable = true)
#  |-- from_station_id: integer (nullable = true)
#  |-- from_station_name: string (nullable = true)
#  |-- to_station_id: integer (nullable = true)
#  |-- to_station_name: string (nullable = true)
#  |-- usertype: string (nullable = true)
#  |-- gender: string (nullable = true)
#  |-- birthyear: integer (nullable = true)


q12020 = spark.read.option("header", True).option("inferSchema", True).csv(q12020_loc)
q12020.printSchema() 
# root
#  |-- ride_id: string (nullable = true)
#  |-- rideable_type: string (nullable = true)
#  |-- started_at: timestamp (nullable = true)
#  |-- ended_at: timestamp (nullable = true)
#  |-- start_station_name: string (nullable = true)
#  |-- start_station_id: integer (nullable = true)
#  |-- end_station_name: string (nullable = true)
#  |-- end_station_id: integer (nullable = true)
#  |-- start_lat: double (nullable = true)
#  |-- start_lng: double (nullable = true)
#  |-- end_lat: double (nullable = true)
#  |-- end_lng: double (nullable = true)
#  |-- member_casual: string (nullable = true)