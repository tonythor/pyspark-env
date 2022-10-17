from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import desc, lit, udf, corr, when, lower

# Make sure you have the latest driver! 
spark = SparkSession.builder.config('spark.jars.packages', 'org.xerial:sqlite-jdbc:3.39.3.0').getOrCreate()


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType
employee_schema = StructType([
            StructField("EmployeeId",IntegerType()),
            StructField("LastName",StringType()),
            StructField("FirstName",StringType()),
            StructField("Title",StringType()),
            StructField("ReportsTo",IntegerType()),
            StructField("BirthDate",StringType()),
            StructField("HireDate",TimestampType()),
            StructField("Address",StringType()),
            StructField("City",StringType()),
            StructField("State",StringType()),
            StructField("Country",StringType()),
            StructField("PostalCode",StringType()),
            StructField("Phone",StringType()),
            StructField("Fax",StringType()),
            StructField("Email",StringType()),
            ])

# Schema: https://www.sqlitetutorial.net/sqlite-sample-database/
employees = spark.read.format('jdbc').options(driver='org.sqlite.JDBC', dbtable='employees', url='jdbc:sqlite:data/chinook.db').load() 
customers = spark.read.format('jdbc').options(driver='org.sqlite.JDBC', dbtable='customers', url='jdbc:sqlite:data/chinook.db').load()
invoices = spark.read.format('jdbc').options(driver='org.sqlite.JDBC', dbtable='invoices', url='jdbc:sqlite:data/chinook.db').load()
invoice_items = spark.read.format('jdbc').options(driver='org.sqlite.JDBC', dbtable='invoice_items', url='jdbc:sqlite:data/chinook.db').load()
albums = spark.read.format('jdbc').options(driver='org.sqlite.JDBC', dbtable='albums', url='jdbc:sqlite:data/chinook.db').load()
playlists = spark.read.format('jdbc').options(driver='org.sqlite.JDBC', dbtable='playlists', url='jdbc:sqlite:data/chinook.db').load()
playlist_track = spark.read.format('jdbc').options(driver='org.sqlite.JDBC', dbtable='playlist_track', url='jdbc:sqlite:data/chinook.db').load()
tracks = spark.read.format('jdbc').options(driver='org.sqlite.JDBC', dbtable='tracks', url='jdbc:sqlite:data/chinook.db').load()
artists = spark.read.format('jdbc').options(driver='org.sqlite.JDBC', dbtable='artists', url='jdbc:sqlite:data/chinook.db').load()
media_types = spark.read.format('jdbc').options(driver='org.sqlite.JDBC', dbtable='media_types', url='jdbc:sqlite:data/chinook.db').load()
genres = spark.read.format('jdbc').options(driver='org.sqlite.JDBC', dbtable='genres', url='jdbc:sqlite:data/chinook.db').load()


#customers with active invoices?
customers.join(invoices, 'CustomerId', 'inner').groupBy("CustomerId", "LastName","FirstName").count().orderBy(desc('count')).show()
#59
invoices.select("CustomerId").distinct().count()
#59 -> less taxing on system.


#largest playlists? 
playlist_track.join(playlists, 'PlaylistId', 'inner').groupBy("Name", "PlaylistId").count().show()
# +--------------------+----------+-----+
# |                Name|PlaylistId|count|
# +--------------------+----------+-----+
# |               Music|         1| 3290|
# |            TV Shows|         3|  213|
# |          90’s Music|         5| 1477|
# |               Music|         8| 3290|
# |        Music Videos|         9|    1|
# |            TV Shows|        10|  213|
# |     Brazilian Music|        11|   39|
# |           Classical|        12|   75|
# |Classical 101 - D...|        13|   25|
# |Classical 101 - N...|        14|   25|
# |Classical 101 - T...|        15|   25|
# |              Grunge|        16|   15|
# | Heavy Metal Classic|        17|   26|
# |         On-The-Go 1|        18|    1|
# +--------------------+----------+-----+