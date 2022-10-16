from fraser.bootstrap import Bootstrap
from pyspark.sql.types import StringType
from pyspark.sql.functions import desc, lit, udf, corr, when
from pyspark.mllib.stat import Statistics
from pyspark.ml.stat import Correlation


b = Bootstrap()
spark = b.get_spark()


# titanic_data_file='s3a://tonyfraser-aws/titanic/titanic.csv'
titanic_data_file='./data/titanic.csv'


def get_title(_s:str):
    return _s.split(',')[1].split('.')[0].strip() + '.'
def get_last_name(_s:str):
    return _s.split(',')[0]
def get_first_name(_s:str):
    return _s.split(',')[1].split('.')[1].strip()
title = udf(lambda s: get_title(s), StringType())
lname = udf(lambda s: get_last_name(s), StringType())
fname = udf(lambda s: get_first_name(s), StringType())
titanic_r = spark.read.option("header", True). \
    option("inferSchema", True). \
    csv(titanic_data_file). \
    withColumnRenamed('Sex', "StringVersionOfSex")

titanic = titanic_r. \
    withColumn('ship', lit('titanic')). \
    withColumn('lname', lname(titanic_r.Name)). \
    withColumn('fname', fname(titanic_r.Name)). \
    withColumn('title', title(titanic_r.Name)). \
    withColumn('sex', when(titanic_r.StringVersionOfSex == 'male', lit(0)).otherwise(lit(1))). \
    drop("Name", "StringVersionOfSex")

# get correlation between columns
titanic.toPandas().corr()



lusitania_data_file='./data/lusitania_crew_manifest_tab_delim.txt'
lusitania_r = spark.read.option("header", True). \
    option("delimiter", "\t"). \
    option("inferSchema", True). \
    csv(lusitania_data_file)


lusitania = lusitania_r


#lets see if there's correlation between title and survived on titanic.
titanic.groupBy('title').count().orderBy(desc('count')).show(100, False)
lusitania.groupBy("Fate").count().orderBy(desc('count')).show()


titanic.printSchema()
lusitania.printSchema()

lusitania.groupBy("Citizenship").count().orderBy(desc("count")).show(10, False) 

#all survivers between 21 and 25

titanic. \
    filter(titanic.Survived == 1). \
    where(titanic.Age.between(20,25)) \
    .show(5, True)


lusitania.limit(5).show()

lusitania.groupBy("Department/Class").count().orderBy(desc('count')).show(100, False)



# In [2]: from pyspark.sql import SparkSession
#    ...:
#    ...: spark = SparkSession.builder\
#    ...:            .config('spark.jars.packages', 'org.xerial:sqlite-jdbc:3.34.0')\
#    ...:            .getOrCreate()
#    ...:
#    ...: df = spark.read.format('jdbc') \
#    ...:         .options(driver='org.sqlite.JDBC', dbtable='customers',
#    ...:                  url='jdbc:sqlite:data/chinook.db')\
#    ...:         .load()