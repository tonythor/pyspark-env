from fraser.bootstrap import Bootstrap

b = Bootstrap()
spark = b.get_spark()


from pyspark.sql.functions import desc

# titanic_data_file='s3a://tonyfraser-aws/titanic/titanic.csv'

titanic_data_file='./data/titanic.csv'
lusitania_data_file='./data/lusitania_crew_manifest_tab_delim.txt'

titanic = spark.read.option("header", True). \
    option("inferSchema", True). \
    csv(titanic_data_file)

lusitania = spark.read.option("header", True). \
    option("delimiter", "\t"). \
    option("inferSchema", True). \
    csv(lusitania_data_file)

titanic.printSchema()
lusitania.printSchema()

lusitania.groupBy("Citizenship").count().orderBy(desc("count")).show(10, False) 

#all survivers between 21 and 25

titanic. \
    filter(titanic.Survived == 1). \
    where(titanic.Age.between(20,25)) \
    .show(5, True)



