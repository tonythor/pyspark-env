from fraser.bootstrap import Bootstrap

b = Bootstrap()
spark = b.get_spark()


# titanic_data_file='s3a://tonyfraser-aws/titanic/titanic.csv'
titanic_data_file="./data/titanic.csv"

titanic = spark.read.option("header", True). \
    option("inferSchema", True). \
    csv(titanic_data_file)

#all survivers between 21 and 25
titanic. \
    filter(titanic.Survived == 1). \
    where(titanic.Age.between(20,25)) \
    .show(5, True)



