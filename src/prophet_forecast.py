"""
# A Simple Prophet Forecast

Both inbound and outbound data is stored
in the data directory.

"""
import pandas as pd
from prophet import Prophet
from fraser.bootstrap import Bootstrap
from pyspark.sql.functions import min,max,count

df = pd.read_csv('./data/product_store.csv') # <-- this is in ths project
df['year'] = df['StringDate'].apply(lambda x: str(x)[-4:])
df['month'] = df['StringDate'].apply(lambda x: str(x)[-6:-4])
df['day'] = df['StringDate'].apply(lambda x: str(x)[:-6])
df['pandas_date']=pd.DatetimeIndex(df['year'] + '-' + df['month']+ '-' + df['day'])
df.drop(['Product', 'Store', 'StringDate','year','month','day'], axis=1, inplace=True)

# prophet uses only two columns to forecast, 
#     what you are measuring, the value, "Y", and
#     the pandas date stamp column, "DS" 
# so, let's rename them in place. 

df.columns = ['y', 'ds']
# In [9]: df.dtypes
# Out[9]:
# y            float64
# ds           datetime64[ns]
# dtype:       object


# good luck with this. In the readme I tried to cover how
# to get supporting libraries installed.

prophet = Prophet(interval_width=.95, daily_seasonality=True)

#NOTE!! Propet is not an immutable object, we are about to change it.
model = prophet.fit(df)

future = prophet.make_future_dataframe(periods=100, freq='D')
forecast = prophet.predict(future)

## If you have plotly installed and working.. 
# Shows you a little more about your data, like weeks or performance
# by day. (basically, prophet knows a lot about days)
prophet.plot_components(forecast).show()
# The graph you'd expect, an estimate of sales based on date. 
prophet.plot(forecast).show()
# Save to FS so you can see the forecat output.


## if you're more comfortable exploring with spark...
b = Bootstrap()
spark = b.get_spark()
forecastSpark = spark.createDataFrame(forecast)
dfSpark = spark.createDataFrame(df)
#looks like the same data, but just 100 days more.
dfSpark.select(min("ds").alias("min_df"), max("ds").alias("max_df"), count("ds").alias("count")).show()
forecastSpark.select(min("ds").alias("min_forecast"), max("ds").alias("max_forecast"), count("ds").alias("count")).show()

# +-------------------+-------------------+-----+
# |             min_df|             max_df|count|
# +-------------------+-------------------+-----+
# |2018-01-01 00:00:00|2020-12-16 00:00:00| 1080|
# +-------------------+-------------------+-----+

# +-------------------+-------------------+-----+
# |       min_forecast|       max_forecast|count|
# +-------------------+-------------------+-----+
# |2018-01-01 00:00:00|2021-03-26 00:00:00| 1180|
# +-------------------+-------------------+-----+


# save if you want.
forecast.head(500).to_csv('./data/pandas_prophet_forecast_output.csv') 
forecastSpark.limit(500) \
    .repartition(1) \
    .write.mode("overwrite").option("header", True) \
    .csv('./data/pandas_prophet_forecast_output_spark/')


