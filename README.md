# pyspark-env
A simple virtualenv to practice pyspark queries.


### Build your venv

```
python3 -m venv .venv     # build your project virtual env
source .venv/bin/activate # activate your virual env
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r ./requirements.txt
```

### install java
ipython yourself a java install... 
```
(.venv) hurricane:src afraser$ ipython
Python 3.10.8 (main, Oct 21 2022, 22:22:30) [Clang 14.0.0 (clang-1400.0.29.202)]
Type 'copyright', 'credits' or 'license' for more information
IPython 8.9.0 -- An enhanced Interactive Python. Type '?' for help.

In [1]: import jdk
   ...: jdk.install('11')
... 
```
And link install to yoru shell, about like this: 
```
export JAVA_HOME=/Users/afraser/.jdk/jdk-11.0.18+10/Contents/Home

```
### spark jars
set up your spark home: 

```
export SPARK_HOME=/Users/afraser/Documents/src/pyspark-env/.venv/lib/python3.10/site-packages/pyspark
./download_spark_jars.sh
cp ./lib/*.jar SPARK_HOME/jars/. 
```


### adjust activate
set and unset SPARK_HOME and JAVA_HOME in .venv/bin/activate


### Test with real data
Test by loading something from S3, like so:

```
In [1]: from fraser.bootstrap import Bootstrap
In [2]: b = Bootstrap()
   ...: spark = b.get_spark()
In [3]: spark.read.csv("s3a://tonyfraser-data/delayed_flights/*.csv").show(3
```



note: this [bootstrap class](./src/fraser/bootstrap.py) will start spark either ECS or ~/.aws/credentials 
