# pyspark-env
A simple virtualenv to practice pyspark queries.

And: 
* loading data from sqlite
* prohpet forecasts



## project set up
### 1 download
git clone wherever you want to run this project from

### 2 build your venv

```
python3 -m venv .venv     # build your project virtual env
source .venv/bin/activate # activate your virual env
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r ./requirements.txt
```

### 3 install java
```
(.venv) hurricane:src afraser$ ipython
Python 3.10.8 (main, Oct 21 2022, 22:22:30) [Clang 14.0.0 (clang-1400.0.29.202)]
Type 'copyright', 'credits' or 'license' for more information
IPython 8.9.0 -- An enhanced Interactive Python. Type '?' for help.

In [1]: import jdk
   ...: jdk.install('11')

```
And link install to your shell, about like this: 
```
export JAVA_HOME=/Users/****/.jdk/jdk-11.0.18+10/Contents/Home

```
### 4 add spark jars
set up your spark home: 

```
export SPARK_HOME=/Users/****/Documents/src/pyspark-env/.venv/lib/python3.10/site-packages/pyspark
./download_spark_jars.sh
cp ./lib/*.jar $SPARK_HOME/jars/. 

```

### 5 adjust activate
add set and unset SPARK_HOME and JAVA_HOME in .venv/bin/activate and deactivate


### 6 test with real data
Test by loading something from S3. First change into the source directory like so:

```
(.venv) hurricane:pyspark-env afraser$ ipython

Python 3.10.8 (main, Oct 21 2022, 22:22:30) [Clang 14.0.0 (clang-1400.0.29.202)]
Type 'copyright', 'credits' or 'license' for more information
IPython 8.9.0 -- An enhanced Interactive Python. Type '?' for help.

In [1]: cd ./src
/Users/afraser/Documents/src/pyspark-env/src

In [2]: from fraser.bootstrap import Bootstrap
   ...: b = Bootstrap()
   ...: spark = b.get_spark()
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
23/02/02 17:29:41 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable

In [3]: spark.read.csv("s3a://tonyfraser-data/delayed_flights/*.csv").show(3)
```



## Notes:
1. This [bootstrap class](./src/fraser/bootstrap.py) will start spark either ECS or ~/.aws/credentials 
