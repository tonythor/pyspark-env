# pyspark-env
A simple virtualenv to practice pyspark queries.


```
python3 -m venv .venv     # build your project virtual env
source .venv/bin/activate # activate your virual env
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r ./requirements.txt
```

Now ipython yourself a java install.. 

```
(.venv) hurricane:src afraser$ ipython
Python 3.10.8 (main, Oct 21 2022, 22:22:30) [Clang 14.0.0 (clang-1400.0.29.202)]
Type 'copyright', 'credits' or 'license' for more information
IPython 8.9.0 -- An enhanced Interactive Python. Type '?' for help.

In [1]: import jdk
   ...: jdk.install('11')
... 
```
And link that jdk to your shell. 
export JAVA_HOME=/Users/afraser/.jdk/jdk-11.0.18+10/Contents/Home
export SPARK_HOME=/Users/afraser/Documents/src/pyspark-env/.venv/lib/python3.10/site-packages/pyspark

Download spark jars:
./download_spark_jars.sh

Copy them into spark home:
cp ./lib/*.jar /Users/afraser/Documents/src/pyspark-env/.venv/lib/python3.10/site-packages/pyspark/jars/. 









note: this [bootstrap class](./src/fraser/bootstrap.py) will start spark either ECS or ~/.aws/credentials 
