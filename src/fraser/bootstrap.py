import configparser
import logging
import os

class Bootstrap:
    def __init__(self):
        pass

    def load_config(self, ini_fn: str = f"resources/{os.getenv('MODULE')}.ini") -> configparser.ConfigParser:
        ini_path = self.get_resource(ini_fn)
        config = configparser.ConfigParser()
        logging.info(f"loading config file from:{str(ini_path)}")
        config.read([str(ini_path)])
        return config

    def running_on_ecs(self) -> bool:
        if  os.environ.get("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"):
            return True
        else:
            return False

    def get_spark(self):
        import botocore.session
        from pyspark.sql import SparkSession
        from pyspark import SparkConf

        session = botocore.session.get_session()
        credentials = session.get_credentials()
        conf = SparkConf()
        conf.set('spark.hadoop.fs.s3a.access.key', credentials.access_key)
        conf.set('spark.hadoop.fs.s3a.secret.key', credentials.secret_key)
        conf.set("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        conf.set('spark.driver.memory', '4g')
        conf.set('spark.executor.memory', '4g')

        if self.running_on_ecs():
            #overwrite provider to use session token, doesn't seem to work as a array, event though it is supposed to.
            conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
            conf.set("spark.hadoop.fs.s3a.session.token", credentials.token)
        spark = (
            SparkSession
                .builder
                .config(conf=conf)
                .appName(f"my-executor")
                .getOrCreate()
        )
        self.spark = spark
        self.spark.sparkContext.setLogLevel("ERROR")
        return self.spark

    def kill_spark(self):
        """
        <i>If necessary, used to close spark after job completion.</i>
        """
        self.spark.stop()

    def set_log_level(self, level="warning"):
        """
        A shortcut to help ipython or jupyter users using bootstrap to adjust logging between code blocks.
        Use this to make sure you're updating all the loggers.
        ::
            bootstrap.set_log_level('debug')
        """
        if level == "warning":
            logging.getLogger().setLevel(logging.WARNING)
        if level == "error":
            logging.getLogger().setLevel(logging.ERROR)
        if level == "info":
            logging.getLogger().setLevel(logging.INFO)
        if level == "debug":
            logging.getLogger().setLevel(logging.DEBUG)
