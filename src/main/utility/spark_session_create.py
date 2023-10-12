from pyspark.sql import SparkSession
import os
from pyspark import SparkConf
from src.main.utility import logger
import resources.dev.config as conf
os.environ["JAVA_HOME"] = "E:\\jdk-20"
os.environ["SPARK_HOME"] = "C:\\Spark\\spark"
def spark_session_create(sparkConfigs:dict):
     try:
        sparkconf = SparkConf()
        for conf in sparkConfigs.items():
            sparkconf.set(conf[0],conf[1])
        spark = SparkSession.builder.config(conf=sparkconf).getOrCreate()
        logger.mylogger.info(f"Spark Session Created - {spark}")
        return spark
     except:
        logger.mylogger.error(f"Unable to Create Spark Session Please Check Logs For More Details")
        return -1






