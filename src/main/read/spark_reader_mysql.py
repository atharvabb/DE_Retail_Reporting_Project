from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import os
from pyspark import SparkConf
from src.main.utility import logger
from resources.dev.config import MYSQL_URL


def spark_reader_mysql(sparksession:SparkSession,database,user,password,table):
    #ONLY 10 PARALEL CONNECTIONS ARE ALLOWED TO MYSQL DB
    jdbcDF = sparksession.read \
        .format("jdbc") \
        .option("url", f"{MYSQL_URL}/{database}") \
        .option("user", user) \
        .option("password", password) \
        .option("dbtable", table).load()
        #.option("partitionColumn",partitionColumn)\
        #.option("numPartitions",numPartitions) \
        #.option("lowerBound",lowerBound)\
        #.option("upperBound",upperBound)\
    logger.mylogger.info(f"Spark DataFrame Read Completed - {jdbcDF}")
    #'partitionColumn', 'lowerBound', 'upperBound', and 'numPartitions'
    # COLUMN,PARTITIONS,LOWER_BOUND,UPPER_BOUND
#pyspark.errors.exceptions.captured.IllegalArgumentException: requirement failed: When reading JDBC data sources, users need to specify all or none for the following options: 'partitionColumn', 'lowerBound', 'upperBound', and 'numPartitions'
#COLUMN,PARTITIONS,LOWER_BOUND,UPPER_BOUND
    return jdbcDF
    #make optimized in next itteration