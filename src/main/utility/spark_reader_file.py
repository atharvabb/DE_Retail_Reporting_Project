from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import os
from pyspark import SparkConf
from src.main.utility import logger

def spark_reader_file(sparksession:SparkSession,format:str,isheader:bool,isinferSchema:bool,
                      location:str,schema="",):
    if format=="parquet":
        dataframe = sparksession.read.option("path", location).load()
        return dataframe
    elif (isinferSchema is False and schema=="") :#or (isinferSchema is True and isinstance(schema,StructType)):
        raise Exception("You need to pass either isinferSchema=True or Schema in StructType format (only any 1 allowed)")
        logger.mylogger.error("You need to pass either isinferSchema=True or Schema in StructType format (only any 1 allowed)")
    elif isinferSchema is True:
        dataframe = sparksession.read.format(format)\
                    .option("header",isheader)\
                    .option("inferSchema",isinferSchema)\
                    .option("path",location)\
                    .load()
        return dataframe
    else:
        dataframe = sparksession.read.format(format) \
            .schema(schema) \
            .option("header", isheader) \
            .option("path", location) \
            .load()
        return dataframe
#spark_reader_file()