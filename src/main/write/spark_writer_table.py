from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import os
from pyspark import SparkConf
from src.main.utility import logger
from resources.dev.config import MYSQL_URL
from pyspark.sql import DataFrame


def spark_writer_table(dataframe:DataFrame,database,writeformat,table,savemode,partitionCol=None,bucketCol=None,numbuckets=None):
     writer = dataframe.write.format(writeformat).mode(savemode)
     if partitionCol:
         writer=writer.partitionBy(partitionCol)
     if bucketCol and numbuckets:
         writer=writer.bucketBy(numbuckets,bucketCol).sortBy(bucketCol)
     writer.saveAsTable(f"{database}.{table}")
        #.option("partitionColumn",partitionColumn)\
        #.option("numPartitions",numPartitions) \
        #.option("lowerBound",lowerBound)\
        #.option("upperBound",upperBound)\

    #'partitionColumn', 'lowerBound', 'upperBound', and 'numPartitions'
    # COLUMN,PARTITIONS,LOWER_BOUND,UPPER_BOUND
#pyspark.errors.exceptions.captured.IllegalArgumentException: requirement failed: When reading JDBC data sources, users need to specify all or none for the following options: 'partitionColumn', 'lowerBound', 'upperBound', and 'numPartitions'
#COLUMN,PARTITIONS,LOWER_BOUND,UPPER_BOUN