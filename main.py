
import resources.dev.config as conf
from src.main.utility.logger import *
from src.main.utility.spark_session_create import *
from src.main.utility.spark_reader_file import *
import logging
from pyspark.sql.types import *


spark = spark_session_create(conf.SparkConfigurations)
dataframe = spark_reader_file(
     sparksession=spark,
     format="csv",
     isheader=True,
     isinferSchema=True,
     location="E:\\sample_today.txt"
 )
dataframe.show()
#dataframe.show()