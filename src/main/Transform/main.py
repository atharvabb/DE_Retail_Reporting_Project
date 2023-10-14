from src.main.read.spark_reader_file import spark_reader_file
from src.main.utility.spark_session_create import *
from src.main.Transform.scd_type_2_generic import scd_type_2
from src.main.read.spark_reader_mysql import *
from src.main.write.spark_writer_table import *
from pyspark.sql.functions import lit
#Creating Spark Session
spark = spark_session_create(conf.SparkConfigurations)
spark.sparkContext.setLogLevel("ERROR")

dataframe1 = spark_reader_file(
     sparksession=spark,
     format="csv",
     isheader=True,
     isinferSchema=True,
     location="E:\\sample_today.txt"
 )

dataframe2 = spark_reader_file(
     sparksession=spark,
     format="csv",
     isheader=True,
     isinferSchema=True,
     location="E:\\sample_today_1.txt"
 )
dataframe2.show()
DF= scd_type_2(dataframe1,dataframe2,"CUST_ID","active","PHONE_NUMBER")
DF.show(50)


# jdf=spark_reader_mysql(sparksession=spark,user="root",password="root",
#                        database="bsl_finx_db",table="sample1")
# #jdf.show()
# spark.sql("select * from bsl_finx_db.out1 limit 5").show()
#spark_writer_table(dataframe=jdf,database="bsl_finx_db",table="out1",writeformat="csv",savemode="overwrite")
