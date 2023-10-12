import os
import platform
import datetime
hostname = platform.node()


if hostname == "DESKTOP-HHJ4LTR":
    ENV = "DEV"
    env_dir = "FIN"
else:
    ENV = "UAT"
    env_dir = "FIN_U"

DATE_CONTROL_FILE = f"E:\\warehouse\\projects\\{env_dir}\\RETAIL_7X4\\retail_date_control_file.txt"
dates = open(DATE_CONTROL_FILE,"r").readlines()[0]
PREV_DATE = dates.split(",")[0]
CURRENT_DATE = dates.split(",")[1]
NEXT_DATE = dates.split(",")[2]
Current_Timestamp=str(datetime.datetime.now()).replace(" ","_").split(".")[0]
RFT_ROOT_DIR = f"E:\\warehouse\\projects\\{env_dir}\\RFT_BSL"
HDFS_ROOT_DIR = f"E:\\warehouse\\projects\\{env_dir}\\RETAIL_7X4"
Logging_path = f"E:\\warehouse\\projects\\{env_dir}\\RETAIL_7X4\\logging\\retail_7x4_{CURRENT_DATE}.txt"
SparkConfigurations = {"spark.master":"local[*]",
                      "spark.app.name":f"RETAIL_7X4_{CURRENT_DATE}"}

