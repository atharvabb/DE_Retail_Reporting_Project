import sys
import logging
import resources.dev.config as conf

logging.basicConfig(filename=conf.Logging_path,filemode='w',level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

mylogger = logging.getLogger(__name__)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
mylogger.addHandler(handler)

