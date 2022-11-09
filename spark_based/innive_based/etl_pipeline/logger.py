import os
import logging
import sys


LOG_DIRS = "/home/rohitg/PycharmProjects/spark_pocs/spark_based/innive_based/Data/from_customer/dataOutput"

class YarnLogger:
    """https://stackoverflow.com/questions/40806225/pyspark-logging-from-the-executor"""

    @staticmethod
    def setup_logger():
        # if not 'LOG_DIRS' in os.environ:
        #     sys.stderr.write('Missing LOG_DIRS environment variable, pyspark logging disabled')
        #     return

        file = LOG_DIRS + '/pyspark.log'
        logging.basicConfig(filename=file, level=logging.INFO,
                format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s')

    def __getattr__(self, key):
        return getattr(logging, key)

YarnLogger.setup_logger()
