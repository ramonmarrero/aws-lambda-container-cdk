import sys
import logging
import sagemaker
from time import gmtime, strftime
from sagemaker.spark.processing import PySparkProcessor

def handler(event, context):


    sagemaker_logger = logging.getLogger("sagemaker")
    sagemaker_logger.setLevel(logging.INFO)
    sagemaker_logger.addHandler(logging.StreamHandler())

    sagemaker_session = sagemaker.Session()
    bucket = sagemaker_session.default_bucket()
    role = sagemaker.get_execution_role()

    print(f'Bucket: {bucket} Role: {role}')

    return 'AWS Lambda Spark Process Ended - ' + sys.version + '!'
