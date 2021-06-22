import sys
import logging
import sagemaker
import boto3
import os
from time import gmtime, strftime
from sagemaker.spark.processing import PySparkProcessor



def handler(event, context):

    AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
    AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
    AWS_REGION = os.environ['AWS_REGION']
    sagemaker_logger = logging.getLogger("sagemaker")
    sagemaker_logger.setLevel(logging.INFO)
    sagemaker_logger.addHandler(logging.StreamHandler())

    boto3Session = boto3.session.Session(aws_access_key_id=AWS_ACCESS_KEY_ID,
                                         aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)

    sagemaker_session = sagemaker.Session(boto3Session)

    bucket = "sagemaker-eu-central-1-310766717595"
    role = "arn:aws:iam::310766717595:role/service-role/AmazonSageMaker-ExecutionRole-20210310T105594"

    # Upload the raw input dataset to a unique S3 location
    timestamp_prefix = strftime("%Y-%m-%d-%H-%M-%S", gmtime())
    prefix = "sagemaker/spark-preprocess-demo/{}".format(timestamp_prefix)
    input_prefix_abalone = "{}/input/raw/abalone".format(prefix)
    input_preprocessed_prefix_abalone = "{}/input/preprocessed/abalone".format(prefix)

    sagemaker_session.upload_data(path="./abalone.csv", bucket=bucket, key_prefix=input_prefix_abalone)
    
    # Run the processing job
    spark_processor = PySparkProcessor(
    	base_job_name="sm-spark",
        framework_version="2.4",
    	role=role,
	sagemaker_session=sagemaker_session,
    	instance_count=2,
    	instance_type="ml.m5.xlarge",
    	max_runtime_in_seconds=1200,
     )

    spark_processor.run(
    	submit_app="./preprocess.py",
    	arguments=[
        	"--s3_input_bucket",
        	bucket,
        	"--s3_input_key_prefix",
        	input_prefix_abalone,
        	"--s3_output_bucket",
        	bucket,
        	"--s3_output_key_prefix",
        	input_preprocessed_prefix_abalone,
    	],
      spark_event_logs_s3_uri="s3://{}/{}/spark_event_logs".format(bucket, prefix),logs=False,)

    return 'AWS Lambda Spark Process Ended - ' + sys.version + '!'

