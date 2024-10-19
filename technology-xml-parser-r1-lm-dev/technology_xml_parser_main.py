from utils.config import load_config, parse_arguments, parse_and_validate_arguments
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
import logging
from utils.spark_session import create_spark_session
from utils.XMLParser import XMLParser


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    config = load_config("config.yaml")

    if config:
        BUCKET_NAME = config["gcs"]["BUCKET_NAME"]
        SPARK_JARS = config["spark"]["SPARK_JARS"]
        BUCKET_OUTPUT = config["gcs"]["BUCKET_OUTPUT"]



    # Create Spark, GCS client and XMLParser
    client = storage.Client()
    spark = create_spark_session(SPARK_JARS)
    parser = XMLParser(spark, client, config)
    sdate, edate, lms, techs = parse_and_validate_arguments()

    for lm in lms:
        bucket_name = BUCKET_NAME.format(lm)
        prefix_list = parser.build_paths(techs, sdate, edate)
        parser.process_xml_files(bucket_name, prefix_list, BUCKET_OUTPUT)
