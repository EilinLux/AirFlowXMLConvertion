
import os
import logging
from pyspark.sql.utils import AnalysisException
from datetime import timedelta
from pyspark.sql.functions import col, explode, agg, lit

class XMLParser:
    def __init__(self, spark, client, config):
        """
        Initializes the XMLParser with SparkSession and GCS client.

        Args:
            spark (SparkSession): SparkSession object.
            client (storage.Client): Google Cloud Storage client object.
            config (dict): Configuration dictionary.
        """
        self.spark = spark
        self.client = client
        self.config = config

    def build_paths(self, techs, sdate, edate):
        """
        Generates a list of GCS paths based on technologies, date range,
        and configuration.

        Args:
            techs (list): List of technologies.
            sdate (datetime): Start date.
            edate (datetime): End date.
            config (dict): Configuration dictionary.

        Returns:
            list: A list of GCS paths.
        """
        paths_list = []

        for tech in techs:
            try:
                # Get technology-specific prefix from config
                prefix_im_provider = self.config["tech_prefixes"][tech]
            except KeyError:
                logging.warning(
                    f"Unsupported technology '{tech}' found in config. Skipping."
                )
                continue

            delta = edate - sdate
            cdate = sdate
            for i in range(delta.days + 1):
                year = str(cdate.year)
                month = str(cdate.month).zfill(2)
                day = str(cdate.day).zfill(2)
                
                # Construct path based on technology and date
                path = f"{prefix_im_provider}/year={year}/month={month}/day={day}"
                paths_list.append(path)

                cdate = sdate + timedelta(days=i + 1)

        return paths_list


    def read_sparkdf(self, blobname, bucketname):
        """
        Reads an XML file from GCS into a Spark DataFrame, handling potential errors.

        Args:         
            blobname (str): Name of the blob (file) in GCS.
            bucketname (str): Name of the GCS bucket.
        Returns:
            DataFrame: A Spark DataFrame containing the XML data, or None if an error occurs.
        """
        xml_file_path = f"gs://{bucketname}/{blobname}"
        logging.info(f"Reading from {xml_file_path}")

        try:
            # Use rowTag from config if available
            row_tag = self.config.get("xml_config", {}).get("rowTag", "configData")
            return self.spark.read.format("xml").option("rowTag", row_tag).load(xml_file_path)
        except AnalysisException as e:
            logging.error(f"Error reading XML file {xml_file_path}: {e}")
            return None


    def save_sparkdf_to_parquet(self, original_path, sparkdf, bucket_output):
        """
        Saves a Spark DataFrame to GCS in Parquet format, handling potential errors.

        Args:
            original_path (str): Original path of the XML file in GCS.
            sparkdf (DataFrame): The Spark DataFrame to save.
            bucket_output (str): The GCS bucket to write the output to.
            config (dict): Configuration dictionary.
        """
        try:
            # Construct output path using config
            output_prefix = self.config.get("output_config", {}).get("output_prefix", "")
            file_name = os.path.basename(original_path)
            file_path = os.path.join(bucket_output, output_prefix, file_name)
            file_path = file_path.replace(".xml", ".parquet")  # Change extension
            logging.info(f"Saving to {file_path}")

            # Use save mode from config if available
            save_mode = self.config.get("output_config", {}).get("save_mode", "overwrite")
            sparkdf.write.mode(save_mode).parquet(file_path)

        except Exception as e:
            logging.error(f"Error saving DataFrame to {file_path}: {e}")




    def flatten_xml_file(sparkdf, flattening_config):
        """
        Flattens an XML file into a Spark DataFrame based on provided configuration.

        Args:
            sparkdf (DataFrame): The Spark DataFrame containing the XML data.
            flattening_config (list): A list of dictionaries, each specifying 
                                    a flattening step.

        Returns:
            DataFrame: A flattened Spark DataFrame.
        """
        for step in flattening_config:
            if step["operation"] == "select":
                sparkdf = sparkdf.select(
                    *[
                        col(src).alias(tgt)
                        for src, tgt in step["columns"].items()
                    ]
                )
            elif step["operation"] == "explode":
                sparkdf = sparkdf.select(
                    explode(col(step["column"])).alias(step["alias"])
                )
            elif step["operation"] == "filter":
                sparkdf = sparkdf.filter(step["condition"])
            # elif step["operation"] == "join":
            #     sparkdf = sparkdf.join(
            #         other_df, on=step["on"], how=step.get("how", "inner")
            #     )  # Make sure other_df is available
            elif step["operation"] == "groupBy":
                sparkdf = sparkdf.groupBy(step["columns"])
            elif step["operation"] == "agg":
                sparkdf = sparkdf.agg(
                    **{
                        tgt: func for tgt, func in step["aggregations"].items()
                    }
                )
            elif step["operation"] == "orderBy":
                sparkdf = sparkdf.orderBy(
                    *[col(c).asc() if asc else col(c).desc() for c, asc in step["columns"].items()]
                )
            # elif step["operation"] == "union":
            #     sparkdf = sparkdf.union(other_df)  # Make sure other_df is available
            elif step["operation"] == "distinct":
                sparkdf = sparkdf.distinct()
            elif step["operation"] == "withColumn":
                if "value" in step:
                    sparkdf = sparkdf.withColumn(step["column"], lit(step["value"]))
                elif "func" in step:
                    sparkdf = sparkdf.withColumn(step["column"], eval(step["func"]))
            # Add more operations as needed 

        return sparkdf

    def process_xml_files(self, bucket_name, prefix_list, bucket_output):
        """
        Processes XML files from GCS, flattens them based on technology,
        and saves them as Parquet.

        Args:
            bucket_name (str): Name of the GCS bucket.
            prefix_list (list): List of prefixes to filter files in the bucket.
            bucket_output (str): The GCS bucket to write the output to.
        """
        try:
            bucket = self.client.get_bucket(bucket_name)
        except Exception as e:
            logging.error(f"Error accessing bucket {bucket_name}: {e}")
            return

        for prefix in prefix_list:
            logging.info(f"Checking prefix: {prefix}")
            blobs = list(bucket.list_blobs(prefix=prefix))  # Get the list of blobs once
            if blobs:
                for blob in blobs:
                    try:
                        logging.info(f"Processing file: {blob.name}")
                        sparkdf = self.read_sparkdf(blob.name, bucket_name)  # Pass spark and config
                        if sparkdf is None:
                            continue  # Skip if reading fails

                        # Determine technology using the config
                        tech = None
                        for tech_key, tech_prefix in self.config["tech_prefixes"].items():
                            if tech_prefix in blob.name:
                                tech = tech_key
                                break

                        if tech is None:
                            logging.warning(f"No supported technology found in path: {blob.name}")
                            continue

                        # Flatten based on technology
                        flat_sparkdf = self.flatten_xml_file(sparkdf, self.config["flattening_config"][tech])

                        self.save_sparkdf_to_parquet(blob.name, flat_sparkdf, bucket_output)  

                    except Exception as e:
                        logging.error(f"Error processing file {blob.name}: {e}")
            else:
                logging.info(f"No files found for prefix: {prefix}")
