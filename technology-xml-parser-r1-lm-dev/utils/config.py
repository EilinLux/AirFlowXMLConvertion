import yaml
from argparse import ArgumentParser, RawTextHelpFormatter
from datetime import datetime


def load_config(config_file):
  """
  Loads configuration from a YAML file.

  Args:
    config_file: Path to the YAML configuration file.

  Returns:
    A dictionary containing the configuration.
  """
  try:
    with open(config_file, "r") as yaml_file:
      config = yaml.safe_load(yaml_file)
      
    # Check if required keys exist
    if not all(key in config for key in ("gcs", "spark")):
      raise ValueError("Missing 'gcs' or 'spark' section in config file.")
    if not all(key in config["gcs"] for key in ("BUCKET_NAME", "BUCKET_OUTPUT")):
      raise ValueError("Missing 'BUCKET_NAME' or 'BUCKET_OUTPUT' in 'gcs' section.")
    if "SPARK_JARS" not in config["spark"]:
      raise ValueError("Missing 'SPARK_JARS' in 'spark' section.")

    return config

  except FileNotFoundError:
    print(f"Error: Configuration file not found: {config_file}")
    return None
  except yaml.YAMLError as e:
    print(f"Error parsing YAML file: {e}")
    return None
  except ValueError as e:
    print(f"Error in config file: {e}")
    return None



def parse_arguments():
    """
    Parses command-line arguments for the script.

    Returns:
        An argparse.Namespace object containing the parsed arguments.
    """
    parser = ArgumentParser(
        description="Process XML files from GCS, flatten them, and save as Parquet.",
        formatter_class=RawTextHelpFormatter  # To allow for formatted help text
    )
    parser.add_argument(
        "--start-date",
        dest="startdate",
        required=True,
        help="Start date for processing data in YYYYMMDD format (e.g., 20241019).",
    )
    parser.add_argument(
        "--end-date",
        dest="enddate",
        required=True,
        help="End date for processing data in YYYYMMDD format (e.g., 20241020).",
    )
    parser.add_argument(
        "--lm",
        dest="lms",
        required=True, 
        help="""Comma-separated list of local markets (e.g., "US,CA,UK").""",
    )
    parser.add_argument(
        "--techs",
        dest="techs",
        required=True,
        help="""Comma-separated list of technologies to process (e.g., "2G,4G,5G").""",
    )
    return parser.parse_args()



def parse_and_validate_arguments(args):
    """
    Parses command-line arguments, validates date formats, and 
    returns processed parameters.

    Returns:
        A tuple containing:
            - sdate: Start date as a datetime object.
            - edate: End date as a datetime object.
            - lms: List of local markets.
            - techs: List of technologies.
    """

    try:
        sdate = datetime.strptime(args.startdate, "%Y%m%d")
        edate = datetime.strptime(args.enddate, "%Y%m%d")
    except ValueError:
        print("Error: Invalid date format. Please use YYYYMMDD.")
        exit(1)

    lms = args.lms.split(",")
    techs = args.techs.split(",")

    return sdate, edate, lms, techs