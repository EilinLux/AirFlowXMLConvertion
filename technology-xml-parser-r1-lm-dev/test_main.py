import unittest
from pyspark.sql import SparkSession
from datetime import datetime
import pytest
from main import (
    XMLParser,
    load_config,
    parse_and_validate_arguments,
    create_spark_session,
    flatten_xml_file,
)



##########################################
# Sample configuration for testing
##########################################

TEST_CONFIG = {
    "gcs": {
        "BUCKET_NAME": "test-bucket",
        "BUCKET_OUTPUT": "gs://test-bucket/output",
        "XML_FILE_PATH": "gs://test-bucket/input",
    },
    "spark": {"SPARK_JARS": ""},
    "tech_prefixes": {"5G": "IM/provider/5G/enmtbl1"},
    "xml_config": {"rowTag": "configData"},
    "output_config": {"output_prefix": "", "save_mode": "overwrite"},
    "flattening_config": {
        "5G": [
            {"operation": "select", "columns": {"xn:SubNetwork._id": "SubNetworkId"}},
            {
                "operation": "explode",
                "column": "xn:SubNetwork.xn:MeContext",
                "alias": "MeContext",
            },
            # ADD MORE, if needed
        ]
    },
}






@pytest.fixture(scope="session")
def spark():
    """Pytest fixture to create a SparkSession."""
    spark = (
        SparkSession.builder.appName("XMLParserTest")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def parser(spark):
    """Pytest fixture to create an XMLParser with mocked GCS client."""
    client = unittest.mock.Mock()
    parser = XMLParser(spark, client)
    parser.config = TEST_CONFIG
    return parser


def test_load_config():
    """Test loading configuration from YAML."""
    config = load_config("config.yaml")
    assert isinstance(config, dict)
    assert "gcs" in config
    assert "spark" in config


def test_parse_and_validate_arguments():
    """Test parsing and validating command-line arguments."""
    # Mock command-line arguments using pytest's monkeypatch fixture
    import sys

    with unittest.mock.patch.object(sys, "argv", [
        "test.py",
        "--start-date",
        "20241019",
        "--end-date",
        "20241020",
        "--lm",
        "US,CA",
        "--techs",
        "5G",
    ]):
        sdate, edate, lms, techs = parse_and_validate_arguments()
        
    assert isinstance(sdate, datetime)
    assert isinstance(edate, datetime)
    assert lms == ["US", "CA"]
    assert techs == ["5G"]


def test_create_spark_session():
    """Test creating a SparkSession."""
    spark = create_spark_session("")
    assert isinstance(spark, SparkSession)

from pyspark.sql import Row

# ... other imports and fixtures ...

def test_flatten_xml_file(spark):
    """Test flattening an XML file."""
    from tests_examples.test_flattern_xml_file_example import data, schema 
    sparkdf = spark.createDataFrame(data=data, schema=schema)

    # Flatten the DataFrame
    flat_sparkdf = flatten_xml_file(
        sparkdf, TEST_CONFIG["flattening_config"]["5G"]
    )

    # Assertions
    assert flat_sparkdf.columns == [
        "SubNetworkId",
        "MeContextId",
        "ManagedElementId",
        "ParentVsDataContainerId",
        "ParentvsDataSystemFunctions",
        "ParentvsDataFormatVersion",
        "ParentvsDataType",
        "MiddleVsDataContainerId",
        "MiddlevsDataSwInventory",
        "MiddlevsDataFormatVersion",
        "MiddlevsDataType",
        "ChildVsDataContainerId",
        "ChildVsDataType",
        "ChildVsDataFormatVersion",
        "vsDataSwItemAdditionalInfo",
        "vsDataSwItemType",
        "vsDataSwItemDescription",
        "vsDataSwItemProductName",
        "vsDataSwItemProductNumber",
        "vsDataSwItemProductRevision",
        "vsDataSwItemProductDate",
        "vsDataSwItemConsistsOf",
        "vsDataSwItemSwItemId",
        "vsDataSwItemUserLabel",
    ]
    assert flat_sparkdf.count() == 1  # Adjust if you added more rows

def test_read_sparkdf(parser):
    """Test reading an XML file from GCS."""
    # Mock the GCS client to return a sample XML content
    sample_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <configData>
        <data>
            <item id="1">
                <value>A</value>
            </item>
        </data>
    </configData>
    """
    parser.client.get_bucket.return_value.list_blobs.return_value = [
        unittest.mock.Mock(name="test.xml", download_as_string=lambda: sample_xml.encode())
    ]

    sparkdf = parser.read_sparkdf(
        parser.spark, "test.xml", "test-bucket", parser.config
    )

    assert sparkdf is not None
    assert sparkdf.count() == 1

def test_save_sparkdf_to_parquet(parser):
    """Test saving a Spark DataFrame to GCS in Parquet format."""
    # Create a sample DataFrame
    data = [("value1",), ("value2",)]
    columns = ["col1"]
    sparkdf = parser.spark.createDataFrame(data, columns)

    # Save the DataFrame (this will call the mocked GCS client)
    parser.save_sparkdf_to_parquet(
        "test.xml", sparkdf, "test-bucket/output", parser.config
    )

    # Assertions (check if the GCS client was called with the correct arguments)
    parser.client.get_bucket.assert_called_with("test-bucket")




##########################################
# Main
##########################################

if __name__ == "__main__":
    unittest.main()