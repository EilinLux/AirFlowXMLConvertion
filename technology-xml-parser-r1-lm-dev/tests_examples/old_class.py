##########################################
# PySparkXMLParserTest
##########################################

class PySparkXMLParserTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up SparkSession and XMLParser for testing."""
        cls.spark = (
            SparkSession.builder.appName("XMLParserTest")
            .master("local[*]")  # Use local mode for testing
            .getOrCreate()
        )
        cls.client = unittest.mock.Mock()  # Mock GCS client
        cls.parser = XMLParser(cls.spark, cls.client)
        cls.parser.config = TEST_CONFIG

    @classmethod
    def tearDownClass(cls):
        """Stop the SparkSession after testing."""
        cls.spark.stop()

    def test_load_config(self):
        """Test loading configuration from YAML."""
        config = load_config("config.yaml")  # Replace with your actual config file
        self.assertIsInstance(config, dict)
        self.assertIn("gcs", config)
        self.assertIn("spark", config)

    def test_parse_and_validate_arguments(self):
        """Test parsing and validating command-line arguments."""
        # Mock command-line arguments
        import sys

        sys.argv = [
            "test.py",
            "--start-date",
            "20241019",
            "--end-date",
            "20241020",
            "--lm",
            "US,CA",
            "--techs",
            "5G",
        ]
        sdate, edate, lms, techs = parse_and_validate_arguments()
        self.assertIsInstance(sdate, datetime)
        self.assertIsInstance(edate, datetime)
        self.assertEqual(lms, ["US", "CA"])
        self.assertEqual(techs, ["5G"])

    def test_create_spark_session(self):
        """Test creating a SparkSession."""
        spark = create_spark_session("")
        self.assertIsInstance(spark, SparkSession)

    def test_flatten_xml_file(self):
        """Test flattening an XML file."""
        # Create a sample DataFrame
        data = [
            (
                "SubNetwork=ONRM_ROOT_MO,SubNetwork=VRF_Public_MPLS,SubNetwork=MPLS_TNL_SN",
            ),
        ]
        columns = ["xn:SubNetwork._id"]
        sparkdf = self.spark.createDataFrame(data, columns)

        # Flatten the DataFrame
        flat_sparkdf = flatten_xml_file(
            sparkdf, self.parser.config["flattening_config"]["5G"]
        )

        # Assertions
        self.assertEqual(flat_sparkdf.columns, ["SubNetworkId"])
        self.assertEqual(flat_sparkdf.count(), 1)

    def test_read_sparkdf(self):
        """Test reading an XML file from GCS."""
        # Mock the GCS client to return a sample XML file
        self.client.get_bucket.return_value.list_blobs.return_value = [
            unittest.mock.Mock(name="test.xml")
        ]

        # Read the XML file
        sparkdf = self.parser.read_sparkdf(
            self.spark, "test.xml", "test-bucket", self.parser.config
        )

        # Assertions
        self.assertIsNotNone(sparkdf)
        # Add more specific assertions based on your XML structure

    def test_save_sparkdf_to_parquet(self):
        """Test saving a Spark DataFrame to GCS in Parquet format."""
        # Create a sample DataFrame
        data = [("value1",), ("value2",)]
        columns = ["col1"]
        sparkdf = self.spark.createDataFrame(data, columns)

        # Save the DataFrame (this will call the mocked GCS client)
        self.parser.save_sparkdf_to_parquet(
            "test.xml", sparkdf, "test-bucket/output", self.parser.config
        )

        # Assertions (check if the GCS client was called with the correct arguments)
        self.client.get_bucket.assert_called_with("test-bucket")