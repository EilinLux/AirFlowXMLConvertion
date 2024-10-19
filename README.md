
# PySpark XML to Parquet on Dataproc

This repository contains an Airflow DAG and a PySpark job that processes XML files from Google Cloud Storage (GCS), flattens them based on technology (2G, 3G, 4G, 5G), and saves the processed data as Parquet files in GCS. The job runs on a Dataproc cluster, and the entire workflow is orchestrated by Airflow.

## Features

* Reads XML files from GCS.
* Supports different XML structures for 2G, 3G, 4G, and 5G technologies.
* Flattens the XML data into a tabular format using a configurable approach.
* Saves the processed data as Parquet files in GCS.
* Highly configurable using YAML configuration files and Airflow variables.
* Uses Spark for efficient distributed processing.
* Includes error handling and logging.
* Orchestrates the entire workflow using an Airflow DAG.
* Runs on a Dataproc cluster for scalable processing.
* Includes unit tests using pytest.

## Requirements

* Python 3.7 or higher
* PySpark
* Google Cloud SDK
* PyYAML
* pytest
* Apache Airflow (ideally Cloud Composer on GCP)

## What involves
 
### Extensible Markup Language
Extensible Markup Language (XML) is a markup language. It is quite common to find files as main source for an ETL pipeline, which stands for extract, transform, load and is a three-phase process where data is extracted, transformed (cleaned, sanitized, scrubbed) and loaded into a new datacontainer (ex. a database).

### pyspark 
PySpark is the Python API for Apache Spark, an open source, distributed computing framework and set of libraries for real-time, large-scale data processing.

# PySpark XML Parser

This PySpark job processes XML files from Google Cloud Storage (GCS), flattens them based on technology (2G, 3G, 4G, 5G), and saves the processed data as Parquet files back into GCS.


## Installation

1. Install the required packages:
   ```bash
   pip install pyspark google-cloud-storage pyyaml
    ```
2. Set up Google Cloud SDK and authenticate to your GCP project.
Configuration
Create a config.yaml file with the following structure in technology-xml-parser-r1-lm-dev:

    ```
    gcs:
    BUCKET_NAME: "your-bucket-name-{}-suffix" 
    BUCKET_OUTPUT: "gs://your-output-bucket/output-folder/"
    XML_FILE_PATH: "gs://your-bucket-name-{}-suffix" 

    spark:
    SPARK_JARS: "path/to/spark-xml_2.12-0.12.0.jar" 

    tech_prefixes:
    2G: "IM/provider/2G/enmranbar1"
    3G: "IM/provider/3G/enmranleg1"
    4G: "IM/provider/4G"
    5G: "IM/provider/5G/enmtbl1"

    xml_config:
    rowTag: configData

    output_config:
    output_prefix: "parquet_data"
    save_mode: "overwrite"

    flattening_config:
    2G:
        # ... flattening steps for 2G ...
    3G:
        # ... flattening steps for 3G ...
    4G:
        # ... flattening steps for 4G ...
    5G:
        # ... flattening steps for 5G ...
    ```

3. Replace the placeholder values with your actual bucket names, file paths, and other configuration details.

### Flattening Configuration

The `flattening_config` section in `config.yaml` defines how the XML data is flattened for each technology. It uses a list of dictionaries, where each dictionary represents a data transformation step.

**Supported Operations**

*   `select`: Selects and renames columns.
    *   `columns`: A dictionary mapping original column names to new names.
    ```yaml
    - operation: select
      columns:
        original_col1: new_col1
        original_col2: new_col2
    ```
*   `explode`: Explodes an array or map column into multiple rows.
    *   `column`: The column to explode.
    *   `alias`: The alias for the exploded column.
    ```yaml
    - operation: explode
      column: array_column
      alias: exploded_item
    ```
*   `filter`: Filters rows based on a condition.
    *   `condition`: The filter condition as a string.
    ```yaml
    - operation: filter
      condition: "col1 > 10"
    ```
*   `join`: Joins the DataFrame with another DataFrame.
    *   `on`: The join condition.
    *   `how`: The join type ("inner", "left_outer", etc.).
    ```yaml
    - operation: join
      on: "id"
      how: "left_outer"
    ```
*   `groupBy`: Groups the DataFrame by specified columns.
    *   `columns`: A list of columns to group by.
    ```yaml
    - operation: groupBy
      columns: ["col1", "col2"]
    ```
*   `agg`: Performs aggregations on the grouped data.
    *   `aggregations`: A dictionary mapping output column names to aggregation functions.
    ```yaml
    - operation: agg
      aggregations:
        sum_col3: sum(col3)
        avg_col4: avg(col4)
    ```
*   `orderBy`: Sorts the DataFrame by specified columns.
    *   `columns`: A dictionary mapping column names to sort order (True for ascending, False for descending).
    ```yaml
    - operation: orderBy
      columns:
        col1: True
        col2: False
    ```
*   `union`: Unions the DataFrame with another DataFrame.
    ```yaml
    - operation: union
    ```
*   `distinct`: Removes duplicate rows from the DataFrame.
    ```yaml
    - operation: distinct
    ```
*   `withColumn`: Adds or replaces a column.
    *   `column`: The name of the new column.
    *   `value`: A literal value for the new column.
    *   `func`: An expression to evaluate for the new column.
    ```yaml
    - operation: withColumn
      column: new_col
      value: "new value"
    - operation: withColumn
      column: calculated_col
      func: "col('col1') * 2"
    ```

**Example**

```yaml
flattening_config:
  2G:
    - operation: select
      columns:
        "xn:SubNetwork._id": "SubNetworkId"
    - operation: explode
      column: "xn:SubNetwork.xn:MeContext"
      alias: "MeContext"
    # ... more steps for 2G ...
  ```

## Usage

For **local testing**, run the script with the following command-line arguments:
 ```Bash
python main.py \
    --start-date YYYYMMDD \
    --end-date YYYYMMDD \
    --lm "comma,separated,list,of,local,markets" \
    --techs "comma,separated,list,of,technologies"
```

* --start-date: Start date for processing data in YYYYMMDD format.
* --end-date: End date for processing data in YYYYMMDD format.
* --lm: Comma-separated list of local markets (e.g., "US,CA,UK").
* --techs: Comma-separated list of technologies to process (e.g., "2G,4G,5G").
  
**Example**
 ```Bash
python main.py \
    --start-date 20241018 \
    --end-date 20241019 \
    --lm "US,CA" \
    --techs "2G,4G,5G" 
 ```

Otherwise upload the DAG file (main.py) and the PySpark job code to the designated GCS bucket (as specified in airflow.cfg).
Make sure the config.yaml file is accessible to the PySpark job (e.g., in the same GCS bucket).
Trigger the Airflow DAG to run the XML processing workflow.

## Testing

This project uses `pytest` for unit testing. 

### Running Tests

1. Make sure you have `pytest` installed:
   ```bash
   pip install pytest
2. Run the tests from the command line:
    ```bash
    pytest test_main.py  
    ```

#### Test Structure
The tests are located in a separate file (i.e, test_main.py) and use pytest fixtures to manage the SparkSession and mock the GCS client for efficient and isolated testing.

The test suite includes tests for:

* Loading configuration from the YAML file.
* Parsing and validating command-line arguments.
* Creating a SparkSession.
* Flattening XML files for different technologies.
* Reading XML files from GCS.
* Saving Spark DataFrames as Parquet files in GCS.
#### Adding Tests
To add new tests, create a new function in the test file with a name starting with test_. Use assert statements to verify the expected behavior of your code. You can also use pytest fixtures to manage test dependencies and resources.


## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.





