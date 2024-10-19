from pyspark.sql import SparkSession

def create_spark_session(spark_jars):
  """
  Creates and configures a SparkSession.

  Args:
      spark_jars (str): Comma-separated list of JAR files required for the session.

  Returns:
      SparkSession: The configured SparkSession object.
  """
  return (
      SparkSession.builder.appName("parser_for_technology")
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.jars", spark_jars)
      .getOrCreate()
  )
