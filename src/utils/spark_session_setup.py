# Import the SparkSession class from PySpark to create a Spark session
from pyspark.sql import SparkSession

# Import the 'col' function, useful for working with DataFrame columns
from pyspark.sql.functions import col

# Start building the Spark session
# This is the starting point for any PySpark program
# It lets you use DataFrames and run Spark operations
spark = SparkSession.builder \
    .appName("MySparkApp") \       # Set the application name for logs and Spark UI
    .getOrCreate()                 # Get an existing session or create a new one

# Explanation of the session builder:
# SparkSession.builder → Starts the session configuration
# .appName("MySparkApp") → Names your application
# .getOrCreate() → Reuses an existing Spark session if there is one, or makes a new one

# Now you can use 'spark' to load, transform, and analyze data
# For example, you can read a CSV or create a DataFrame manually
