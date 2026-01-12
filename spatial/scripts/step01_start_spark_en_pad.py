# Spark sessie starten en basis pad instellen

from pyspark.sql import SparkSession

# Start Spark sessie (indien nog niet gestart)
spark = SparkSession.builder.appName("GML Inlezen").getOrCreate()

# Basis pad naar de GML-bestanden (pas aan naar jouw locatie)
base_path = "/mnt/processeddata/unzipped_files/"
