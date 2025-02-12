from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ResultsReader") \
    .getOrCreate()

# Read anomaly detection results
results = spark.read.parquet("/app/results_combined")

# Show the results
results.show(truncate=False, n=1000)

# Print schema of the results
results.printSchema()