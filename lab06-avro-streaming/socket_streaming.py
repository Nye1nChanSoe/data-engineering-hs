from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split


"""
Run:
  nc -lk 5555
"""

spark = SparkSession.builder.appName("HSUStructuredStreamingDemo").getOrCreate()
lines = (
    spark.readStream.format("socket")
    .option("host", "127.0.0.1")
    .option("port", 5555)
    .load()
)
words = lines.select(explode(split(lines.value, " ")).alias("word"))
word_counts = words.groupBy("word").count()
query = word_counts.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()
