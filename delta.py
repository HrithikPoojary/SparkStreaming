from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
from pyspark.sql.functions import lower, trim, col

spark = (
    SparkSession.builder
    .appName("delta-stream")
    .master("local[*]")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages","io.delta:delta-spark_2.12:3.2.0")
    .getOrCreate()
)

df = spark.readStream.format("text").load("file:///home/hrithik_poojary/test/")

words = df.select(explode(split(df.value," ")).alias("word"))

clean = words.select(lower(trim(col("word"))).alias("word"))

counts = clean.groupBy("word").count()
print("Bellooo")

query = (
    counts.writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation","/tmp/checkpoint-wordcount")
    .start("/tmp/delta-wordcount")
)

query.awaitTermination()