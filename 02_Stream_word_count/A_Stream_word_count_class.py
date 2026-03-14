class StreamWC():
    def __init__(self):
        self.base_dir = "/data"
        self.output_path = "file:///home/hrithik_poojary/spark_meta/spark_warehouse/emp"
        self.checkpoint = "file:///home/hrithik_poojary/spark_meta/spark_warehouse/checkpoint/wordcount"
        self.spark_session = None

    def spark(self):
        from pyspark.sql import SparkSession

        # Only create a new session if one doesn't exist
        jar_dir = "/home/hrithik_poojary/jars"
        delta_core = f"{jar_dir}/delta-spark_2.12-3.2.0.jar"
        delta_storage = f"{jar_dir}/delta-storage-3.2.0.jar"
        cp_jars = f"{delta_core}:{delta_storage}"
        spark_jars = f"{delta_core},{delta_storage}"

        self.spark_session = (SparkSession.builder
            .appName("StreamingDelta")
            .master("local[2]")
            .config("spark.driver.extraClassPath", cp_jars)
            .config("spark.executor.extraClassPath", cp_jars)
            .config("spark.jars", spark_jars)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate())

        return self.spark_session


    def getRawData(self, spark):
        from pyspark.sql.functions import split, explode
        return spark.readStream.format("text").load(f"{self.base_dir}/test/").select(
            explode(split("value", " ")).alias("word")
        )

    def getQualityData(self, raw_data):
        from pyspark.sql.functions import trim, lower, col
        return raw_data.select(lower(trim(col("word"))).alias("word")) \
                       .where("word != '' AND word IS NOT NULL") \
                       .where(col("word").rlike("[a-z]"))

    def getWordCount(self, quality_data):
        return quality_data.groupBy("word").count()

    def overwriteWordCount(self, grouped_data):
        # IMPORTANT: Removed spark.stop() logic entirely.
        return (grouped_data.writeStream
                .format("delta")
                .outputMode("complete")
                .option("checkpointLocation", f"{self.checkpoint}")
                .start(self.output_path))

        # Optional: Add a console logger to see live updates
        # console_query = grouped_data.writeStream \
        #     .format("console") \
        #     .outputMode("complete") \
        #     .start()



    def wordCount(self):
        print("Streaming Job Starting...")
        spark_session = self.spark()
        raw_data = self.getRawData(spark_session)
        quality_data = self.getQualityData(raw_data)
        grouped_data = self.getWordCount(quality_data)
        squery = self.overwriteWordCount(grouped_data)
        
        #print(f"Query Started: {squery.id}")
        return squery

sw = StreamWC()
sw.wordCount()