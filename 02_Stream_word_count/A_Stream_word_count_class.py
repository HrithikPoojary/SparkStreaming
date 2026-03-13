class StreamWC():

    def __init__(self):
        self.base_dir = "/data"

    def spark(self):
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip

        # Stop existing session if present
        try:
            SparkSession.getActiveSession().stop()
        except:
            pass

        builder = (
            SparkSession.builder
            .appName("Demo")
            .master("local[2]")
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        return spark

    def getRawData(self, spark):
        from pyspark.sql.functions import split, explode

        df = (
            spark.readStream
            .format("text")
            .load(f"{self.base_dir}/test/")
        )

        return df.select(
            explode(split(df.value, " ")).alias("word")
        )

    def getQualityData(self, raw_data):
        from pyspark.sql.functions import trim, lower

        return (
            raw_data
            .select(lower(trim("word")).alias("word"))
            .where("word is not null")
            .where("word rlike '[a-z]'")
        )

    def getWordCount(self, quality_data):
        return (
            quality_data
            .groupBy("word")
            .count()
        )

    def overwriteWordCount(self, grouped_data):

        return (
            grouped_data.writeStream
            .format("delta")
            .outputMode("complete")
            .option(
                "checkpointLocation",
                "file:///home/hrithik_poojary/spark_meta/spark_warehouse/check_point/word_count"
            )
            .start("file:///home/hrithik_poojary/spark_meta/spark_warehouse/emp")
        )

    def wordCount(self):
        print("Streaming Data Inserting to EMP Execution Started")
        spark = self.spark()
        raw_data = self.getRawData(spark)
        quality_data = self.getQualityData(raw_data)
        grouped_data = self.getWordCount(quality_data)
        squery = self.overwriteWordCount(grouped_data)
        print("EMP Created Successfully")
        return squery