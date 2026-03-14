class StreamWC():
    def __init__(self):
        self.input_path = '/data/test/'
        self.output_path = '/tmp/delta-wordcount'
        self.checkpoint_path = '/tmp/checkpoint-wordcount'

        from pyspark.sql import SparkSession 
        self.spark  = (
                        SparkSession.builder
                        .appName("delta-stream")
                        .master("local[*]")
                        .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                        .config("spark.jars.packages","io.delta:delta-spark_2.12:3.2.0")
                        .config("spark.sql.adaptive.enabled","false")
                        .getOrCreate()
                    )
        self.spark.sparkContext.setLogLevel("ERROR")

    def getRawData(self):

        from pyspark.sql.functions import split , explode
        df =  (
            self.spark.readStream.format("text")
                                 .load(path = self.input_path)
        )

        return (
            df.select(explode(split(df.value,' ')).alias("word"))
        )

    def getQualityData(self,getrawdata):

        from pyspark.sql.functions import trim , lower ,col

        return(  
                getrawdata.select(lower(trim(col("word"))).alias("word"))
                            .where("word is not null")
                            .where("word rlike '[a-z]'")
                )

    def getGroupedData(self,qualitydata):
        return (
            qualitydata.groupBy("word").count()
        )

    def overwriteWordCount(self,getgroupedata):
        return (
            getgroupedata.writeStream
                         .format("delta")
                         .outputMode("complete")
                         .option("checkpointLocation" , self.checkpoint_path)
                         .start(self.output_path)
        )

    def mainWordCount(self):
        print("Starting Streaming Process")
        raw = self.getRawData()
        print("Completed Raw Process")
        quality = self.getQualityData(raw)
        print("Completed Quality Process")        
        group = self.getGroupedData(quality)
        print("Completed Grouped Process")   
        query = self.overwriteWordCount(group)
        print("Completed Writing Process")   
        return query
