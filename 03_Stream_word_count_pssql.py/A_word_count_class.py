import sys 

class StreamWC():
    def __init__(self):
        self.input_path = "hdfs://localhost:9000/data/txt/"
        self.output_path = "file:///home/hrithik_poojary/spark_meta/spark_warehouse/emp"
        self.checkpoint = "file:///home/hrithik_poojary/spark_meta/spark_warehouse/checkpoint/wordcount"
        self.jdbc_url = 'jdbc:postgresql://localhost:5432/spark'
        self.user  = "spark_user"
        self.password = sys.argv[1]
        self.driver  =  'org.postgresql.Driver'

        from pyspark.sql import SparkSession
        self.spark = (
                        SparkSession.builder
                                    .appName("jdbc")
                                    .master('local[*]')
                                    .config("spark.jars.packages" , "org.postgresql:postgresql:42.7.3")
                                    .getOrCreate()
                     )

    def getRawData(self):

        from pyspark.sql.functions import split , explode
        df =  (
                    self.spark.readStream.format("text")
                                   .load(path = self.input_path)
                )
        
        return (
            df.select(explode(split(df.value, ' ')).alias("word"))
        )

    def getQualityData(self , getrawdata):
        from pyspark.sql.functions import col , trim , lower
        return (
            getrawdata.select(lower(trim(col('word'))).alias("word"))
                      .where("word is not null")
                      .where("word rlike '[a-z]'")
        )

    def groupedCount(self , getqualitydata):
        return (
            getqualitydata.groupBy("word").count()
        )

    def show(self,groupedcount):
        print(groupedcount.count())

    def writeToJdbc(self , batchdf , batchid):
        print("Write To Jdbc")
        ( batchdf.write.format("jdbc")
                        .option("url" , self.jdbc_url)
                        .option("dbtable" , "public.word_count")
                        .option("user" , self.user)
                        .option("password" , self.password)
                        .option("driver" , self.driver)
                        .mode("overwrite")
                        .save()
        )
    def overwriteWordCount(self, groupedcount):
        print("Over Write word count")
        return (
                    groupedcount.writeStream
                            .foreachBatch(self.writeToJdbc)
                            .outputMode("complete")
                            .option("checkpointLocation" , self.checkpoint)
                            .start()
                )

    def wordCount(self):
        raw = self.getRawData()
        quality = self.getQualityData(raw)
        group = self.groupedCount(quality)
        #self.show(group)
        query = self.overwriteWordCount(group)
        query.awaitTermination()
if __name__ =='__main__':
    sw = StreamWC()
    sw.wordCount()