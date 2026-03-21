
def sparkSession():
    from pyspark.sql import SparkSession
    sparksession = ( SparkSession.builder
                                    .appName("TimeWindow")
                                    .master("local[*]")
                                    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                                .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                .config("spark.jars.packages","io.delta:delta-spark_2.12:3.2.0")
                                .config("spark.sql.adaptive.enabled","false")
                                .enableHiveSupport()
                                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                                .getOrCreate()
    )
    sparksession.sparkContext.setLogLevel("ERROR")
    return sparksession

spark = sparkSession()

class SlidingWindow:
    def __init__(self):
        pass


    def readSensor(self):
        return spark.readStream.table("kafka_sensor")

    def getSchema(self):
        import pyspark.sql.types import StructType,StructField , FloatType , StringType
        return (
                StructType(
                            [
                                StructField("CreatedTime" , StringType(),nullable = False),
                                StructField("Reading" , FloatType())
                            ]
                        )
                    )     

    def getsensor(self, read_sensor_df):
        from pyspark.sql.functions import expr,from_json
        return (
                read_sensor_df.select(read_sensor_df.key.cast("string").alias("SensorId"),
                                        from_json(read_sensor_df.value.cast("string") , self.getSchema()).alias("value"))
                              .select("SensorId" , "value.*")
                              .withColumn("CreatedTime" , expr("to_timestamp(CreatedTime , 'yyyy-MM-dd HH:mm:ss')"))
                    )

    def getAggregate(self , sensor_df):
        from pyspark.sql.functions import window , max 
        return (
                    sensor_df.withWatermark(sensor_df.CreatedTime , "30 minutes")
                             .groupBy(sensor_df.SensorId , window(sensor_df.CreatedTime , "15 minutes" , "5 minutes" ))  # 15 - window time , 5 sliding interval
                                .agg(max("Reading").alias("MaxReading"))
                            .select("SensorId" , "window.start" , "window.end" , "MaxReading")
                )

    def saveResult(self , aggregate_df):
        return (
                    aggregate_df.writeStream.format("delta")
                                            .queryName("Brown-Sliding-Intrgation")
                                            .option("checkpointLocation" ,"/tmp/checkpioint-sliding-sensor/")
                                            .outputMode("complete")
                                            .toTable("sensor_summary")
                )
        print("Done")

    def process(self):
        read_sensor_df = self.readSensor()
        sensor_df = self.getsensor(read_sensor_df)
        aggregate_df = self.getAggregate(sensor_df)
        sQuery = self.saveResult(aggregate_df)
        return sQuery

    