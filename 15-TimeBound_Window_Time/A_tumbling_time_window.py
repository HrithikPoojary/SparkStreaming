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

class TradeSummary:
    def __init__(self):
        self.spark = None

    def readKafka(self):
        return (
            spark.readStream.table("kafka_bz3")
        )

    def getSchema(self):
        from pyspark.sql.types import StructType , StructField , StringType  , DoubleType
        return (
            StructType(
                [
                    StructField("CreatedTime" , StringType()),
                    StructField("Type" , StringType()),
                    StructField("Amount" , DoubleType()),
                    StructField("BrokerCode" , StringType())
                ]
            )
        )

    def getTrade(self , kafka_df):
        from pyspark.sql.functions import expr , from_json  
        return (
            kafka_df.select(from_json(kafka_df.value , self.getSchema()).alias("value"))
                   .select("value.*")
                   .withColumn("CreatedTime" , expr("to_timestamp(CreatedTime , 'yyyy-MM-dd HH:mm:ss')"))
                   .withColumn("Buy" ,expr("case when Type = 'BUY' then Amount else 0 end"))
                   .withColumn("Sell" , expr("case when Type = 'SELL' then Amount else 0 end"))
        )

    def getAggregate(self , trade_df):
        from pyspark.sql.functions import window , sum , col
        return (
                    trade_df.groupBy(window(trade_df.CreatedTime , "15 minutes"))       # gives structtype of timewindow like 0-15,15-30...
                                .agg(sum("Buy").alias("TotalBuy"),
                                    sum("Sell").alias("TotalSell"))
                                .select("window.start" , "window.end" , col("TotalBuy") , col("TotalSell"))
                )

    def saveResult(self, aggregate_df):
        return (
                    aggregate_df.writeStream.format("delta")
                                            .queryName("Brownze-Integration")
                                            .option("checkpointLocation" , '/tmp/checkpoint-windowtime/')
                                            .outputMode("complete")
                                            .toTable("trade_summary3")
                )

    def process(self):
        print("Strting Brownze Layer...........")
        kafka_df = self.readKafka()
        trade_df = self.getTrade(kafka_df)
        aggregate_df = self.getAggregate(trade_df)
        result = self.saveResult(aggregate_df)
        print("Brown Layer is Completed...........")
        return result 


    