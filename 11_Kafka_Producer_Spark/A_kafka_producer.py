class KafkaProducer:
    def __init__(self):
        self.bootstrap_server = 'localhost:9092'
        self.topic = 'invoices'
        self.spark = None

    def sparkSession(self): 
        from pyspark.sql import SparkSession
        spark_session =  (
                            SparkSession.builder
                                        .appName("Kafka_Reader")
                                        .master("local[*]")
                                        .config("spark.jars.packages" , "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0")
                                        .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                                        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                        .config("spark.sql.adaptive.enabled","false")
                                        .getOrCreate()
                        )
        spark_session.sparkContext.setLogLevel("ERROR")
        return spark_session

    def getSchema(self):
        from pyspark.sql.types import StructType , StructField , IntegerType , StringType , ArrayType, FloatType
        return (
                StructType (
                        [
                            StructField("InvoiceNumber" , StringType()),
                            StructField("CreatedTime" , IntegerType()),
                            StructField("StoreID" , StringType()),
                            StructField("PosID" , StringType()),
                            StructField("CashierID" , StringType()),
                            StructField("CustomerType" , StringType()),
                            StructField("CustomerCardNo" , StringType()),
                            StructField("TotalAmount" , FloatType()),
                            StructField("NumberOfItems" , IntegerType()),
                            StructField("PaymentMethod" , StringType()),
                            StructField("TaxableAmount" , FloatType()),
                            StructField("CGST" , FloatType()),
                            StructField("SGST" , FloatType()),
                            StructField("CESS" , FloatType()),
                            StructField("DeliveryType" , StringType()),
                            StructField(
                                        "DeliveryAddress" , StructType([
                                                                        StructField("AddressLine" , StringType()),
                                                                        StructField("City" , StringType()),
                                                                        StructField("State" , StringType()),
                                                                        StructField("PinCode" , StringType()),
                                                                        StructField("ContactNumber" , StringType())
                                                                        ])
                                        ),

                            StructField("InvoiceLineItems" , ArrayType(
                                                                        StructType([
                                                                        StructField("ItemCode" , StringType()),
                                                                        StructField("ItemDescription" , StringType()),
                                                                        StructField("ItemPrice" , FloatType()),
                                                                        StructField("ItemQty" , IntegerType()),
                                                                        StructField("TotalValue" , FloatType())   
                                                                        ])
                                                                        )
                                    )
                        ]
                    )
                 )


    def readInvoice(self , condition):

        return (
            self.spark.readStream.format("json")
                            .schema(self.getSchema())
                            .load('/data/test/json/')
                            .where(condition)
        )

    def kafkaMessage(self , kafkaRawDf , key):

        return (
            kafkaRawDf.selectExpr(f"{key} as key" , "to_json(struct(*)) as value")
        )

    def kafkaWriter(self , kafkaMsgDf):

        return (
            kafkaMsgDf.writeStream.format("kafka")
                                 .option("kafka.bootstrap.servers" , self.bootstrap_server)
                                 .option("topic" , self.topic)
                                 .option("checkpointLocation" , "/tmp/checkpint-kafka-spark")
                                 .outputMode("append")
                                 .start()
        )

    def process(self,condition):
        print("Spark Kafka Process is Stared ..............")
        self.spark = self.sparkSession()
        readinvoice = self.readInvoice(condition)
        kafkamessage = self.kafkaMessage(readinvoice,"StoreID")
        sQuery = self.kafkaWriter(kafkamessage)
        print("Spark kafka Process is Completed...............")
        return sQuery

    

        

    

    