class Brownze():

    def __init__(self):

        self.bootstrap_server = 'localhost:9092'
        self.jass_module = "org.apache.kafka.common.security.plain.PlainLoginModule"
        self.api_key = "<Api key>"
        self.api_secret = "api_secret"
        self.spark = None

    def sparkSession(self):
        from pyspark.sql import SparkSession
        sparksession = (
                        SparkSession.builder
                                .appName("Kafka")
                                .master("local[*]")
                                .config("spark.jars.packages" , "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0")
                                .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                                .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                .config("spark.sql.adaptive.enabled","false")
                                .getOrCreate()
        )
        sparksession.sparkContext.setLogLevel("ERROR")

        return sparksession

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


    def ingestFromKafka(self, startingtime =1):

        return  (
                    self.spark.readStream.format("kafka")
                                         .option("kafka.bootstrap.servers" , self.bootstrap_server)
                    #                     .option("kafka.security.protocol" , 'SASL_SSL')
                    #                     .option("kafka.sasl.mechanism" , "PLAIN")
                    #                     .option("kafka.sasl.jass.config" , f"{JAAS_MODULE} required username = '{CLUSTER_API_KEY}' password = '{CLUSTER_API_SECRET  }';")
                                          .option("subscribe" , "invoices")
                                          .option("maxOffsetsPerTrigger" , 10 )  # Every microbatch will take only 10 records to evenly distribute the records
                                          .option("startingTimestamp" , startingtime) # from which date we have to pull the data startingtime(long converted number of seconds)
                                          .load()
        )

    def getInvoice(self , kafkaDf):
        from pyspark.sql.functions import from_json
        return (
            kafkaDf.select(
                             kafkaDf.key.cast("string").alias("key"),
                             from_json(kafkaDf.value.cast("string"),self.getSchema()).alias("value"),
                             "topic",
                             "timestamp"
                        )
              )
    
    def main(self , startingtime = 1):

        print("Starting Brownze Kafka Process.........")
        self.spark = self.sparkSession()
        kafka_df = self.ingestFromKafka(startingtime)
        invoice_df = self.getInvoice(kafka_df)

        sQuery = (
                    invoice_df.writeStream.format("delta")
                                          .queryName("Kafka-Ingetion")
                                          .outputMode("append")
                                          .option("checkpointLocation" , "/tmp/checkpoint-kafka-invoices")
                                          .start("/tmp/delta-kafka-invoices")
                    )

        print("Data Has Been Successfully Inserted")
        return sQuery