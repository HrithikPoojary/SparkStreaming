def sparkSession():
    from pyspark.sql import SparkSession 
    sparksession = SparkSession.builder\
                                .appName("StreamingAgg")\
                                .master("local[*]")\
                                .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")\
                                .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")\
                                .config("spark.jars.packages","io.delta:delta-spark_2.12:3.2.0")\
                                .config("spark.sql.adaptive.enabled","false")\
                                .getOrCreate()

    sparksession.sparkContext.setLogLevel("ERROR")

    return sparksession

spark = sparkSession()

class Brownze:
    def __init__(self):
        self.base_dir = None
        self.spark = None

    def getSchema(self):
        from pyspark.sql.types import StructType , StructField , IntegerType , StringType , ArrayType , MapType,DoubleType , FloatType

        return (
                    StructType(
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
                                        StructField("DeliveryAddress" , StructType(
                                                                                        [
                                                                                            StructField("AddressLine" , StringType()),
                                                                                            StructField("City" , StringType()),
                                                                                            StructField("State" , StringType()),
                                                                                            StructField("PinCode" , StringType()),
                                                                                            StructField("ContactNumber" , StringType())
                                                                                        ]
                                                                                    )
                                                ),
                                        StructField("InvoiceLineItems" ,  ArrayType( StructType
                                                                                                (
                                                                                                  [
                                                                                                    StructField("ItemCode" , StringType()),
                                                                                                    StructField("ItemDescription" , StringType()),
                                                                                                    StructField("ItemPrice" , FloatType()),
                                                                                                    StructField("ItemQty" , IntegerType()),
                                                                                                    StructField("TotalValue" , FloatType()) 
                                                                                                  ]              
                                                                                                )
                                                                                    )        
                                                   )
                                    ]                        
                                )
                 )

    def readInvoices(self ):

        from pyspark.sql.functions import input_file_name
        return (spark.readStream.format("json")
                                .schema(self.getSchema())
                                .load(path = f"/data/test/json/")
                                .withColumn("inputFile" , input_file_name())
        )

    def process(self):
        invoicedf =self.readInvoices()
        sQuery =  (
            invoicedf.writeStream.format("delta")
                                 .queryName("brownze-ingestion")
                                 .outputMode("append")
                                 .option("checkpointLocation" , '/tmp/checkpoint_stream_agg_bz')
                                 .toTable('delta_stream_agg_bz')
        )

        print("Done With The Brownze")
        return sQuery


class Gold():
    def __init__(self):
        self.base_dir = None

    def readBronze(self):
        return (
            spark.readStream.table('delta_stream_agg_bz')
            )
        
    def getAggregates(self,readbrowndf):
        from pyspark.sql.functions import sum,expr 
        return (
            readbrowndf.groupBy("CustomerCardNo")
                            .agg(sum("TotalAmount").alias("Totalamount"),
                                  (sum("TotalAmount") * 0.02).alias("TotalPoints"))
        )

    def saveResult(self,aggreagedf):
        return (
            aggreagedf.writeStream.format("delta")
                                  .queryName("gold-integration")
                                  .outputMode("complete")
                                  .option("checkpointLocation" , '/tmp/checkpoint_stream_agg_gl')
                                  .toTable('delta_stream_agg_gl')
        )
        
    def process(self):
        invoices_df = self.readBronze()
        aggregate_df = self.getAggregates(invoices_df)
        sQuery = self.saveResult(aggregate_df)
        print("Done With The Gold")
        return sQuery



    



    