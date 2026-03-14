class StreamData():
    def __init__(self):
        self.input_path = '/data/json/'
        self.output_path = ''
        self.checkpoint_path = ''

        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType , StructField , StringType , IntegerType , FloatType ,ArrayType , MapType
        self.spark = (
                        SparkSession.builder    
                                    .appName("Invoice")
                                    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                                    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                    .config("spark.jars.packages","io.delta:delta-spark_2.12:3.2.0")
                                    .config("spark.sql.adaptive.enabled","false")
                                    .master("local[*]")
                                    .getOrCreate()
                    )
        self.spark.sparkContext.setLogLevel("ERROR")

        self.data_schema = StructType (
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

    def getRawData(self):
        return (
            self.spark.read.format("json")
                            .schema(self.data_schema)
                            .load(path = self.input_path)
        )

    def flatten(self , rawdf):
        from pyspark.sql.functions import explode , col
        return (
                    ( 
                        rawdf.withColumns(
                                            {
                                                "AddressLine" : "DeliveryAddress.AddressLine",
                                                "City" : "DeliveryAddress.City",
                                                "State" : "DeliveryAddress.State",
                                                "PinCode" : "DeliveryAddress.PinCode",
                                                "ContactNumber" : "DeliveryAddress.ContactNumber"
                                            }
                                    )
                        .withColumn("InvoiceLineItems" , explode("InvoiceLineItems"))
                        .withColumns(
                                            {
                                                "ItemCode" : "InvoiceLineItems.ItemCode",
                                                "ItemDescription" : "InvoiceLineItems.ItemDescription",
                                                "ItemPrice" : "InvoiceLineItems.ItemPrice",
                                                "ItemQty" : "InvoiceLineItems.ItemQty",
                                                "TotalValue" : "InvoiceLineItems.TotalValue"
                                            }
                                        )
                        .drop(col("DeliveryAddress") , col("InvoiceLineItems"))
                    )
                )

    def showprintschema(self, flattendf):
        flattendf.printSchema()

    def overWriteFlatten(self,flattenDf):
        return (
                    flattenDf.writeStream.format("delta")
                                         .outputMode("complete")
                                         .option("checkpointLocation" , self.checkpoint_path)
                                         .start(self.output_path)
        )

    def main(self):
        raw = self.getRawData()
        flat = self.flatten(raw)
        self.showprintschema(flat)
    
sc = StreamData()
sc.main()