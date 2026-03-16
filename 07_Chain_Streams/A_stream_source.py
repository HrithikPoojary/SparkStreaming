from pyspark.sql import SparkSession 
spark  =  (SparkSession.builder
                            .appName("Invoice")
                            .master("local[*]")
                            .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                            .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                            .config("spark.jars.packages","io.delta:delta-spark_2.12:3.2.0")
                            .config("spark.sql.adaptive.enabled","false")
                            .getOrCreate()            
            )
spark.sparkContext.setLogLevel("ERROR")

class Brown():
    def __init__(self):
        self.input_path = '/data/test/json/'
        self.archive_path = '/home/hrithik_poojary/data/archive/invoice/'
        self.checkpoint_path = '/tmp/checkpoint-invoice_bz/'
        self.output_path = '/tmp/delta-invoice_bzz/'
        self.spark_val = None

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
    
    def readDF(self):
        return  (
                    spark.readStream.format("json")
                                         .schema(self.getSchema())
                                         #.option("cleanSouce" , "delete")  # this will delete the processed file (only for directory source)
                                         .option("cleanSource" ,"archive")   # this will move the processed data to archived location which is processed by the previous microbatch
                                         .option("sourceArchiveDir" , self.archive_path)  # location for the archive processed file (self.input_path + self.archive_path (entire structure will be copied))
                                         .load(path = self.input_path)
        )

    def main(self):
        print("Starting Brownze Stream .............")
        rawdf = self.readDF()
        sQuery = (
                rawdf.writeStream                                           # This will create a background thread for every stream
                    .format("delta")                                            
                    .queryName("brown-ingestion-query")                      # Name for the thread we can track this via UI
                    .option("checkpointLocation" , self.checkpoint_path)
                    .outputMode("append")
                    .start(self.output_path)
        )
        print("Done Brown Layer")
        return sQuery


class Silver():
    def __init__(self):
        self.checkpoint_path = '/tmp/checkpoint-invoice_sl/'
        self.output_path = '/tmp/delta-invoice_sl/'
        self.input_path = '/tmp/delta-invoice_bzz/' 

    def readDataFrame(self):
        return (

            spark.readStream.format("delta")
                            .load(path = self.input_path)
        )

    def explodeDF(self , rawdf):
        from pyspark.sql.functions import expr
        return (
                    rawdf.withColumns(
                                        {
                                            "AddressLine" : expr("DeliveryAddress.AddressLine"),
                                            "City" : expr("DeliveryAddress.City"),
                                            "State" : expr("DeliveryAddress.State"),
                                            "PinCode" : expr("DeliveryAddress.PinCode"),
                                            "ContactNumber" : expr("DeliveryAddress.ContactNumber"),
                                            "LineItems" : expr("explode(InvoiceLineItems)")
                                        }
                                    )
                )
    
    def flattenInvoices(self , explodedf ):
        from pyspark.sql.functions import expr
        return (
            explodedf.withColumns(
                                    {
                                        "ItemCode" : expr("LineItems.ItemCode"),
                                        "ItemDescription" : expr("LineItems.ItemDescription"),
                                        "ItemPrice" : expr("LineItems.ItemPrice"),
                                        "ItemQty" : expr("LineItems.ItemQty"),
                                        "TotalValue" : expr("LineItems.TotalValue")
                                    }
                                ).drop('InvoiceLineItems' , 'DeliveryAddress')
                )
            
    def appendFlattenDf(self, flattendf ):
        sQuery = (
                    flattendf.writeStream.format("delta")
                                        .queryName("silver-ingestion-query")
                                        .option("checkpointLocation" , self.checkpoint_path)
                                        .outputMode("append")
                                        .start(self.output_path)                                                            
                 )
        return sQuery      

    def main(self):
        print("Starting Silver Stream .............")
        rawdf = self.readDataFrame()
        explodedf = self.explodeDF(rawdf)
        result = self.flattenInvoices(explodedf)
        sQuery = self.appendFlattenDf(result)
        print("Done by Silver Layer")
        return sQuery
             

    