class invoiceStream():
    def __init__(self):
        self.input_path = '/data/test/json/'
        self.checkpoint_path = '/tmp/checkpoint-invoice/'
        self.output_path = '/tmp/delta-invoice/'
        self.spark_val = None

    def sparkSession(self):
        from pyspark.sql import SparkSession 
        sparksession =  (SparkSession.builder
                                    .appName("Invoice")
                                    .master("local[*]")
                                    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                                    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                    .config("spark.jars.packages","io.delta:delta-spark_2.12:3.2.0")
                                    .config("spark.sql.adaptive.enabled","false")
                                    .getOrCreate()            
                    )
        sparksession.sparkContext.setLogLevel("ERROR")
        return sparksession

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
                    self.spark_val.readStream.format("json")
                                         .schema(self.getSchema())
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
                                            "Pincode" : expr("DeliveryAddress.Pincode"),
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
            
    def appendFlattenDf(self,flattendf):
        return (
                    flattendf.writeStream.format("delta")
                                        .option("checkpointLocation" , self.checkpoint_path)
                                        .outputMode("append")
                                        .start(self.output_path)   #  toTable() , start() --> Action
        )

    def main(self):
       
        print("Starting Process .....")
        self.spark_val = self.sparkSession()
        rawdf = self.readDF()
        explodedf = self.explodeDF(rawdf)
        result = self.flattenInvoices(explodedf)
        sQuery = self.appendFlattenDf(result)
        print("Done \n")
        return sQuery
             

    