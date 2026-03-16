from pyspark.sql import SparkSession
from pyspark.sql.types import StructType , StructField , IntegerType , FloatType , StringType ,ArrayType 
from pyspark.sql.functions import expr
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

schema = StructType(
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


df = (spark.read.format("json").schema(schema)
                                         .load(path = '/data/test/json/'))

df = df.withColumns(
                                        {
                                            "AddressLine" : expr("DeliveryAddress.AddressLine"),
                                            "City" : expr("DeliveryAddress.City"),
                                            "State" : expr("DeliveryAddress.State"),
                                            "PinCode" : expr("DeliveryAddress.PinCode"),
                                            "ContactNumber" : expr("DeliveryAddress.ContactNumber"),
                                            "LineItems" : expr("explode(InvoiceLineItems)")
                                        }
)

df = df.withColumns(
                                    {
                                        "ItemCode" : expr("LineItems.ItemCode"),
                                        "ItemDescription" : expr("LineItems.ItemDescription"),
                                        "ItemPrice" : expr("LineItems.ItemPrice"),
                                        "ItemQty" : expr("LineItems.ItemQty"),
                                        "TotalValue" : expr("LineItems.TotalValue")
                                    }
                                ).drop('InvoiceLineItems' , 'DeliveryAddress')

df = df.withColumns(
                                    {
                                        "ItemCode" : expr("LineItems.ItemCode"),
                                        "ItemDescription" : expr("LineItems.ItemDescription"),
                                        "ItemPrice" : expr("LineItems.ItemPrice"),
                                        "ItemQty" : expr("LineItems.ItemQty"),
                                        "TotalValue" : expr("LineItems.TotalValue")
                                    }
                                ).drop('InvoiceLineItems' , 'DeliveryAddress')

df.selectExpr("count(*) as count").show()


print("Till here we are good")
print("Done")

