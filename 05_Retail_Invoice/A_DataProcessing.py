from pyspark.sql import SparkSession
from pyspark.sql.types import StructType , StructField , IntegerType , StringType , FloatType , ArrayType , MapType
from pyspark.sql.functions import explode,col

spark = (
            SparkSession.builder
                        .appName("Retail")
                        .master("local[*]")
                        .getOrCreate()
)

data_schema = StructType (
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

df = spark.read.format("json")\
                .schema(data_schema)\
                .load(path = '/data/json/')

df = ( 
        df.withColumns({
                  "AddressLine" : "DeliveryAddress.AddressLine",
                  "City" : "DeliveryAddress.City",
                  "State" : "DeliveryAddress.State",
                  "PinCode" : "DeliveryAddress.PinCode",
                  "ContactNumber" : "DeliveryAddress.ContactNumber"
                    })
                    .withColumn("InvoiceLineItems" , explode("InvoiceLineItems"))
                    .withColumns({
                        "ItemCode" : "InvoiceLineItems.ItemCode",
                        "ItemDescription" : "InvoiceLineItems.ItemDescription",
                        "ItemPrice" : "InvoiceLineItems.ItemPrice",
                        "ItemQty" : "InvoiceLineItems.ItemQty",
                        "TotalValue" : "InvoiceLineItems.TotalValue"
                    })
                    .drop(col("DeliveryAddress") , col("InvoiceLineItems"))
)

df.show()