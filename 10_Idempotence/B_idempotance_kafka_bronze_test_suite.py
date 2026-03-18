class KafkaBrownTestSuite:

    def __init__(self):
        self.spark = None

    def cleanUp(self):

        print("Starting Clean Up.......")
        import subprocess
        subprocess.run(["hadoop" , "fs" , "-rm" ,"-r" , "/tmp/checkpoint-kafka-invoices/*"])
        subprocess.run(["hadoop" , "fs" , "-rm" ,"-r" , "/tmp/delta-kafka-invoices/*"])
        delta_table = '/home/hrithik_poojary/Spark_Streaming/SparkStreaming/10_Idempotence/spark-warehouse/'
        subprocess.run(["rm" ,"-rf" , f"{delta_table}"])
        self.spark.sql("drop table if exists invoice_bz")
        print("Clean Up is Completed........")

    def assertResult(self,expected_result):

        df = (
            self.spark.read.table("invoice_bz")
        )

        actual_result = df.selectExpr("count(*) as total_count").collect()[0][0]

        assert actual_result == expected_result , f"The actual result is {actual_result} but expected result {expected_result}"

    def waitMicroBatchTime(self):
        print("Waiting for 30 Seconds.......")
        import time
        time.sleep(60)
        print("Waiting is completed...")

    
    def start(self):

        from A_idempotance_kafka_bronze import Brownze
        
        bc = Brownze()
        self.spark = bc.spark
        self.cleanUp()


        schema = bc.getSchema()

        self.spark.sql(f"""
                                CREATE TABLE invoice_bz (
                                                key STRING,
                                                value STRUCT<
                                                    InvoiceNumber: STRING,
                                                    CreatedTime: INT,
                                                    StoreID: STRING,
                                                    PosID: STRING,
                                                    CashierID: STRING,
                                                    CustomerType: STRING,
                                                    CustomerCardNo: STRING,
                                                    TotalAmount: FLOAT,
                                                    NumberOfItems: INT,
                                                    PaymentMethod: STRING,
                                                    TaxableAmount: FLOAT,
                                                    CGST: FLOAT,
                                                    SGST: FLOAT,
                                                    CESS: FLOAT,
                                                    DeliveryType: STRING,
                                                    DeliveryAddress: STRUCT<
                                                    AddressLine: STRING,
                                                    City: STRING,
                                                    State: STRING,
                                                    PinCode: STRING,
                                                    ContactNumber: STRING
                                                    >,
                                                    InvoiceLineItems: ARRAY<
                                                    STRUCT<
                                                        ItemCode: STRING,
                                                        ItemDescription: STRING,
                                                        ItemPrice: FLOAT,
                                                        ItemQty: INT,
                                                        TotalValue: FLOAT
                                                    >
                                                    >
                                                >,
                                                topic STRING,
                                                timestamp TIMESTAMP
                                                )
                                                USING DELTA
        """)
        print("Table has been created")

        print("First Test case Started.........")
        bzquery = bc.main()
        self.waitMicroBatchTime()
        bzquery.stop()
        self.assertResult(30)
        print("First Test Case Passed.....")


        print("Second Test case Started.........")
        bzquery = bc.main()
        self.waitMicroBatchTime()
        bzquery.stop()
        self.assertResult(30)
        print("Second Test Case Passed.....")


        print("Third Test case Started.........")
        import subprocess
        import time 
        subprocess.run(["hadoop" , "fs" , "-rm" ,"-r" , "/tmp/checkpoint-kafka-invoices/*"])
        bzquery = bc.main(int((time.time()-(24*60*60))*1000))
        self.waitMicroBatchTime()
        bzquery.stop()
        self.assertResult(30)
        print("Third Test Case Passed.....")

kbts = KafkaBrownTestSuite()
kbts.start()






    
