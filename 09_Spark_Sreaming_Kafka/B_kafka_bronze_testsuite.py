class KafkaBrownTestSuite:

    def __init__(self):
        self.spark = None

    def cleanUp(self):

        print("Starting Clean Up.......")
        import subprocess
        subprocess.run(["hadoop" , "fs" , "-rm" ,"-r" , "/tmp/checkpoint-kafka-invoices/*"])
        subprocess.run(["hadoop" , "fs" , "-rm" ,"-r" , "/tmp/delta-kafka-invoices/*"])
        print("Clean Up is Completed........")

    def assertResult(self,expected_result):

        df = (
            self.spark.read.format("delta")
                           .load("/tmp/delta-kafka-invoices/")
        )

        actual_result = df.selectExpr("count(*) as total_count").collect()[0][0]

        assert actual_result == expected_result , f"The actual result is {actual_result} but expected result {expected_result}"

    def waitMicroBatchTime(self):
        print("Waiting for 30 Seconds.......")
        import time
        time.sleep(30)
        print("Waiting is completed...")

    
    def start(self):

        from A_kafka_bronze import Brownze

        self.cleanUp()

        bc = Brownze()


        print("First Test case Started.........")
        bzquery = bc.main()
        self.spark = bc.spark
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


        # Print("Third Test case Started.........")
        # # We have to pull the data from some perticular time exm - timestamp for 10 messages 20-oct and 20 messages 21 -oct 
        # # 20-oct and 21 -oct we have to convert these date to starttime seconds for 20-oct -> 126463637 21-oct -> 868357388
        # # it will try to pull the records from 21-oct only 

        # import subprocess
        # subprocess.run(["hadoop" , "fs" , "-rm" ,"-r" , "/tmp/checkpoint-kafka-invoices/*"]) # we have clean the checkpoint,check point ensure the incremental processing
        # bzquery = bc.main(1576848749796)
        # self.waitMicroBatchTime()
        # bzquery.stop()
        # self.assertResult(30)
        # print("Third Test Case Passed.....")

kbts = KafkaBrownTestSuite()
kbts.start()






    
