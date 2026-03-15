class InvoiceStreamBatchTestSuite():
    def __init__(self):
        self.output_path = '/tmp/delta-invoice/'
        self.checkpoint_path = '/tmp/checkpoint-invoice/'
        self.test_path = '/data/test/json/'
        self.spark = None

    
    def cleanUp(self):
        import subprocess
        subprocess.run(['hadoop' , 'fs' , '-rm' ,'-r', f'{self.output_path}*'] )
        subprocess.run(['hadoop' , 'fs' , '-rm' ,'-r', f'{self.checkpoint_path}*'] )
        subprocess.run(['hadoop' , 'fs' , '-rm' , f'{self.test_path}*.json'] )

    def ingestionFile(self, itr):
        import subprocess
        subprocess.run(['hadoop' , 'fs' , '-cp' ,f"/data/json/invoices-{itr}.json" , '/data/test/json/'] ,check=True)

    def assertResult(self , expected_result):
        df = self.spark.read.format("delta")\
                            .load(path = '/tmp/delta-invoice/' )
        actual_value  = df.selectExpr("count(*) as count_data").collect()[0][0]
        assert expected_result == actual_value , f"The the actual value is {actual_value} but the expected value is {expected_result}"

    def microBatchTime(self , sleepTime = 15):
        import time 
        time.sleep(sleepTime)
    
    def runStreamTestCases(self):

        self.cleanUp()
        print("Clean Up ended  ...........")

        from A_Stream_Batch import invoiceStreamBatch
        iss = invoiceStreamBatch()
        streamingQuery = iss.main("30 seconds")
        self.spark = iss.spark_val

        

        self.ingestionFile(1)
        print("First Ingestion Started........")
        self.microBatchTime()
        streamingQuery.processAllAvailable()
        self.assertResult(1253)
        print("first Ingestion Completed And Successfully Completed the test Cases")


        self.ingestionFile(2)
        print("Second Ingestion Started........")
        self.microBatchTime()
        streamingQuery.processAllAvailable()
        self.assertResult(2510)
        print("Second Ingestion Completed And Successfully Completed the test Cases")


        self.ingestionFile(3)
        print("Third Ingestion Started........")
        self.microBatchTime()
        streamingQuery.processAllAvailable()
        self.assertResult(3994)
        print("Third Ingestion Completed And Successfully Completed the test Cases")

        streamingQuery.stop()
        print("WE ARE GOOD GO With STREAM.............")
    

    def runBatchTestCases(self):
        from A_Stream_Batch import invoiceStreamBatch

        self.cleanUp()
        print("Clean Up ended  ...........")

        iss = invoiceStreamBatch()


        print("Batch First Ingestion Started........")
        self.ingestionFile(1)
        self.ingestionFile(2)
        print("Batch First Ingestion Started........")
        iss.main("batch")                                # After process it will automatically stop the streaming query
        self.microBatchTime()
        self.assertResult(2510)
        print("First Batch Ingestion Completed And Successfully Completed the test Cases") 


        print("Batch Second Ingestion Started........")
        self.ingestionFile(3)
        print("Batch Ingestion Started........")
        iss.main("batch")                               # Previous one is stoped so reruning the stream
        self.microBatchTime()
        self.assertResult(3994)
        print("Second Batch Ingestion Completed And Successfully Completed the test Cases") 



if __name__ == '__main__':
    ists = InvoiceStreamBatchTestSuite()
    ists.runStreamTestCases()
    ists.runBatchTestCases()






    

    

        