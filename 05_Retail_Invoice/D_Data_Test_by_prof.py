class InvoiceStreamTestSuite():
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
    
    def runTestCases(self):

        self.cleanUp()
        print("Clean Up ended  ...........")

        from C_Data_Creation_by_Prof import invoiceStream
        iss = invoiceStream()
        streamingQuery = iss.main()
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
        print("WE ARE GOOD GO .............")

if __name__ == '__main__':
    ists = InvoiceStreamTestSuite()
    ists.runTestCases()






    

    

        