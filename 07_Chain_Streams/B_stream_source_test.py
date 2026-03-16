class medallianAproachTestSuite():
    def __init__(self):
        self.spark = None


    def cleanUp(self):

        print("Started Cleaning process")
        import subprocess
        subprocess.run(['hadoop' , 'fs' , '-rm', '/data/test/json/*.json'])
        subprocess.run(['rm' ,'-rf' ,'/home/hrithik_poojary/data/archive/invoice/*'])
        subprocess.run(['hadoop' ,'fs' , '-rm' , '-r','/tmp/delta-invoice_bz/*'])
        subprocess.run(['hadoop' ,'fs' ,'-rm' , '-r','/tmp/checkpoint-invoice_bzz/*'])
        subprocess.run(['hadoop' ,'fs' ,'-rm' , '-r','/tmp/checkpoint-invoice_sl/*'])
        subprocess.run(['hadoop' ,'fs' ,'-rm' , '-r' , '/tmp/delta-invoice_sl/*'])
        print('Clean Up is Completed')

    def ingestion(self, itr):
        print(f"{itr} is started")

        import subprocess 
        subprocess.run(['hadoop' , 'fs' , '-cp' , f'/data/json/invoices-{itr}.json' , '/data/test/json/'])

        print(f"{itr} is Completed")

    def assertResult(self , expected_result):
        df = self.spark.read.format("delta")\
                       .load(path = '/tmp/delta-invoice_sl/')

        actual_result= (
            df.selectExpr("count(*) as count_records").collect()[0][0]
        )

        assert actual_result == expected_result , f"Actual is {actual_result} but expected value is {expected_result}"

    def batchTime(self, sleep = 15):
        import time 
        print(f"Waiting for {sleep}seconds")
        time.sleep(sleep)



    def runTestCases(self):
        from A_stream_source import Brown , Silver , spark

        self.spark = spark
        self.cleanUp()

        br = Brown()
        brQuery = br.main()


        print("Starting First Iteration")
        self.ingestion(1)
        brQuery.processAllAvailable()

        sl = Silver()
        slQuery = sl.main() 
        slQuery.processAllAvailable()

        self.batchTime(15)
        self.assertResult(1253)
        print("For the first process test cases are passed...........")


        print("Starting Second Iteration")
        self.ingestion(2)
        self.batchTime(15)
        self.assertResult(2510)
        print("For the second process test cases are passed...........")


        print("Starting third Iteration")
        self.ingestion(3)
        self.batchTime(15)
        self.assertResult(3994)
        print("For the third process test cases are passed...........")

        brQuery.stop()
        slQuery.stop()


        print("Started validating Archived data files ............")
        import os
        archive_files = ['invoices-1.json' , 'invoices-2.json' , 'invoices-3.json'] 
        for i in os.listdir('/home/hrithik_poojary/data/archive/invoice//data/test/json/'):
            assert i in archive_files , f"{i} Is not found"

        print("I am Good to Sleep")


mel  = medallianAproachTestSuite()
mel.runTestCases()