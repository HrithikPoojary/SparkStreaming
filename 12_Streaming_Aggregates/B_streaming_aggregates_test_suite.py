class StreamingAggTestSuite:
    def __init__(self):
        self.base_dir = None
        self.spark = None

    def cleanUP(self):
        print("Clean Up has started..............")
        import subprocess
        subprocess.run(['hadoop' , 'fs' ,"-rm" , "-r" , "/data/test/json/*"])
        subprocess.run(['hadoop' , 'fs' , "-rm" , "-r" , f"/tmp/checkpoint_stream_agg_bz/*"])
        subprocess.run(['hadoop' , 'fs' , "-rm" , "-r" , f"/tmp/checkpoint_stream_agg_gl/*"])
        subprocess.run("bash -c 'rm -rf /home/hrithik_poojary/Spark_Streaming/SparkStreaming/12_Streaming_Aggregates/spark-warehouse/{*,.*}'", shell = True)
        print("Clean Up is completed...............")


    def ingestion(self,itr):
        import subprocess
        subprocess.run(['hadoop' , 'fs' , "-cp" , f'/data/json/invoices-{itr}.json' , f"/data/test/json/"])
        print(f"Ingestion is completed for the {itr}")

    def waitforMicroBatch(self ,sleep=60):
        print("Waiting for 30 seconds")
        import time 
        time.sleep(sleep)
        print("Completed 30 Seconds.................")

    def assertResultBrownze(self , expected_result):

        actual_result = self.spark.sql("select count(*) from delta_stream_agg_bz").collect()[0][0]
        assert expected_result == actual_result , f"Failed Due to Mismatch Actual - {actual_result}  Expected - {expected_result}"

    def assertResultGold(self , expected_result):
        
        actual_result = self.spark.sql("select totalAmount from delta_stream_agg_gl where CustomerCardNo=='2262471989'").collect()[0][0]

        assert expected_result == actual_result , f"Failed Due to Mismatch Actual - {actual_result}  Expected - {expected_result}"


    def runtestCases(self):
        from A_streaming_aggregates import Brownze , Gold ,spark 
        self.cleanUP()


        bz = Brownze()
        bzQuery = bz.process()
        self.spark = spark

        gl = Gold()
        glQuuery = gl.process()


        print("First Test Case Running............")
        self.ingestion(1)
        self.waitforMicroBatch()
        self.assertResultBrownze(501)
        self.assertResultGold(36859)
        print("First Test Case Passeddddddd............")


        print("Second Test Case Running............")
        self.ingestion(2)
        self.waitforMicroBatch()
        self.assertResultBrownze(501+500)
        self.assertResultGold(36859+20740)
        print("Second Test Case Passeddddddd............")

        print("Third Test Case Running............")
        self.ingestion(3)
        self.waitforMicroBatch()
        self.assertResultBrownze(501+500+590)
        self.assertResultGold(36859+20740+31959)
        print("Third Test Case Passeddddddd............")

        bzQuery.stop()
        glQuuery.stop()

if __name__ == '__main__':
    sats = StreamingAggTestSuite()
    sats.runtestCases()