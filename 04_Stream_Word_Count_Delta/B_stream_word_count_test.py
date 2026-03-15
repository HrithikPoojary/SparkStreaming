class StreamWCTest():
    def __init__(self):
        self.test_path = '/data/test/text*'
        self.delta_path = '/tmp/delta-wordcount/'
        self.spark = None

    
    def cleanUpFolder(self):
        import subprocess
         
        subprocess.run(f"hadoop fs -rm -r {self.test_path}" , shell = True)
        subprocess.run(f"hadoop fs -rm -r {self.delta_path}" , shell = True)
        subprocess.run("hadoop fs -rm -r /tmp/checkpoint-wordcount" , shell = True)
    
    def ingestData(self,itr):
        import subprocess

        src = "/data/txt/"
        dts = "/data/test/"

        subprocess.run(["hadoop","fs","-cp" , f"{src}text_data_{itr}.txt" , dts] , check = True)

    def assertResult(self, expected_result):

        df = (
               self.spark.read.format("delta")
                             .load(self.delta_path)
        )

        actual_value =  (
                df.where( "substr(word , 1,1) = 's'")
                .selectExpr("sum(count) as sum")
                .collect()[0][0]
        )
        print(expected_result , actual_value)
        assert expected_result== actual_value , f"Test failed Actual is {actual_value}"

    def runTests(self):

        from A_stream_word_count import StreamWC
        import time
        self.cleanUpFolder()
        swt = StreamWC()
        query = swt.mainWordCount()
        self.spark = swt.spark 


        print("First Iteration")
        self.ingestData(1)
        query.processAllAvailable()
        time.sleep(15)
        self.assertResult(25)
        print("First Iteration Completed")

        print("Second Iteration")
        self.ingestData(2)
        query.processAllAvailable()
        time.sleep(15)
        self.assertResult(32)
        print("Second Iteration Completed")

        print("Third Iteration")
        self.ingestData(3)
        query.processAllAvailable()
        time.sleep(15)
        self.assertResult(37)
        print("Third Iteration Completed")

        query.stop()
        

st = StreamWCTest()
st.runTests()
print("We are Good to go............")

