from pyspark.sql import SparkSession 
spark =  ( SparkSession.builder
            .appName("Demo")
            .master('local[2]')
            .getOrCreate() )

class StreamWCTestSuite():
    def __init__(self):
        self.dase_dir_date = '/data'
    
    def cleanUpTest(self):
        print("Start Clean Up")
        import subprocess
        subprocess.run(["rm -rf /home/hrithik_poojary/spark_meta/spark-warehouse/emp"] ,shell =True) # default = False
        print("End Clean Up")

    def ingestData(self, itr):
        print("Starting Ingest")
        import subprocess
        src = f"/data/txt/text_data_{itr}.txt"
        dst = "/data/test/"
        subprocess.run(['hdfs' , 'dfs' , '-cp' , src , dst])
        print("Ingestion is Completed") 

    def assertResult(self ,expected_result):
        
        df = (
            spark.read.format("parquet")\
                      .option("header" , "true")\
                      .load(path = 'file:///home/hrithik_poojary/spark_meta/spark-warehouse/emp')
        )
        actual_value = (
                df.where("substr(word,1,1) = 's'")
                 .selectExpr("sum(count) as sum")
                 .collect()[0][0]
        )
        assert actual_value == expected_result , f"Test failed Actual is {actual_value}"

    def runtests(self):
        from A_Stream_word_count_class import StreamWC
        import subprocess
        import time
        self.cleanUpTest()
        wc = StreamWC()

        sQuery = wc.wordCount()

        print("First Iteration")
        self.ingestData(1)
        #wc.wordCount()  No need to call manually automaticall cal the function for data
        print(f"Waiting for 30 seconds..")
        time.sleep(30)
        self.assertResult(25)
        print("First Iteration is completed")

        print("Second Iteration")
        self.ingestData(2)
        #wc.wordCount() No need to call manually automaticall cal the function for data
        print(f"Waiting for 30 seconds..")
        time.sleep(30)
        self.assertResult(32)
        print("Second Iteration is completed")

        print("Third Iteration")
        self.ingestData(3)
        #wc.wordCount() No need to call manually automaticall cal the function for data
        print(f"Waiting for 30 seconds..")
        time.sleep(30)
        self.assertResult(37)
        print("Third Iteration is completed")


        subprocess.run("hdfs dfs -rm -r /data/test/*.txt" , shell = True)
        sQuery.stop()


stbts = StreamWCTestSuite()
stbts.runtests()

