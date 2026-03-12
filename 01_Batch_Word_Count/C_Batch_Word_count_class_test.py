from pyspark.sql import SparkSession 
spark =  ( SparkSession.builder
            .appName("Demo")
            .master('local[2]')
            .getOrCreate() )

class batchWCTestSuite():
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
        from B_Batch_Word_count_class import batchWC
        import subprocess
        self.cleanUpTest()
        wc = batchWC()

        print("First Iteration")
        self.ingestData(1)
        wc.wordCount()
        self.assertResult(25)
        print("First Iteration is completed")

        print("Second Iteration")
        self.ingestData(2)
        wc.wordCount()
        self.assertResult(32)
        print("Second Iteration is completed")

        print("Third Iteration")
        self.ingestData(3)
        wc.wordCount()
        self.assertResult(37)
        print("Third Iteration is completed")

        subprocess.run("hdfs dfs -rm -r /data/test/*.txt" , shell = True)


wcbts = batchWCTestSuite()
wcbts.runtests()

