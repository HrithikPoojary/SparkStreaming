from A_Stream_word_count_class import StreamWC

class StreamWCTestSuite():
    def __init__(self):
        self.dase_dir_date = '/data'
        self.spark = None # We will capture this later

    def cleanUpTest(self):
        print("Start Clean Up")
        import subprocess
        # Corrected path to match your output_path variable
        subprocess.run(["rm -rf /home/hrithik_poojary/spark_meta/spark_warehouse/emp"], shell=True)
        subprocess.run(["rm -rf /home/hrithik_poojary/spark_meta/spark_warehouse/checkpoint"], shell=True)
        print("End Clean Up")

    def ingestData(self, itr):
        print(f"Starting Ingest {itr}")
        import subprocess
        src = f"/data/txt/text_data_{itr}.txt"
        dst = "/data/test/"
        subprocess.run(['hdfs', 'dfs', '-cp', '-f', src, dst]) # Added -f to overwrite if exists
        print("Ingestion is Completed") 

    def assertResult(self, expected_result):
        import os
        # Path without the file:// prefix for the os check
        local_path = '/home/hrithik_poojary/spark_meta/spark_warehouse/emp'
            # 2. Table exists, now read it
        df = (
            self.spark.read.format("delta")
                .load(path=local_path)
        )
        actual_value = (
            df.where("substr(word,1,1) = 's'")
                .selectExpr("sum(count) as sum")
                .collect()[0][0]
        )
        
        actual_value = actual_value if actual_value is not None else 0
        assert actual_value == expected_result, f"Test failed! Actual: {actual_value}, Expected: {expected_result}"

    def runtests(self):
        from A_Stream_word_count_class import StreamWC
        import time
        import subprocess
        
        self.cleanUpTest()
        wc = StreamWC()
        
        # 1. Start the query
        sQuery = wc.wordCount()
        
        # 2. CAPTURE the session so the test suite can use it
        self.spark = wc.spark() 

        print("First Iteration")
        self.ingestData(1)
        print("Waiting for data to process...")
        time.sleep(15) # Delta needs a few seconds to commit the JSON log
        self.assertResult(0)
        print("First Iteration is complete")

        # ... (repeat for other iterations)

        subprocess.run("hdfs dfs -rm -f /data/test/*.txt", shell=True)
        
        sQuery.stop()
        

stbts = StreamWCTestSuite()
stbts.runtests()