class KafkaProducerTestSuite:

    def __init__(self):
        self.spark = None

    def cleanUp(self):
        print("Clean Is Started..............")
        import subprocess
        subprocess.run(['hadoop' , 'fs' , '-rm' ,'-r' , '/tmp/checkpint-kafka-spark/*'])
        subprocess.run(['hadoop' , 'fs' , '-rm' ,'-r' , '/data/test/json/*'])
        print("Cleaning is Done......................")

    def ingestion(self , itr):
        import subprocess
        print(f"{itr} iterations is started..............")
        subprocess.run(['hadoop' , 'fs' , '-cp' ,f'/data/json/invoices-{itr}.json' , '/data/test/json/'])
        print("Data has been insterted successfully")

    def microBatchWait(self ,sleep = 15):
        print(f'Waiting for {sleep} seconds')
        import time 
        time.sleep(sleep)
        print("Waiting is completed........")

    def asserResult(self , starttime , expected_result):
        
        actual_count = ( self.spark.read.format("kafka")
                            .option("kafka.bootstrap.servers" , "localhost:9092")
                            .option("subscribe" , 'invoices')
                            .option("startingTimestamp" , starttime)
                            .option("startingOffsetsByTimestampStrategy" , 'latest')  # default error -> if no records after startingTimestamp by default it will throw error avoid we can use latest it will not give errors
                            .load()
                            .count()
                        )

        assert expected_result == actual_count , f"Actual result {actual_count} Expected result {expected_result}"
        print("Assert Done!!!!!!!")

    def runTestCases(self):
        from A_kafka_producer import KafkaProducer
        import time
        starttime = int(round(time.time()*1000,2))
        self.cleanUp()

        kp = KafkaProducer()
        squery = kp.process(condition = "StoreID == 'STR7188'")
        self.spark = kp.spark 

        self.ingestion(1)
        self.microBatchWait()
        self.asserResult(starttime , 53)
        print("First Iteration is successfully done_____________")

        self.ingestion(2)
        self.microBatchWait()
        self.asserResult(starttime , 53+11)
        print("Second Iteration is successfully done____________")

        self.ingestion(3)
        self.microBatchWait()
        self.asserResult(starttime , 53+11+25)
        print("Third Iteration is successfully done____________")

        squery.stop()

    
kpts = KafkaProducerTestSuite()
kpts.runTestCases()
