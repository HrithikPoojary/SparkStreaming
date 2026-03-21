class TestSuiteWindowTime:
    def __init__(self):
        from A_tumbling_time_window import spark
        self.spark = spark

    def cleanUp(self):
        print("Starting Clean Up........")

        import subprocess
        subprocess.run(['hadoop' , 'fs' , '-rm' , '-r' , '/tmp/checkpoint-windowtime/'])
        subprocess.run(['hadoop' , 'fs' , '-rm' , '-r' ,'/user/hive/warehouse/*'])
        subprocess.run("bash -c 'rm -rf /home/hrithik_poojary/Spark_Streaming/SparkStreaming/15-TimeBound_Window_Time/metastore_db/{*,.*}'", shell = True)
        subprocess.run("bash -c 'rm -rf /home/hrithik_poojary/Spark_Streaming/SparkStreaming/15-TimeBound_Window_Time/spark-warehouse/{*,.*}'", shell = True)
        subprocess.run("bash -c 'rm -rf /home/hrithik_poojary/Spark_Streaming/SparkStreaming/15-TimeBound_Window_Time/derby.log'", shell = True)

        self.spark.sql("create table if not exists kafka_bz3(key STRING , value STRING) using delta")
        print("Clean Up Is Copmleted..........")
    
    def waitForMicroBatch(self,sleep = 60):
        print("Waiting for 60 seconds")
        import time
        time.sleep(sleep)
        print("Waiting is completed........")

    def assertTradeSummary(self , start , end , expected_buy , expected_sell):
        print("Starting Trade summary Validation.....")
        df = self.spark.sql(f"""
                                select TotalBuy , TotalSell from trade_summary3
                                where date_format(start , 'yyyy-MM-dd HH:mm:ss') = '{start}'
                                and date_format(end , 'yyyy-MM-dd HH:mm:ss') = '{end}'
                                """)\
                                .collect()
        actual_buy = df[0][0]
        actual_sell = df[0][1]

        assert expected_buy == actual_buy , f"Failed Expected {expected_buy} != Actual {actual_buy}"
        assert expected_sell == actual_sell , f"Failed Expected {expected_sell} != Actual {actual_sell}"

    def runTest(self):
        self.cleanUp()
        from A_tumbling_time_window import TradeSummary
        ts = TradeSummary() 
        sQuery = ts.process()

        print("Testnig First two events....")

        self.spark.sql("""INSERT INTO kafka_bz3 VALUES
                  ('2019-02-05', '{"CreatedTime": "2019-02-05 10:05:00", "Type": "BUY", "Amount": 500, "BrokerCode": "ABX"}'),
                  ('2019-02-05', '{"CreatedTime": "2019-02-05 10:12:00", "Type": "BUY", "Amount": 300, "BrokerCode": "ABX"}')
            """)
        self.waitForMicroBatch()
        self.assertTradeSummary('2019-02-05 10:00:00', '2019-02-05 10:15:00', 800, 0)



        print("\nTesting third and fourth events...") 
        self.spark.sql("""INSERT INTO kafka_bz3 VALUES
                  ('2019-02-05', '{"CreatedTime": "2019-02-05 10:20:00", "Type": "BUY", "Amount": 600, "BrokerCode": "ABX"}'),
                  ('2019-02-05', '{"CreatedTime": "2019-02-05 10:40:00", "Type": "BUY", "Amount": 900, "BrokerCode": "ABX"}')
            """)
        self.waitForMicroBatch()        
        self.assertTradeSummary('2019-02-05 10:15:00', '2019-02-05 10:30:00', 600, 0)
        self.assertTradeSummary('2019-02-05 10:30:00', '2019-02-05 10:45:00', 900, 0)

        print("\nTesting late event...") 
        self.spark.sql("""INSERT INTO kafka_bz3 VALUES
                    ('2019-02-05', '{"CreatedTime": "2019-02-05 10:48:00", "Type": "SELL", "Amount": 500, "BrokerCode": "ABX"}'),
                    ('2019-02-05', '{"CreatedTime": "2019-02-05 10:25:00", "Type": "SELL", "Amount": 400, "BrokerCode": "ABX"}')
            """)
        self.waitForMicroBatch()        
        self.assertTradeSummary('2019-02-05 10:45:00', '2019-02-05 11:00:00', 0, 500)
        self.assertTradeSummary('2019-02-05 10:15:00', '2019-02-05 10:30:00', 600, 400)

        print("Validation passed.\n")        

        sQuery.stop()

tswt = TestSuiteWindowTime()
tswt.runTest()