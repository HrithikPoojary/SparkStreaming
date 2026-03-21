class SlidingWindowTestSuite:
    def __init__(self):
        from A_Prepare_and_Process import spark
        self.spark = spark 
    
    def cleanUP(self):
        print("Cleaning is started.......")
        import subprocess
        subprocess.run(['hadoop' , 'fs' , '-rm' , '-r' , '/tmp/checkpioint-sliding-sensor/*'])
        subprocess.run(['hadoop' , 'fs' , '-rm' , '-r' , '/user/hive/warehouse/*'])

        # manual clean metadata_db , derby.log
        self.spark.sql("create table if not exists kafka_sensor(key STRING , value STRING)")

        print("cleaning is completed......")

    def waitForMicroBatch(self , sleep = 60):
        print("Waiting for 60 Minutes...........")
        import time 
        time.sleep(sleep)
        print("Waiting is over...........")

    def assertSensorSummery(self):
        actual_result = (self.spark.read.format("csv")
                                    .option("header" , "true")
                                    .load("file:///home/hrithik_poojary/data/csv/*")
                                    .collect()
        )

        expected_result = (self.spark.sql("select * from sensor_summary").collect())

        assert actual_result==expected_result , f"Failed ! actual - {actual_result} but expected - {expected_result}"4

    def runTest(self):
        from A_Prepare_and_Process import SlidingWindow
         self.cleanUP()

         sw = SlidingWindow()
         sQuery = sw.process()

        print("\nTesting all events...") 
        self.spark.sql("""INSERT INTO kafka_bz VALUES
                  ('SET41', '{"CreatedTime": "2019-02-05 09:54:00","Reading": 36.2}'),
                  ('SET41', '{"CreatedTime": "2019-02-05 09:59:00","Reading": 36.5}'),
                  ('SET41', '{"CreatedTime": "2019-02-05 10:04:00","Reading": 36.8}'),
                  ('SET41', '{"CreatedTime": "2019-02-05 10:09:00","Reading": 36.2}'),
                  ('SET41', '{"CreatedTime": "2019-02-05 10:14:00","Reading": 36.5}'),
                  ('SET41', '{"CreatedTime": "2019-02-05 10:19:00","Reading": 36.3}'),
                  ('SET41', '{"CreatedTime": "2019-02-05 10:24:00","Reading": 37.7}'),
                  ('SET41', '{"CreatedTime": "2019-02-05 10:29:00","Reading": 37.2}')
            """)
        self.waitForMicroBatch()
        self.assertSensorSummery()
        print("Validation is Complete We are good to go.....")

        sQuery.stop()

if __name__ = '__main__':
    swts = SlidingWindowTestSuite()
    swts.runTest()


    