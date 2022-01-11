import unittest

from etl.latestPayload import processdata
from pyspark.sql.types import *
from pyspark.sql import SparkSession


class testLatestPayloadState(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
                     .builder
                     .master("local[*]")
                     .appName("PySpark-unit-test")
                     .config('spark.port.maxRetries', 30)
                     .getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


    # Test Case for processData Function
    def test_etl(self):
        input_schema = StructType([
                StructField('action', StringType(), True),
                StructField('company_guid', StringType(), True),
                StructField('employee_guid', StringType(), True),
                StructField('guid', StringType(), True),
                StructField('name', StringType(), True),
                StructField('source_table', StringType(), True),
                StructField('state', StringType(), True),
                StructField('status', StringType(), True),
                StructField('timestamp', StringType(), True)
            ])

        input_data = [("INSERT","","","1c898066-858e-406c-a15d-36146c9642de","Paylocity","Company","","1","100.0"),
                      ("UPDATE","","","1c898066-858e-406c-a15d-36146c9642de","Paylocity","Company","","2","300.0")]


        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)
                
        expected_schema = StructType([
                StructField('guid', StringType(), True),
                StructField('name', StringType(), True),
                StructField('status', StringType(), True)
                ])

        expected_data = [("1c898066-858e-406c-a15d-36146c9642de","Paylocity","2")]
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        #Apply transforamtion on the input data frame
        input_df.show()
        expected_df.show()
        result = processdata(input_df)
        company_df=result['Company']
        # Compare schema of transformed_df and expected_df
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, company_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)

        # assert
        self.assertTrue(res)
        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(company_df.collect()))

if __name__ == '__main__':
    unittest.main()