"""
author: Srilakshmi
Purpose:To identify final state of payload by Module for Employee,Company,Position,Job
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("Paylocity DE Challenege") \
    .getOrCreate()

# Read JSON file into dataframe


def readdata():
    df = spark.read.json("C:/poc/paylocity-de-challenge/src/resources/sample_payload.txt")
    return df


# def processData(input_data)->map[String,DataFrame]:
def processdata(input_data):
    company_schema=("guid","name","status")
    job_schema=("guid","company_guid","employee_guid")
    position_schema=("guid","name","status")
    employee_schema=("guid","status","state")

    window_desc = Window.partitionBy(col("guid")).orderBy(col("timestamp").desc(),col("status").desc())
    result_df=input_data.withColumn("timestamp",input_data.timestamp.cast(DoubleType())).withColumn("rank_desc", row_number().over(window_desc)).filter(col("rank_desc")==1).filter(~(input_data.action == "DELETE"))\
              .drop("action","timestamp","rank_desc")

    company_df=result_df.filter(col("source_table")=="Company").select(*company_schema)
    job_df=result_df.filter(col("source_table")=="Job").select(*job_schema)
    position_df=result_df.filter(col("source_table")=="Position").select(*position_schema)
    employee_df=result_df.filter(col("source_table")=="Employee").select(*employee_schema)

    outputDFsMap ={"Company":company_df,
                   "Job":job_df,
                   "Position":position_df,
                   "Employee":employee_df
                   }
    return outputDFsMap


def printjsonoutput(dataFrameList):
    for key,df in dataFrameList.items():
        print(key)
        print("===============")
        json_formatted_str=df.toPandas().to_json(orient="records",lines="True").replace('"', "'")
        print(json_formatted_str)

input_data=readdata()
dataFrameList=processdata(input_data)
printjsonoutput(dataFrameList)