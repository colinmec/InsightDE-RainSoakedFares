# Main ETL for aggregating data
# 1. Handles Spark session setup
# 2. Create object batchProcess for each table
# 3. Process data in sequential order
# Author: Colin Chow
# Date created: 10/05/2019
# Version:

import os
import pyspark.sql.functions as spf

from datetime  import datetime as dt
from globalVar import getVal as glb              # Custom, see globalVar.py
from pyspark   import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from batchProcessing import batchProcess         # Custom, see batchProcessing.py

def main():
    conf  = SparkConf()
    conf.set('spark.executorEnv.PGHOST'    , os.environ['PGHOST'])
    conf.set('spark.executorEnv.PGUSER'    , os.environ['PGUSER'])
    conf.set('spark.executorEnv.PGPASSWORD', os.environ['PGPASSWORD'])
    spark = SparkSession.builder                 \
                        .appName("batchProcess") \
                        .config(conf=conf)       \
                        .getOrCreate()
    
    spark.sparkContext.addPyFile("postgres.py")
    spark.sparkContext.addPyFile("globalVar.py")
    spark.sparkContext.addPyFile("datetimeTools.py")
    spark.sparkContext.addPyFile("batchProcessing.py")

    sqlc = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)
    
    # Years ond months of interest: n-years back from current year
    nOfYears = glb('nOfPassYears')
    currYear = dt.now().year
    yearList = [str(cnt + currYear - nOfYears + 1) for cnt in range(nOfYears)]
    #yearList = ['2017','2018','2019']
    months   = [str(val + 1).zfill(2) for val in range(12)]
    
    # Create an object for every taxi table
    # Make sure to remove object if file does not exist
    ptr = 0; tableObj = []
    for yr in yearList:
        for mn in months:
            tableObj.append(batchProcess(yr, mn))
            if not tableObj[ptr].hasTable(sqlc):
                del tableObj[ptr]
            else:
                ptr = ptr + 1

    # Start calling methods in batchProcessing.py
    for table in tableObj:
        table.readTable(sqlc)                # Read table
        table.addDatetime()                  # Add year, month, day and hour to table
        table.addMetrics()                   # Add vehicle speed and fare per mile
        table.fixPrecip()                    # Fix precipitation values
        for ind, station in enumerate(glb('noaaStations')):
            table.setStation(ind)
            table.aggByHour()                    # Aggregate data by the hour
            table.writeResults('hourly_yellow_') # Write to DB with prefix 'yellow'

    spark.stop()
    
    # End of main()

if __name__ == "__main__":
    main()
