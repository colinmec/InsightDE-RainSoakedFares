from datetime import datetime
from pyspark    import SparkConf, SparkContext
from pyspark.sql import SparkSession
from globalVar    import getVal as glb # Custom, see globalVar.py
from dataProcessing import dataProcess # Custom, see dataProcessing.py

def main():
    # One spark session to join them all
    spark = SparkSession.builder             \
                        .appName("timeJoin") \
                        .getOrCreate()

    spark.sparkContext.addPyFile("datetimeTools.py")
    spark.sparkContext.addPyFile("appendWeatherData.py")
    spark.sparkContext.addPyFile("dataProcessing.py")

    # Years ond months of interest: n-years back from current year
    nOfYears = glb('nOfPassYears')
    currYear = datetime.now().year
    yearList = [str(cnt + currYear - nOfYears + 1) for cnt in range(nOfYears)]
    months   = [str(val + 1).zfill(2) for val in range(12)]
    #yearList = ['2012','2013','2014']
    #months   = ['01']

    # Create an object for every taxi data file
    # Make sure to remove object if file does not exist
    ptr = 0; dataObj = []
    for yr in yearList:
        for mn in months:
            dataObj.append(dataProcess(yr, mn))
            if not dataObj[ptr].hasData():
                del dataObj[ptr]
            else:
                ptr = ptr + 1

    # Start calling methods in dataProcessing.py
    for dProp in dataObj:
        dProp.readData(spark)           # Read data
        dProp.addTimestamp()            # Convert string to timestamp
        dProp.addWthrStationID()        # Add weather station ID
        dProp.joinTables(spark)         # Main join process
        dProp.writeToPostgres('yellow') # Write to DB with prefix 'yellow'
        #dProp.printCheck()

    spark.stop()
    # end of Main()

if __name__ == "__main__":
    main()
