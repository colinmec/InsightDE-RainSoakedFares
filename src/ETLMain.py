from datetime import datetime
from pyspark    import SparkConf, SparkContext
from pyspark.sql import SparkSession
from globalVar    import getVal as glb
from dataProcessing import dataProcess

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

    ptr = 0; dataObj = []
    for yr in yearList:
        for mn in months:
            dataObj.append(dataProcess(yr, mn))
            if not dataObj[ptr].hasData():
                del dataObj[ptr]
            else:
                ptr = ptr + 1

    for dProp in dataObj:
        dProp.readData(spark)
        dProp.addTimestamp()
        dProp.addWthrStationID()
        dProp.joinTables(spark)
        dProp.writeToPostgres('yellow')
        #dProp.printCheck()

    spark.stop()
    # end of Main()

if __name__ == "__main__":
    main()
