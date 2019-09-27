import sys
import postgres
import globalVar
import datetimeTools
import appendWeatherData as apdW

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf


class shortTimeJoin:

    def __init__(self):
        self.spark = SparkSession.builder \
                                 .appName("simpleTimeJoin") \
                                 .getOrCreate()

        self.weatherBucket = globalVar.getVal('s3WeatherBucket')
        self.weatherSchema = ', '.join([var1 + ' ' + var2 for var1, var2 in \
                                        zip(globalVar.getVal('weatherFields'), \
                                            globalVar.getVal('weatherDataType'))])

        self.taxiBucket    = globalVar.getVal('s3TaxiBucket')
        self.ylwTaxiPrefix = globalVar.getVal('ylwTaxiPrefix')
        self.ylwTaxiSchema = ', '.join([var1 + ' ' + var2 for var1, var2 in \
                                        zip(globalVar.getVal('ylwTaxiFields'), \
                                            globalVar.getVal('ylwTaxiDataType'))])

    def s3LoadWeatherData(self, keyName):
        pathname = "s3a://" + self.weatherBucket + "/" + keyName
        return self.spark.read.csv(pathname, header = True, mode = "PERMISSIVE", schema = self.weatherSchema)

    def s3LoadYlwTaxiData(self, keyName):
        pathname = "s3a://" + self.taxiBucket + "/" + keyName
        return self.spark.read.csv(pathname, header = True, mode = "PERMISSIVE", schema = self.ylwTaxiSchema)

    def writeToPostgres(self, dataFrame, tableName):
        mode = "append"
        connector = postgres.PostgresConnector()
        connector.write(dataFrame, tableName, mode)

noaaStations = globalVar.getVal('noaaStations')
stationName = 'Central-Park'
stationId = noaaStations.keys()[noaaStations.values().index(stationName)]

year = '2018'
month = '01'
weatherKey = stationName + '/' + stationId + '-' + year + '.csv'

Year2018 = shortTimeJoin()
Year2018.spark.sparkContext.addPyFile("datetimeTools.py")
Year2018.spark.sparkContext.addPyFile("appendWeatherData.py")
                          
ylwTaxiKey = Year2018.ylwTaxiPrefix + year + '-' + month + '.csv'

weatherRep  = Year2018.s3LoadWeatherData(weatherKey)
ylwTaxiData = Year2018.s3LoadYlwTaxiData(ylwTaxiKey)

udf_strToTimeStamp = udf(datetimeTools.strToTimeStamp, LongType())
ylwTaxiData = ylwTaxiData.withColumn("pUTimeStamp", udf_strToTimeStamp("pUDateTime"))
ylwTaxiData = ylwTaxiData.withColumn("dOTimeStamp", udf_strToTimeStamp("dODateTime"))

ts_bc = Year2018.spark.sparkContext.broadcast([row.timeStamp  for row in weatherRep.collect()])
at_bc = Year2018.spark.sparkContext.broadcast([row.airTemp    for row in weatherRep.collect()])
cc_bc = Year2018.spark.sparkContext.broadcast([row.cloudCover for row in weatherRep.collect()])
p1_bc = Year2018.spark.sparkContext.broadcast([row.precip1Hr  for row in weatherRep.collect()])

udfSchema = StructType([
            StructField("pUAirTemp",   FloatType(),  True),
            StructField("pUCloudCov",  StringType(), True),
            StructField("pUPrecip1Hr", FloatType(),  True)])

udf_appendWeatherData = \
    udf(lambda x: apdW.appendWeatherData(x, ts_bc.value, at_bc.value, cc_bc.value, p1_bc.value), udfSchema)

ylwTaxiData = ylwTaxiData.withColumn("output", udf_appendWeatherData("pUTimeStamp"))

colList = [cols for cols in ylwTaxiData.columns if cols not in {"output"}]
colList.append('output.*')

ylwTaxiData = ylwTaxiData.select(colList)
ylwTaxiData = ylwTaxiData.drop("output")

keepCol = ['VendorID', 'pUTimeStamp', 'pULocId', 'dOTimeStamp', 'dOLocId', 'distance', \
           'nPax', 'fare', 'tip', 'totalPaid', 'pUAirTemp', 'pUCloudCov', 'pUPrecip1Hr']
dropCol = [cols for cols in ylwTaxiData.columns if cols not in keepCol]

for clm in dropCol:
    ylwTaxiData = ylwTaxiData.drop(clm)

ylwTaxiData = ylwTaxiData.select(keepCol)

#print(weatherRep.show(50))
#print(ylwTaxiData.show(50))
#print("Check here:")
#print(ylwTaxiData.count())
#print("Check here:")

Year2018.writeToPostgres(ylwTaxiData, "testTaxi3")

Year2018.spark.stop()



