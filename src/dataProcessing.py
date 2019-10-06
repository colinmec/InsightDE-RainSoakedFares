# Called by main: all ETL processes are defined here
# Object dataProcess is created for each taxi data file
# Author: Colin Chow
# Date created: 09/30/2019
# Version:

import postgres                  # Custom, see postgres.py
import S3Tools as S3             # Custom, see S3Tools.py
import datetimeTools as dtt      # Custom, see datetimeTools.py
import getTaxiFields as gtf      # Custom, see getTaxiFields.py
import appendWeatherData as apdW # Custom, see appendWeatherData.py

from globalVar import getVal as glb # Custom, see globalVar.py
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as SF
from pyspark.sql.types import *
from pyspark.sql.functions import udf

class dataProcess:

    def __init__(self, year, month):
        self.year          = year
        self.month         = month
        self.weatherBucket = glb('s3WeatherBucket')
        self.taxiBucket    = glb('s3TaxiBucket')
        self.ylwTaxiPrefix = glb('ylwTaxiPrefix')
        self.weatherS3Key  = []
        self.weatherFile   = []
        self.wthrRep       = []
        self.getYlwTaxiFilename()
        self.getWeatherFilename(['Central-Park','La-Guardia','JFK-Intl','Newark-Intl'])
        
    def getWeatherFilename(self, stationNames):
        # Create a list of weather filepaths for every station
        s3Prefix = glb('s3Prefix')
        for stationName in stationNames:
            keyName  = stationName + '/' + getStationID(stationName) + '-' + \
                         self.year + '-' + self.month + '.csv'
            filepath = s3Prefix + self.weatherBucket + "/" + keyName
            self.weatherS3Key.append(keyName)
            self.weatherFile.append(filepath)
    
    def getYlwTaxiFilename(self):
        # Define taxi data filepath
        s3Prefix          = glb('s3Prefix')
        self.ylwTaxiS3Key = self.ylwTaxiPrefix + self.year + '-' + self.month + '.csv'
        self.ylwTaxiFile  = s3Prefix + self.taxiBucket + "/" + self.ylwTaxiS3Key

    def hasData(self):
        # Make sure all data files (weather report and taxi) exist
        allStations = [S3.key_exists(self.weatherBucket, key) for key in self.weatherS3Key]
        return S3.key_exists(self.taxiBucket, self.ylwTaxiS3Key) and \
               not False in allStations

    def readData(self, spark):
        # Read both weather reports (all stations) and taxi data
        s3ReadMode = glb('s3ReadMode')
        if self.year == '2016' and self.month in ['07','08','09','10','11','12']:
            badSchmType = '1' # Known bad schema
        else:
            badSchmType = '0'
        fields, dTypes     = \
            gtf.getColNamesAndTypes(self.taxiBucket, self.ylwTaxiS3Key, badSchmType)
        self.ylwTaxiSchema = makeSchema(fields, dTypes)
        self.ylwTaxi = spark.read.csv(self.ylwTaxiFile, header=True, \
                       mode=s3ReadMode, schema=self.ylwTaxiSchema)
        self.weatherSchema = makeSchema(glb('weatherFields'), glb('weatherDataType'))
        for wFile in self.weatherFile:
            self.wthrRep.append(spark.read.csv(wFile, header=True, \
                                mode=s3ReadMode, schema=self.weatherSchema))
        
    def addTimestamp(self):
        # Create and append timestamps for taxi data
        udf_strToTimeStamp = udf(dtt.strToTimeStamp, LongType())
        self.ylwTaxi = self.ylwTaxi.withColumn("pUTimeStamp", udf_strToTimeStamp("pUDateTime"))
        self.ylwTaxi = self.ylwTaxi.withColumn("dOTimeStamp", udf_strToTimeStamp("dODateTime"))
        
    def addWthrStationID(self):
        # Find which weather station by location ID (since 2017) or coordinates (prior to 2017)
        if 'pULocId' in self.ylwTaxi.columns:
            udf_getStationWithID = udf(apdW.getStationWithID, IntegerType())
            self.ylwTaxi = self.ylwTaxi.withColumn("station", udf_getStationWithID("pULocId"))
        elif 'pULong' in self.ylwTaxi.columns:
            udf_getStation = udf(apdW.getStationWithCoord, IntegerType())
            self.ylwTaxi = self.ylwTaxi.withColumn("station", udf_getStation("pULong", "pULat"))

    def joinTables(self, spark):
        # Main heavy-lifting here
        # First, broadcast weather report to all Spark workers
        tstmp = []; artmp = []; cldcv = []; prcp1 = []
        for station in self.wthrRep:
            tstmp.append([row.timeStamp  for row in station.collect()])
            artmp.append([row.airTemp    for row in station.collect()])
            cldcv.append([row.cloudCover for row in station.collect()])
            prcp1.append([row.precip1Hr  for row in station.collect()])
        ts_bc = spark.sparkContext.broadcast(tstmp)
        at_bc = spark.sparkContext.broadcast(artmp)
        cc_bc = spark.sparkContext.broadcast(cldcv)
        p1_bc = spark.sparkContext.broadcast(prcp1)
        
        # I chose to include 3 readings:
        apndSchema = StructType([
                     StructField("pUAirTemp",   FloatType(),  True),
                     StructField("pUCloudCov",  StringType(), True),
                     StructField("pUPrecip1Hr", FloatType(),  True)])

        # Use user-defined function
        udf_appendWeatherData = udf(lambda x, y: apdW.appendWeatherData(x, y, \
                                ts_bc.value, at_bc.value, cc_bc.value, p1_bc.value), apndSchema)
        self.ylwTaxi = self.ylwTaxi.withColumn("output", udf_appendWeatherData("pUTimeStamp","station"))
        
        # Clean up the schema by flattening weather data
        colList = [cols for cols in self.ylwTaxi.columns if cols not in {"output"}]
        colList.append('output.*')
        self.ylwTaxi = self.ylwTaxi.select(colList)
        self.ylwTaxi = self.ylwTaxi.drop("output")
        
    def writeToPostgres(self, prefix):
        # There are two different kind of schemas:
        # Prior to 2017, use coordinates; after, location ID
        if 'pULocId' in self.ylwTaxi.columns:
            keepCols = glb('pgKeepCols1')
        else:
            keepCols = glb('pgKeepCols2')
        dropCols = [clm for clm in self.ylwTaxi.columns if clm not in keepCols]
        for clm in dropCols:
            self.ylwTaxi = self.ylwTaxi.drop(clm)
        self.ylwTaxi = self.ylwTaxi.select(keepCols)
        self.pgTableName  = prefix + '_' + self.year + '_' + self.month
        connector = postgres.PostgresConnector()
        connector.write(self.ylwTaxi, self.pgTableName, glb('pgWriteMode'))
        
    def printCheck(self):
        # For debuging purpose
        print(self.ylwTaxi.show(25))
        print(self.wthrRep[0].show(25))
    
    # End of class dataProcess

def makeSchema(dFields, dTypes):
    return ', '.join([var1 + ' ' + var2 for var1, var2 in zip(dFields, dTypes)])

def getStationID(stationName):
    noaaStations = glb('noaaStations')
    return noaaStations.keys()[noaaStations.values().index(stationName)]

