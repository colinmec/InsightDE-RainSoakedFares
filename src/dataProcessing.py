import postgres
import S3Tools as S3
import datetimeTools as dtt
import getTaxiFields as gtf
import appendWeatherData as apdW

from globalVar import getVal as glb
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
        s3Prefix = glb('s3Prefix')
        for stationName in stationNames:
            keyName  = stationName + '/' + getStationID(stationName) + '-' + \
                         self.year + '-' + self.month + '.csv'
            filepath = s3Prefix + self.weatherBucket + "/" + keyName
            self.weatherS3Key.append(keyName)
            self.weatherFile.append(filepath)
    
    def getYlwTaxiFilename(self):
        s3Prefix          = glb('s3Prefix')
        self.ylwTaxiS3Key = self.ylwTaxiPrefix + self.year + '-' + self.month + '.csv'
        self.ylwTaxiFile  = s3Prefix + self.taxiBucket + "/" + self.ylwTaxiS3Key

    def hasData(self):
        allStations = [S3.key_exists(self.weatherBucket, key) for key in self.weatherS3Key]
        return S3.key_exists(self.taxiBucket, self.ylwTaxiS3Key) and \
               not False in allStations

    def readData(self, spark):
        s3ReadMode = glb('s3ReadMode')
        if self.year == '2016' and self.month in ['07','08','09','10','11','12']:
            badSchmType = '1'
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
        udf_strToTimeStamp = udf(dtt.strToTimeStamp, LongType())
        self.ylwTaxi = self.ylwTaxi.withColumn("pUTimeStamp", udf_strToTimeStamp("pUDateTime"))
        self.ylwTaxi = self.ylwTaxi.withColumn("dOTimeStamp", udf_strToTimeStamp("dODateTime"))
        
    def addWthrStationID(self):
        if 'pULocId' in self.ylwTaxi.columns:
            udf_getStationWithID = udf(apdW.getStationWithID, IntegerType())
            self.ylwTaxi = self.ylwTaxi.withColumn("station", udf_getStationWithID("pULocId"))
        elif 'pULong' in self.ylwTaxi.columns:
            udf_getStation = udf(apdW.getStationWithCoord, IntegerType())
            self.ylwTaxi = self.ylwTaxi.withColumn("station", udf_getStation("pULong", "pULat"))

    def joinTables(self, spark):
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
        
        apndSchema = StructType([
                     StructField("pUAirTemp",   FloatType(),  True),
                     StructField("pUCloudCov",  StringType(), True),
                     StructField("pUPrecip1Hr", FloatType(),  True)])

        udf_appendWeatherData = udf(lambda x, y: apdW.appendWeatherData(x, y, \
                                ts_bc.value, at_bc.value, cc_bc.value, p1_bc.value), apndSchema)

        self.ylwTaxi = self.ylwTaxi.withColumn("output", udf_appendWeatherData("pUTimeStamp","station"))
        
        colList = [cols for cols in self.ylwTaxi.columns if cols not in {"output"}]
        colList.append('output.*')
        self.ylwTaxi = self.ylwTaxi.select(colList)
        self.ylwTaxi = self.ylwTaxi.drop("output")
        
    def writeToPostgres(self, prefix):
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
        print(self.ylwTaxi.show(25))
        print(self.wthrRep[0].show(25))
    
    # End of class dataProcess

def makeSchema(dFields, dTypes):
    return ', '.join([var1 + ' ' + var2 for var1, var2 in zip(dFields, dTypes)])

def getStationID(stationName):
    noaaStations = glb('noaaStations')
    return noaaStations.keys()[noaaStations.values().index(stationName)]

