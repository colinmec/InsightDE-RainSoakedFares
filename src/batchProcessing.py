# Called by batchMain: all batch processes are defined here
# Object batchProcess is created for each taxi table in postgres database
# Author: Colin Chow
# Date created: 10/05/2019
# Version:

import postgres                     # Custom, see postgres.py
import datetimeTools as dtt         # Custom, see datetimeTools.py

from datetime  import datetime as dt
from globalVar import getVal as glb # Custom, see globalVar.py
from pyspark.sql import functions as SF
from pyspark.sql.types import *
from pyspark.sql.functions import udf, when, col

class batchProcess:

    def __init__(self, year, month):
        self.year  = year
        self.month = month
        self.tableName = 'yellow_' + year + '_' + month

    def hasTable(self, sqlc):
        # Check if the table exists
        # There must be a better way than this
        pg = postgres.PostgresConnector()
        qr = "(SELECT 1 FROM pg_tables WHERE tablename='" + self.tableName + "') AS wtf"
        df = pg.read(sqlc, table=qr)
        return df.count() == 1

    def readTable(self, sqlc):
        # To speed things up, define partition scheme here:
        mStart  = self.year + '-' + self.month + '-01 00:00:00'
        mEnd    = str(int(self.year)+(int(self.month))/12) + '-' + \
                  str((int(self.month)+1)%12).zfill(2).replace('00','12') + '-01 00:00:00'
        lb      = dtt.strToTimeStamp(mStart) - 3600*24 # from 1 day prior
        ub      = dtt.strToTimeStamp(mEnd)   + 3600*24 # upto 1 day after
        pg      = postgres.PostgresConnector()
        qr      = "(SELECT * FROM " + self.tableName + ") AS wtf"
        self.df = pg.read(sqlc, table=qr, numPartitions=32, column="pUTimeStamp", \
                          lowerBound=lb, upperBound=ub)

    def addDatetime(self):
        apndSchema = StructType([
                     StructField("year" , StringType(),  True),
                     StructField("month", StringType(),  True),
                     StructField("day"  , IntegerType(), True),
                     StructField("hour" , IntegerType(), True),
                     StructField("wkday", StringType(),  True)])
        self.df = self.df.where(col("pUTimeStamp").isNotNull())
        self.df = self.df.where(col("dOTimeStamp").isNotNull())
        udf_strToTimeStamp = udf(appendTimeStr, apndSchema)
        self.df = self.df.withColumn("output", udf_strToTimeStamp("pUTimeStamp"))

        # Clean up the schema by flattening "output"
        colList = [cols for cols in self.df.columns if cols not in {"output"}]
        colList.append('output.*')
        self.df = self.df.select(colList)
        self.df = self.df.drop("output")

        # Remove bad data
        self.df = self.df.filter(self.df['year'] == self.year)
        self.df = self.df.filter(self.df['month'] == self.month)

    def addMetrics(self):
        self.df = self.df.withColumn("duration", col("dOTimeStamp")-col("pUTimeStamp"))
        self.df = self.df.withColumn("speed", when(col("duration") > 0, \
                      3600*col("distance")/col("duration")).otherwise(None))
        self.df = self.df.withColumn("speed", SF.round(self.df["speed"], 2))
        self.df = self.df.withColumn("FPM", when(col("distance") > 0, \
                      (col("totalPaid")-col("tip"))/col("distance")).otherwise(None))
        self.df = self.df.withColumn("FPM", SF.round(self.df["FPM"], 2))
        self.df = self.df.withColumn("FPHr", col("FPM")*col("speed"))
        self.df = self.df.withColumn("FPHr", SF.round(self.df["FPHr"], 2))

    def fixPrecip(self):
        # NOAA use -0.1 to represent trace precipitation (< instrumental precision)
        # Convert -0.1 to 0.05 (arbitrarily defined as 0.5*precision)
        self.df = self.df.withColumn("precip", when(col('pUPrecip1Hr') < 0, \
                      -col('pUPrecip1Hr')/2).otherwise(col('pUPrecip1Hr')))
        self.df = self.df.drop("pUPrecip1Hr")
        self.df = self.df.fillna({'precip':0, 'pUCloudCov':0}) # Fill NULL with 0

    def setStation(self, station):
        self.station = station
        self.dfst    = self.df.filter(self.df['station'] == self.station)

    def aggByHour(self):
        self.hr = self.dfst.groupby(['day', 'wkday', 'hour'])\
                           .agg({'pUTimeStamp':'count', \
                                 'fare'       :'sum'  , \
                                 'tip'        :'sum'  , \
                                 'totalPaid'  :'sum'  , \
                                 'speed'      :'mean' , \
                                 'FPM'        :'mean' , \
                                 'FPHr'       :'mean' , \
                                 'nPax'       :'sum'  , \
                                 'precip'     :'mean' , \
                                 'pUAirTemp'  :'mean' , \
                                 'pUCloudCov' :'mean'})

        self.hr = self.hr.withColumnRenamed("count(pUTimeStamp)", "n_trips")   \
                         .withColumnRenamed("sum(fare)"         , "sum_fare")  \
                         .withColumnRenamed("sum(tip)"          , "sum_tip")   \
                         .withColumnRenamed("sum(totalPaid)"    , "sum_paid")  \
                         .withColumnRenamed("avg(speed)"        , "avg_speed") \
                         .withColumnRenamed("avg(FPM)"          , "avg_FPM")   \
                         .withColumnRenamed("avg(FPHr)"         , "avg_FPHr")  \
                         .withColumnRenamed("sum(nPax)"         , "sum_nPax")  \
                         .withColumnRenamed("avg(precip)"       , "precip")    \
                         .withColumnRenamed("avg(pUAirTemp)"    , "air_temp")  \
                         .withColumnRenamed("avg(pUCloudCov)"   , "cloud_cvr")

        self.hr = self.hr.withColumn("sum_fare" , SF.round(self.hr["sum_fare"] , 2))
        self.hr = self.hr.withColumn("sum_tip"  , SF.round(self.hr["sum_tip"]  , 2))
        self.hr = self.hr.withColumn("sum_paid" , SF.round(self.hr["sum_paid"] , 2))
        self.hr = self.hr.withColumn("avg_speed", SF.round(self.hr["avg_speed"], 2))
        self.hr = self.hr.withColumn("avg_FPM"  , SF.round(self.hr["avg_FPM"]  , 2))
        self.hr = self.hr.withColumn("avg_FPHr" , SF.round(self.hr["avg_FPHr"] , 2))
        self.hr = self.hr.withColumn("precip"   , SF.round(self.hr["precip"]   , 2))
        self.hr = self.hr.withColumn("air_temp" , SF.round(self.hr["air_temp"] , 2))
        self.hr = self.hr.withColumn("cloud_cvr", SF.round(self.hr["cloud_cvr"], 2))

    def writeResults(self, prefix):
        pgTableName  = prefix + self.year + '_' + self.month + '_' + 'st' + str(self.station)
        conn = postgres.PostgresConnector()
        conn.write(self.hr, pgTableName, glb('pgWriteMode'), db='taxi_aggregates')

    # End of class batchProcess

def dayLabel(intKey):
    # Gives week day label
    return {0:'M',1:'Tu',2:'W',3:'Th',4:'F',5:'Sa',6:'Su'}.get(intKey)

def appendTimeStr(ts):
    return [str(dt.fromtimestamp(ts).year),           \
            str(dt.fromtimestamp(ts).month).zfill(2), \
            int(dt.fromtimestamp(ts).day),            \
            int(dt.fromtimestamp(ts).hour),           \
            dayLabel(dt.fromtimestamp(ts).weekday())]
