# Here I store all the variables I might need in multiple scripts
# Author: Colin Chow
# Date created: 09/20/2019
# Version:

import os

# Data volume:
nOfPassYears = 11

# PostgreSQL variables
pgHost      = os.environ['PGHOST']     # Host of postgres database
pgUser      = os.environ['PGUSER']     # My postgres username
pgPasswd    = os.environ['PGPASSWORD'] # My postgres password
dbName      = 'taxi_and_weather_5pc'   # Name of postgres database (default: postgres)
pgWriteMode = 'append'
pgKeepCols1 = ['VendorID'   , 'pUTimeStamp', 'pULocId',    \
               'dOTimeStamp', 'distance'   , 'dOLocId',    \
               'nPax'       , 'fare'       , 'tip',        \
               'totalPaid'  , 'pUAirTemp'  , 'pUCloudCov', \
               'pUPrecip1Hr', 'station']
pgKeepCols2 = ['VendorID'   , 'pUTimeStamp', 'pULong',    \
               'pULat'      , 'dOTimeStamp', 'distance',  \
               'dOLong'     , 'dOLat'      , 'nPax',      \
               'fare'       , 'tip'        , 'totalPaid', \
               'pUAirTemp'  , 'pUCloudCov' , 'pUPrecip1Hr', 'station']

# AWS S3 variables
s3Prefix    = 's3a://'
s3ReadMode  = 'PERMISSIVE'

# NYC-TLC taxi data variables
#s3TaxiBucket    = 'colinmec-test'
#s3TaxiBucket    = 'nyc-tlc'
s3TaxiBucket    = 'colinmec-nyc-tlc-5pc'
ylwTaxiPrefix   = 'trip data/yellow_tripdata_'

# NOAA IDS data variables
s3WeatherBucket = 'noaa-csv-data'
noaaFtpDomain   = 'ftp.ncdc.noaa.gov'
noaaFtpPath     = 'pub/data/noaa/isd-lite/'
noaaLogin       = 'anonymous'
noaaPassword    = ''
noaaStations    = {'725053-94728' : 'Central-Park',
                   '725030-14732' : 'La-Guardia',
                   '744860-94789' : 'JFK-Intl',
                   '725020-14734' : 'Newark-Intl'}
weatherFields   = ['timeStamp' , 'airTemp'  , 'dewPt'    ,\
                   'pressure'  , 'windDir'  , 'windSpd'  ,\
                   'cloudCover', 'precip1Hr', 'precip6Hr']
weatherDataType = ['LONG' , 'FLOAT', 'FLOAT', \
                   'FLOAT', 'FLOAT', 'FLOAT', \
                   'FLOAT', 'FLOAT', 'FLOAT']

def getVal(keyName):
    return {
        'nOfPassYears'    : nOfPassYears,
        'dbName'          : dbName,
        'pgHost'          : pgHost,
        'pgUser'          : pgUser,
        'pgPasswd'        : pgPasswd,
        'pgWriteMode'     : pgWriteMode,
        'pgKeepCols1'     : pgKeepCols1,
        'pgKeepCols2'     : pgKeepCols2,
        's3Prefix'        : s3Prefix,
        's3ReadMode'      : s3ReadMode,
        's3TaxiBucket'    : s3TaxiBucket,
        'ylwTaxiPrefix'   : ylwTaxiPrefix,
        's3WeatherBucket' : s3WeatherBucket,
        'noaaFtpDomain'   : noaaFtpDomain,
        'noaaFtpPath'     : noaaFtpPath,
        'noaaLogin'       : noaaLogin,
        'noaaPassword'    : noaaPassword,
        'noaaStations'    : noaaStations,
        'weatherFields'   : weatherFields,
        'weatherDataType' : weatherDataType
    }.get(keyName)
