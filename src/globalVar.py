# Here I store all the variables I might need in multiple scripts
# Author: Colin Chow
# Date created: 09/20/2019
# Version:

# PostgreSQL variables
dbName   = 'postgres'    # Name of postgres database (default: postgres)
pgHost   = 'ip-10-0-0-5' # Host of postgres database
pgUser   = 'colinmec'    # My postgres username
pgPasswd = 'password'    # My postgres password

# NYC-TLC taxi data variables
#s3TaxiBucket    = 'nyc-tlc'
#s3TaxiBucket    = 'colinmec-test'
s3TaxiBucket    = 'colinmec-test-nyc-tlc'
ylwTaxiPrefix   = 'trip data/yellow_tripdata_'
ylwTaxiFields   = ['VendorID'      , 'pUDateTime', 'dODateTime'   , \
                   'nPax'          , 'distance'  , 'rateCode'     , \
                   'storeNFwdFlag' , 'pULocId'   , 'dOLocId'      , \
                   'paymentType'   , 'fare'      , 'extra'        , \
                   'mtaTax'        , 'tip'       , 'tollsAmount'  , \
                   'imprvSurcharge', 'totalPaid' , 'congSurcharge']
ylwTaxiDataType = ['STRING', 'STRING', 'STRING', \
                   'INT'   , 'FLOAT' , 'STRING', \
                   'STRING', 'STRING', 'STRING', \
                   'STRING', 'FLOAT' , 'FLOAT' , \
                   'FLOAT' , 'FLOAT' , 'FLOAT' , \
                   'FLOAT' , 'FLOAT' , 'FLOAT']

# NOAA IDS data variables
nOfPassYears    = 11
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
        'dbName'          : dbName,
        'pgHost'          : pgHost,
        'pgUser'          : pgUser,
        'pgPasswd'        : pgPasswd,
        's3TaxiBucket'    : s3TaxiBucket,
        'ylwTaxiPrefix'   : ylwTaxiPrefix,
        'ylwTaxiFields'   : ylwTaxiFields,
        'ylwTaxiDataType' : ylwTaxiDataType,
        'nOfPassYears'    : nOfPassYears,
        's3WeatherBucket' : s3WeatherBucket,
        'noaaFtpDomain'   : noaaFtpDomain,
        'noaaFtpPath'     : noaaFtpPath,
        'noaaLogin'       : noaaLogin,
        'noaaPassword'    : noaaPassword,
        'noaaStations'    : noaaStations,
        'weatherFields'   : weatherFields,
        'weatherDataType' : weatherDataType
    }.get(keyName)
