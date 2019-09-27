# This module appends weather data to taxi data
# Returns 3 columns: air temperature, cloud coverage and precipitation
# Author: Colin Chow
# Date created: 09/25/2019
# Version:

import pyspark.sql.types as sqlt

hrInSec = 3600 # 1 hour = 3600 seconds

# Join based on closest time - not efficient
#def appendWeatherData(ts, tsVec, airTemp, cloudCov, precip1Hr):
#    deltaTime = [abs(val - ts) for val in tsVec]
#    ind       = deltaTime.index(min(deltaTime))
#    
#    return sqlt.Row('airTemp'    , 'cloudCov'    , 'pricip1Hr')\
#                   ( airTemp[ind],  cloudCov[ind],  precip1Hr[ind])

def appendWeatherData(ts, tsVec, airTemp, cloudCov, precip1Hr):
    deltaTime = ts - tsVec[0] + hrInSec
    ind       = int(deltaTime/hrInSec)
    if ind < 0:
        ind = 0
    elif ind >= len(tsVec):
        ind = len(tsVec) - 1
    
    return sqlt.Row('airTemp'    , 'cloudCov'    , 'pricip1Hr')\
                    (airTemp[ind], cloudCov[ind], precip1Hr[ind])

    # end of appendWeatherData

