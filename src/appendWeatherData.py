# This module appends weather data to taxi data
# Returns 3 columns: air temperature, cloud coverage and precipitation
# Author: Colin Chow
# Date created: 09/25/2019
# Version:

import pyspark.sql.types as sqlt

from operator import truediv

hrInSec = 3600 # 1 hour = 3600 seconds

# Join based on closest time - not efficient
#def appendWeatherData(ts, stID, tsVec, airTemp, cloudCov, precip1Hr):
#    if not ts == None:
#        deltaTime = [abs(val - ts) for val in tsVec[stID]]
#        ind       = deltaTime.index(min(deltaTime))
    
#        return sqlt.Row('airTemp'    , 'cloudCov'    , 'pricip1Hr')\
#                       ( airTemp[stID][ind],  cloudCov[stID][ind],  precip1Hr[stID][ind])
#    else:
#        return sqlt.Row('airTemp', 'cloudCov', 'pricip1Hr')\
#                        (None    ,  None     ,  None)

def appendWeatherData(ts, stID, tsVec, airTemp, cloudCov, precip1Hr):
    if not ts == None:
        deltaTime = ts - tsVec[stID][0]
        ind       = int(round(truediv(deltaTime,hrInSec)))
        if ind < 0:
            ind = 0
        elif ind >= len(tsVec[stID]):
            ind = len(tsVec[stID]) - 1
        return sqlt.Row('airTemp'          , 'cloudCov'         , 'pricip1Hr')\
                        (airTemp[stID][ind], cloudCov[stID][ind], precip1Hr[stID][ind])

    else:
        return sqlt.Row('airTemp', 'cloudCov', 'pricip1Hr')\
                        (None    ,  None     ,  None)

    # end of appendWeatherData

def getStationWithID(locID):
    # See hash map below
    return locIDToStation.get(locID)

def getStationWithCoord(lng, lat):
    # Arbitrarily defined:
    # West of -74.042 goes to Newark
    # East of -74.042 and south of 40.697 goes to JFK
    # For the remaining sector, draw a line between (40.697,-73.990) & (40.854,-73.846)
    # If falls below, goes to La-Guardia, the rest Central-Park
    if lng < -74.042:
        return 3
    elif lat < 40.697:
        return 2
    elif lat < (1.0903*lng + 121.368):
        return 1
    else:
        return 0

locIDToStation = { \
      '1' : 3,   '2' : 2,   '3' : 0,   '4' : 0,   '5' : 3,   '6' : 3,   '7' : 1,   '8' : 1,   '9' : 1,  '10' : 2,
     '11' : 2,  '12' : 0,  '13' : 0,  '14' : 2,  '15' : 1,  '16' : 1,  '17' : 2,  '18' : 0,  '19' : 1,  '20' : 0,
     '21' : 2,  '22' : 2,  '23' : 3,  '24' : 0,  '25' : 2,  '26' : 2,  '27' : 2,  '28' : 1,  '29' : 2,  '30' : 2,
     '31' : 0,  '32' : 0,  '33' : 2,  '34' : 2,  '35' : 2,  '36' : 2,  '37' : 2,  '38' : 2,  '39' : 2,  '40' : 2,
     '41' : 0,  '42' : 0,  '43' : 0,  '44' : 3,  '45' : 0,  '46' : 0,  '47' : 0,  '48' : 0,  '49' : 2,  '50' : 0,
     '51' : 0,  '52' : 2,  '53' : 1,  '54' : 2,  '55' : 2,  '56' : 1,  '57' : 1,  '58' : 0,  '59' : 0,  '60' : 0,
     '61' : 2,  '62' : 2,  '63' : 2,  '64' : 1,  '65' : 2,  '66' : 2,  '67' : 2,  '68' : 0,  '69' : 0,  '70' : 1,
     '71' : 2,  '72' : 2,  '73' : 1,  '74' : 0,  '75' : 0,  '76' : 2,  '77' : 2,  '78' : 0,  '79' : 0,  '80' : 2,
     '81' : 0,  '82' : 1,  '83' : 1,  '84' : 3,  '85' : 2,  '86' : 2,  '87' : 0,  '88' : 0,  '89' : 2,  '90' : 0,
     '91' : 2,  '92' : 1,  '93' : 1,  '94' : 0,  '95' : 1,  '96' : 1,  '97' : 2,  '98' : 1,  '99' : 3, '100' : 0,
    '101' : 1, '102' : 1, '103' : 0, '104' : 0, '105' : 0, '106' : 2, '107' : 0, '108' : 2, '109' : 3, '110' : 3,
    '111' : 2, '112' : 2, '113' : 0, '114' : 0, '115' : 3, '116' : 0, '117' : 2, '118' : 3, '119' : 0, '120' : 0,
    '121' : 1, '122' : 2, '123' : 2, '124' : 2, '125' : 0, '126' : 0, '127' : 0, '128' : 0, '129' : 1, '130' : 2,
    '131' : 1, '132' : 2, '133' : 2, '134' : 1, '135' : 1, '136' : 0, '137' : 0, '138' : 1, '139' : 2, '140' : 0,
    '141' : 0, '142' : 0, '143' : 0, '144' : 0, '145' : 1, '146' : 1, '147' : 0, '148' : 0, '149' : 2, '150' : 2,
    '151' : 0, '152' : 0, '153' : 0, '154' : 2, '155' : 2, '156' : 3, '157' : 1, '158' : 0, '159' : 0, '160' : 1,
    '161' : 0, '162' : 0, '163' : 0, '164' : 0, '165' : 2, '166' : 0, '167' : 0, '168' : 0, '169' : 0, '170' : 0,
    '171' : 1, '172' : 3, '173' : 1, '174' : 0, '175' : 1, '176' : 3, '177' : 2, '178' : 2, '179' : 1, '180' : 2,
    '181' : 2, '182' : 0, '183' : 0, '184' : 0, '185' : 0, '186' : 0, '187' : 3, '188' : 2, '189' : 2, '190' : 2,
    '191' : 2, '192' : 1, '193' : 1, '194' : 0, '195' : 2, '196' : 1, '197' : 2, '198' : 1, '199' : 0, '200' : 0,
    '201' : 2, '202' : 0, '203' : 2, '204' : 3, '205' : 2, '206' : 3, '207' : 1, '208' : 0, '209' : 0, '210' : 2,
    '211' : 0, '212' : 0, '213' : 0, '214' : 3, '215' : 2, '216' : 2, '217' : 2, '218' : 2, '219' : 2, '220' : 0,
    '221' : 3, '222' : 2, '223' : 1, '224' : 0, '225' : 2, '226' : 1, '227' : 2, '228' : 2, '229' : 0, '230' : 0,
    '231' : 0, '232' : 0, '233' : 0, '234' : 0, '235' : 0, '236' : 0, '237' : 0, '238' : 0, '239' : 0, '240' : 0,
    '241' : 0, '242' : 0, '243' : 0, '244' : 0, '245' : 0, '246' : 3, '247' : 0, '248' : 0, '249' : 0, '250' : 0,
    '251' : 3, '252' : 1, '253' : 1, '254' : 0, '255' : 2, '256' : 2, '257' : 2, '258' : 2, '259' : 0, '260' : 1,
    '261' : 0, '262' : 0, '263' : 0, '264' : 0, '265' : 0}