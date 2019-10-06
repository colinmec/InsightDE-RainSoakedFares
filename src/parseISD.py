# This module parses a row of characters in ISD fixed-width format to key-value dictionary.
# Author: Colin Chow
# Date created: 09/20/2019
# Version:

import datetimeTools as dtt         # Custom, see datetimeTools.py

from datetime import datetime
from globalVar import getVal as glb # Custom, see globalVar.py

"""
'readings' is a key-value dictionary that stores the following variables:
    
'timeStamp' : time-stamp
'airTemp'   : air temp in degrees C scaled up by 10 or -9999 (missing value)
'dewPt'     : dew point in degrees C scaled up by 10 or -9999 (missing value)
'pressure'  : sea level pressure in hectopascals scaled up by 10 or -9999 (missing value)
'windDir'   : wind direction in angular degrees
'windSpd'   : wind speed rate in meters per second scaled up by 10 or -9999 (missing value)
'cloudCover': cloud cover, see cloudCoverKeyToText for more info (-9999 is missing value)
'precip1hr' : liquid precipitation depth measured over 1 hr accumulation period, in mm,
              scaled up by 10 (-1 means trace, -9999 means missing value)
'precip6Hr' : liquid precipitation depth measured over 6 hr accumulation period, in mm,
              scaled up by 10 (-1 means trace, -9999 means missing value)
"""

def parseIsdLine(line):

    weatherFields = glb('weatherFields')
    
    # Initialize all to none
    readings={key: None for key in weatherFields}  
    
    # First 4 fields are year, month, day and hour.
    # Convert them into a time-stamp:
    date_hr = []
    fields = line.split()
    for cnt in range(4):
        date_hr.append(fields.pop(0))
            
    date_hr_str = date_hr[0]+'-'+date_hr[1]+'-'+date_hr[2]+' '+date_hr[3]+':00:00'
    timeType    = dtt.getTimeType(date_hr_str)
    timeStmp    = dtt.getTimeStamp(timeType)
    fields.insert(0,timeStmp)
       
    # Load values into readings and remove scaling:
    for cnt, val in enumerate(fields):
        if not isNone(val):
            key_val = getActualReadings(cnt, val)
            readings[weatherFields[key_val[0]]] = key_val[1]
    
    return readings  
    # End of parseIsdLine
        
def isNone(value):
    if value in ("-9999", "-99999", "-999999", ""):
        return True
    else:
        return False
        
def getActualReadings(index, val):
    return {
        0:[0, val],
        1:[1, float(val)/10],
        2:[2, float(val)/10],
        3:[3, float(val)/10],
        4:[4, float(val)],
        5:[5, float(val)/10],
        6:[6, val],
        7:[7, float(val)/10],
        8:[8, float(val)/10],
    }.get(index)
        
def cloudCoverKeyToText(keyName):
    return {
        "0": "None, SKC or CLR",
        "1": "One okta - 1/10 or less but not zero",
        "2": "Two oktas - 2/10 - 3/10, or FEW",
        "3": "Three oktas - 4/10",
        "4": "Four oktas - 5/10, or SCT",
        "5": "Five oktas - 6/10",
        "6": "Six oktas - 7/10 - 8/10",
        "7": "Seven oktas - 9/10 or more but not 10/10, or BKN",
        "8": "Eight oktas - 10/10, or OVC",
        "9": "Sky obscured, or cloud amount cannot be estimated",
        "10": "Partial obscuration",
        "11": "Thin scattered",
        "12": "Scattered",
        "13": "Dark scattered",
        "14": "Thin broken",
        "15": "Broken",
        "16": "Dark broken",
        "17": "Thin overcast",
        "18": "Overcast",
        "19": "Dark overcast",
    }.get(keyName)