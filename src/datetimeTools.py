# This module specifies conversion format between string, time obj and time stamp
# Needed for Python 2. In Python 3, the datetime library has them
# Author: Colin Chow
# Date created: 09/20/2019
# Version:

from datetime import datetime

hrInSec = 3600                 # 1 hour = 3600 seconds
dayInHr = 24                   # 1 day = 24 hours
refDate = datetime(1970, 1, 1) # reference date according to Python 2 datetime

# String to time obj.:
def getTimeType(timeStr):
    return datetime.strptime(timeStr,'%Y-%m-%d %H:%M:%S')

# Time obj. to time stamp (long type)
def getTimeStamp(time):
    timeDelta = time - refDate
    return timeDelta.days*hrInSec*dayInHr + timeDelta.seconds

# String to time stamp (just a union of the two methods above)
def strToTimeStamp(timeStr):
    if timeStr is not None:
        timeDelta = getTimeType(timeStr) - refDate
        return timeDelta.days*hrInSec*dayInHr + timeDelta.seconds
    else:
        return None

# Same as above, but receives and returns an array
def strArrayToTimeStamps(strArray):
    timeStamps = []
    for timeStr in strArray:
        timeDelta = getTimeType(timeStr) - refDate
        timeStamps.append(timeDelta.days*hrInSec*dayInHr + timeDelta.seconds)
    return timeStamps

    