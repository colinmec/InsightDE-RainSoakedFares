from datetime import datetime
from globalVar import getVal as glb


    # Years of interest: n-years back from current year
    currYear = datetime.now().year
    yearList = [cnt + currYear - nOfYears + 1 for cnt in range(nOfYears)]
    months   = [val + 1 for val in range(12)]
    
    noaaStations = glb('noaaStations')
    stationName  = 'Central-Park'
    stationId    = noaaStations.keys()[noaaStations.values().index(stationName)]
    