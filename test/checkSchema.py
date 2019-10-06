# Taxi data is really messed-up with frequent schema changes
# This code is to make sure getTaxiFields.getColNamesAndTypes does not miss something
# I. e., if one of the fields is None, something is wrong
# Author: Colin Chow
# Date created: 09/30/2019
# Version:

import getTaxiFields as gtf            # Custom, see getTaxiFields.py

from datetime import datetime
from globalVar import getVal as glb    # Custom, see globalVar.py
from dataProcessing import dataProcess # Custom, see dataProcessing.py

def main():
    print("Performing schema check on taxi data...")

    nOfYears = glb('nOfPassYears')
    currYear = datetime.now().year
    yearList = [str(cnt + currYear - nOfYears + 1) for cnt in range(nOfYears)]
    months   = [str(val + 1).zfill(2) for val in range(12)]

    # Create an object for every taxi data file
    # Make sure to remove object if files does not exist
    ptr = 0; dataObj = []
    for yr in yearList:
        for mn in months:
            dataObj.append(dataProcess(yr, mn))
            if not dataObj[ptr].hasData():
                del dataObj[ptr]
            else:
                ptr = ptr + 1

    # Start checking
    flag = 0
    for dProp in dataObj:
        if dProp.year == '2016' and dProp.month in ['07','08','09','10','11','12']:
            badSchmType = '1' # Known bad schema
        else:
            badSchmType = '0'
        fields, dTypes = \
            gtf.getColNamesAndTypes(dProp.taxiBucket, dProp.ylwTaxiS3Key, badSchmType)
        indc1 = [ind for ind, val in enumerate(fields) if val == None]
        indc2 = [ind for ind, val in enumerate(dTypes) if val == None]
        if not indc1 == [] or not indc2 == []:
            print("Oops: Schema error found in %s" %(dProp.ylwTaxiS3Key))
            print("Fields:")
            print(fields)
            print("Data types")
            print(dTypes)
            flag = flag + 1

    if flag == 0:
        print("Schema check passed.")
    else: 
        print("Schema check failed in %d files." %flag)
        
    # End of main()
    
if __name__ == "__main__":
    main()
