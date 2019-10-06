import getTaxiFields as gtf

from datetime import datetime
from globalVar import getVal as glb
from dataProcessing import dataProcess

print("Performing schema check on taxi data...")

nOfYears = glb('nOfPassYears')
currYear = datetime.now().year
yearList = [str(cnt + currYear - nOfYears + 1) for cnt in range(nOfYears)]
months   = [str(val + 1).zfill(2) for val in range(12)]

ptr = 0; dataObj = []
for yr in yearList:
    for mn in months:
        dataObj.append(dataProcess(yr, mn))
        if not dataObj[ptr].hasData():
            del dataObj[ptr]
        else:
            ptr = ptr + 1

flag = 0
for dProp in dataObj:
    if dProp.year == '2016' and dProp.month in ['07','08','09','10','11','12']:
        badSchmType = '1'
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