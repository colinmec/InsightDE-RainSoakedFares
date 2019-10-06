# This module imports weather data from NOAA ftp server,
# It converts data from fixed with format to .csv, and copy to AWS S3
# Author: Colin Chow
# Date created: 09/20/2019
# Version:

import os
import csv
import gzip
import ftplib
import S3Tools              # custom, see S3Tools.py
import parseISD             # custom, see parseISD.py
import datetimeTools as dtt # custom, see datetimeTools.py

from shutil    import copyfile
from datetime  import datetime
from globalVar import getVal as glb # see globalVar.py

# These variables are sourced from globalVar.py
weatherFields = glb('weatherFields')
noaaFtpDomain = glb('noaaFtpDomain')
noaaFtpPath   = glb('noaaFtpPath')
noaaStations  = glb('noaaStations')
noaaLogin     = glb('noaaLogin')
noaaPassword  = glb('noaaPassword')
nOfYears      = glb('nOfPassYears')
weatherBucket = glb('s3WeatherBucket')

def downloadFromFtp(domain,folder,login,password,filenames):
    ftp = ftplib.FTP(domain)   # Create ftp session
    ftp.login(login, password) # Login as 'anonymous'
    ftp.cwd(folder)            # Go to destination directory
    for fName in filenames:
        # Copy files to local machine
        ftp.retrbinary('RETR %s' %fName, open(fName, 'wb').write)
    ftp.quit

def noaaGzipToCsv(filename):
    # Translate the isd-lite format to csv
    readings = []
    with gzip.open(filename, "r") as text:
        for line in text:
            readings.append(parseISD.parseIsdLine(line)) # See parseISD.py

    outFName = filename.replace('.gz', '.csv')
    with open(outFName, 'w') as outFile:
        writer = csv.DictWriter(outFile, fieldnames=weatherFields)
        writer.writeheader()
        for row in readings:
            writer.writerow(row)
    return outFName

def splitIntoMonth(csvFiles, years):
    # Partition into months
    # Need to read the year before and after to account for
    # bad taxi data (some entry appears a couple of days late)
    outFiles = []
    for ind, csvFile in enumerate(csvFiles):
        curYearRec = []
        with open(csvFile) as curFile:
            dfCur = csv.DictReader(curFile)
            for row in dfCur:
                curYearRec.append(row)
        if not ind == 0: # Previous year data
            ptr = 0
            with open(csvFiles[ind - 1]) as preFile:
                dfPre = csv.DictReader(preFile)
                for row in dfPre:
                    curYearRec.insert(ptr, row)
                    ptr = ptr + 1
        if not ind == len(csvFiles) - 1: # Following year data
            with open(csvFiles[ind + 1]) as posFile:
                dfPost = csv.DictReader(posFile)
                for row in dfPost:
                    curYearRec.append(row)

        # Create a time stamp for each beginning of month
        months = [str(val + 1).zfill(2) for val in range(12)]        
        monStr = [str(years[ind]) + '-' + mn + '-01 00:00:00' for mn in months]
        monTS  = dtt.strArrayToTimeStamps(monStr)
        monTS.append(monTS[-1] + 31*24*3600)

        for cnt, month in enumerate(months):
            indices = [idx for idx, val in enumerate(curYearRec) \
                if int(val['timeStamp']) >= monTS[cnt] - 48*3600 and \
                   int(val['timeStamp']) <= monTS[cnt + 1] + 49*3600]
                   # 48 hr overlap; 49 because of the way list works

            if not indices == []:
                monthRec = curYearRec[indices[0]:indices[-1]]
                outFName = csvFile.replace('.csv', '-' + month + '.csv')
                with open(outFName, 'w') as outFile:
                    writer = csv.DictWriter(outFile, fieldnames=weatherFields)
                    writer.writeheader()
                    for row in monthRec:
                        writer.writerow(row)
                outFiles.append(outFName)

    return outFiles

def uploadToS3(filenames, bucket, path):
    for fName in filenames:
        objKey = path + '/' + fName
        S3Tools.uploadFileFromLocal(fName, bucket, objKey)

def cleanupLocal(filenames):
    for fName in filenames:
        os.remove(fName)     

def fixBadNoaaData():
    print("Central-Park 2012 APR-AUG messed up, use La-Guardia data.")
    ovr = True # Overwrite
    S3Tools.copy_among_buckets(weatherBucket, 'La-Guardia/725030-14732-2012-04.csv', \
                               weatherBucket, 'Central-Park/725053-94728-2012-04.csv', ovr)
    S3Tools.copy_among_buckets(weatherBucket, 'La-Guardia/725030-14732-2012-05.csv', \
                               weatherBucket, 'Central-Park/725053-94728-2012-05.csv', ovr)
    S3Tools.copy_among_buckets(weatherBucket, 'La-Guardia/725030-14732-2012-06.csv', \
                               weatherBucket, 'Central-Park/725053-94728-2012-06.csv', ovr)
    S3Tools.copy_among_buckets(weatherBucket, 'La-Guardia/725030-14732-2012-07.csv', \
                               weatherBucket, 'Central-Park/725053-94728-2012-07.csv', ovr)
    S3Tools.copy_among_buckets(weatherBucket, 'La-Guardia/725030-14732-2012-08.csv', \
                               weatherBucket, 'Central-Park/725053-94728-2012-08.csv', ovr)
        
def main():
    # Backup weather data
    print('Moving current data to back-up bucket.')
    backupBucket = weatherBucket + '-bak'
    S3Tools.duplicateBucket(origBucket=weatherBucket, newBucket=backupBucket)
                         
    # Years of interest: n-years back from current year
    currYear = datetime.now().year
    yearList = [cnt + currYear - nOfYears + 1 for cnt in range(nOfYears)]
    
    # Loop over a few stations of interest
    for station in noaaStations:
        csvFiles = []
        for yrInd, year in enumerate(yearList):
            pathname = noaaFtpPath + str(year) # According to NOAA dir. structure
            filename = station + '-' + str(year) + '.gz'
            print("Processing: %s" %filename)
            downloadFromFtp(noaaFtpDomain, pathname, noaaLogin, noaaPassword, [filename])
            csvFiles.append(noaaGzipToCsv(filename))
            cleanupLocal([filename]) # Remove *.gz files
            
        # Partition by month
        print("Partitioning by month...")
        outFiles = splitIntoMonth(csvFiles, yearList)
        uploadToS3(outFiles, weatherBucket, noaaStations.get(station))
        cleanupLocal(csvFiles)
        cleanupLocal(outFiles)
    
    fixBadNoaaData()
    # End of main()
            
main()
