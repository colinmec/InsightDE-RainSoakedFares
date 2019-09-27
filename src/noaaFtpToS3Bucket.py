# This module imports weather data from NOAA ftp server,
# It converts data from fixed with format to .csv, and copy to AWS S3
# Author: Colin Chow
# Date created: 09/20/2019
# Version:

import os
import csv
import gzip
import ftplib
import S3Tools          # see S3Tools.py
import parseISD         # see parseISD.py

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

def uploadToS3(filename, bucket, path):
    objKey = path + '/' + filename
    S3Tools.uploadFileFromLocal(filename, bucket, objKey)
    
def cleanupLocal(filenames):
    for fName in filenames:
        os.remove(fName)     

def main():
    # Backup weather data
    print('Moving current data to back-up bucket')
    backupBucket = weatherBucket + '-bak'
    S3Tools.duplicateBucket(origBucket=weatherBucket, newBucket=backupBucket)
                         
    # Years of interest: n-years back from current year
    currYear = datetime.now().year
    yearList = [cnt + currYear - nOfYears + 1 for cnt in range(nOfYears)]

    for year in yearList:
        pathname  = noaaFtpPath + '/' + str(year) # According to NOAA directory structure
        filenames = [station + '-' + str(year) + '.gz' for station in noaaStations.keys()]
        downloadFromFtp(noaaFtpDomain, pathname, noaaLogin, noaaPassword, filenames)
        for ind, fName in enumerate(filenames):
            print("Processing: %s" %fName)
            csvFile = noaaGzipToCsv(fName)
            uploadToS3(csvFile, weatherBucket, noaaStations.get(noaaStations.keys()[ind]))
            cleanupLocal([fName, csvFile])
            
    # End of main()
            
main()