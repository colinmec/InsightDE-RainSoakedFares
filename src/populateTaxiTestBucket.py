# Sample a subset of data for code development and testing
# Keep separate from the rest of the codes (Don't source globalVar)
# Author: Colin Chow
# Date created: 09/26/2019
# Version:

import os
import pandas as pd
import S3Tools as S3 # Custom, see S3Tools.py

sourceBucket = 'nyc-tlc'
targetBucket = 'colinmec-test-nyc-tlc'
partialPath  = 'trip data/yellow_tripdata_'
nSamples     = 100000

years  = [val + 2009 for val in range(11)]
months = [val + 1 for val in range(12)]

# Generate key names according to nyc-tlc naming format
keys = []
for yr in years:
    for mn in months:
        keys.append(partialPath + str(yr) + '-' + str(mn).zfill(2) + '.csv')

# Load, sample, save, copy and clean-up
for keyname in keys:
    if S3.key_exists(sourceBucket, keyname):
        print("Processing: %s" %(keyname))
        df = S3.preview_csv_dataset(bucket=sourceBucket, key=keyname)
        df_short = df.sample(n=nSamples, random_state=1, replace=True) # Error if False
        df_short.to_csv('tmp.csv',index=False)
        S3.uploadFileFromLocal('tmp.csv', targetBucket, keyname)
        os.remove('tmp.csv')
    