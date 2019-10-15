# Sample a subset of data for code development and testing
# Keep separate from the rest of the codes (Don't source globalVar)
# Author: Colin Chow
# Date created: 09/26/2019
# Version:

import os
import pandas as pd
import S3Tools as S3 # Custom, see S3Tools.py

def main():
    sourceBucket = 'nyc-tlc'
    targetBucket = 'colinmec-nyc-tlc-5pc'
    partialPath  = 'trip data/yellow_tripdata_'

    years  = [val + 2009 for val in range(11)]
    months = [val + 1 for val in range(12)]

    # Generate key names according to nyc-tlc naming format
    keys = []
    badSchema = []
    for yr in years:
        for mn in months:
            filepath = partialPath + str(yr) + '-' + str(mn).zfill(2) + '.csv'
            keys.append(filepath)
            if yr == 2016 and mn in [7,8,9,10,11,12]:
                badSchema.append(filepath)            

    # Load, sample, save, copy and clean-up
    for keyname in keys:
        if S3.key_exists(sourceBucket, keyname):
            print("Processing: %s" %(keyname))
            nrows   = 5000000 # Memory error if significantly larger
            dnScale = 20
            cnt     = 0
            while True:
                if cnt == 0:
                    hFlag = True
                else:
                    hFlag = False
                if keyname in badSchema:
                    df = S3.preview_csv_dataset(bucket=sourceBucket, key=keyname, rows=nrows, skip=cnt*nrows+1)
                    df_short = df.sample(frac=1.0/dnScale, random_state=1)
                    if hFlag == True:
                        hFlag = ['VendorID','tpep_pickup_datetime','tpep_dropoff_datetime','passenger_count',    \
                                 'trip_distance','RatecodeID','store_and_fwd_flag','PULocationID','DOLocationID',\
                                 'payment_type','fare_amount','extra','mta_tax','tip_amount','tolls_amount',     \
                                 'improvement_surcharge','total_amount','','']
                    df_short.to_csv('tmp.csv',header=hFlag,index=False,mode='a')
                else:
                    df = S3.preview_csv_dataset(bucket=sourceBucket, key=keyname, rows=nrows, skip=cnt*nrows)
                    df_short = df.sample(frac=1.0/dnScale, random_state=1)
                    df_short.to_csv('tmp.csv',header=hFlag,index=False,mode='a')
                cnt = cnt + 1
                if  df.shape[0] < nrows:
                    break
            S3.uploadFileFromLocal('tmp.csv', targetBucket, keyname)
            os.remove('tmp.csv')
    # End of main()

if __name__ == "__main__":
    main()