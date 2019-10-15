# The schema for taxi data changes from time-to-time
# This module standardizes the schema 
# Returns 2 lists: column names and associated data types
# Author: Colin Chow
# Date created: 09/28/2019
# Version:

import re
import S3Tools as S3

synDict = { \
    'vendor_name'           : 'VendorID',
    'vendor_id'             : 'VendorID',
    'VendorID'              : 'VendorID',
    'Trip_Pickup_DateTime'  : 'pUDateTime',
    'pickup_datetime'       : 'pUDateTime',
    'tpep_pickup_datetime'  : 'pUDateTime',
    'Trip_Dropoff_DateTime' : 'dODateTime',
    'dropoff_datetime'      : 'dODateTime',
    'tpep_dropoff_datetime' : 'dODateTime',
    'Passenger_Count'       : 'nPax',
    'passenger_count'       : 'nPax',
    'Trip_Distance'         : 'distance',
    'trip_distance'         : 'distance',
    'Start_Lon'             : 'pULong',
    'pickup_longitude'      : 'pULong',
    'Start_Lat'             : 'pULat',
    'pickup_latitude'       : 'pULat',
    'PULocationID'          : 'pULocId',
    'Rate_Code'             : 'rateCode',
    'rate_code'             : 'rateCode',
    'RateCodeID'            : 'rateCode',
    'RatecodeID'            : 'rateCode',
    'store_and_forward'     : 'storeNFwdFlag',
    'store_and_fwd_flag'    : 'storeNFwdFlag',
    'End_Lon'               : 'dOLong',
    'dropoff_longitude'     : 'dOLong',
    'End_Lat'               : 'dOLat',
    'dropoff_latitude'      : 'dOLat',
    'DOLocationID'          : 'dOLocId',
    'Payment_Type'          : 'payType',
    'payment_type'          : 'payType',
    'Fare_Amt'              : 'fare',
    'fare_amount'           : 'fare',
    'extra'                 : 'extra',
    'surcharge'             : 'surcharge',
    'improvement_surcharge' : 'surcharge',
    'mta_tax'               : 'mtaTax',
    'Tip_Amt'               : 'tip',
    'tip_amount'            : 'tip',
    'Tolls_Amt'             : 'tolls',
    'tolls_amount'          : 'tolls',
    'Total_Amt'             : 'totalPaid',
    'total_amount'          : 'totalPaid',
    'congestion_surcharge'  : 'congSurcharge'
}

mapNameToType = { \
    'VendorID'      : 'STRING',
    'pUDateTime'    : 'STRING',
    'dODateTime'    : 'STRING',
    'nPax'          : 'INT',
    'distance'      : 'FLOAT',
    'pULong'        : 'FLOAT',
    'pULat'         : 'FLOAT',
    'pULocId'       : 'STRING',
    'rateCode'      : 'STRING',
    'storeNFwdFlag' : 'STRING',
    'dOLong'        : 'FLOAT',
    'dOLat'         : 'FLOAT',
    'dOLocId'       : 'STRING',
    'payType'       : 'STRING',
    'fare'          : 'FLOAT',
    'extra'         : 'FLOAT',
    'surcharge'     : 'FLOAT',
    'mtaTax'        : 'FLOAT',
    'tip'           : 'FLOAT',
    'tolls'         : 'FLOAT',
    'totalPaid'     : 'FLOAT',
    'congSurcharge' : 'FLOAT',
    'unknown'       : 'STRING'
    }

def getColNamesAndTypes(bucketName, keyName, badSchemaFlag):
    df     = S3.preview_csv_dataset(bucket=bucketName, key=keyName, rows=5)
    cols   = [synDict.get(re.sub(' ', '', clm)) for clm in df.columns]
    # Data from 07-2016 to 12-2016 is messed up
    if badSchemaFlag == '1':
        cols = filter(None, cols)
        cols.append('congSurcharge')
        cols.append('unknown')
    dTypes = [mapNameToType.get(clm) for clm in cols]
    
    return cols, dTypes