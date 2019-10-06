# Source: https://github.com/aws-samples/aws-open-data-analytics-notebooks
# Colin added last two methods

import boto3
import botocore
import pandas as pd
from IPython.display import display, Markdown

s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')

def create_bucket(bucket):
    import logging

    try:
        s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
    except botocore.exceptions.ClientError as e:
        logging.error(e)
        return 'Bucket ' + bucket + ' could not be created.'
    return 'Created or already exists ' + bucket + ' bucket.'

def list_buckets(match=''):
    response = s3.list_buckets()
    if match:
        print("Existing buckets containing \'" +  match + "\' string:")
    else:
        print('All existing buckets:')
    for bucket in response['Buckets']:
        if match:
            if match in bucket["Name"]:
                print(bucket["Name"])

def list_bucket_contents(bucket, match='', size_mb=0):
    bucket_resource = s3_resource.Bucket(bucket)
    total_size_gb = 0
    total_files = 0
    match_size_gb = 0
    match_files = 0
    for key in bucket_resource.objects.all():
        key_size_mb = key.size/1024/1024
        total_size_gb += key_size_mb
        total_files += 1
        list_check = False
        if not match:
            list_check = True
        elif match in key.key:
            list_check = True
        if list_check and not size_mb:
            match_files += 1
            match_size_gb += key_size_mb
            print('%s (%3.0f MB)' %(key.key, key_size_mb))
        elif list_check and key_size_mb <= size_mb:
            match_files += 1
            match_size_gb += key_size_mb
            print('%s (%3.0f MB)' %(key.key, key_size_mb))

    if match:
        print('Matched file size is %3.1f GB with %d files' %(match_size_gb/1024, match_files))

    print('Bucket \'%s\' total size is %3.1f GB with %d files' %(bucket, total_size_gb/1024, total_files))

def preview_csv_dataset(bucket, key, rows=10, skip=None):
    data_source = {
            'Bucket': bucket,
            'Key': key
        }
    # Generate the URL to get Key from Bucket
    url = s3.generate_presigned_url(
        ClientMethod = 'get_object',
        Params = data_source
    )

    data = pd.read_csv(url, nrows=rows, skiprows=skip, error_bad_lines=False, low_memory=False)
    return data

def key_exists(bucket, key):
    try:
        s3_resource.Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The key does not exist.
            return(False)
        else:
            # Something else has gone wrong.
            raise
    else:
        # The key does exist.
        return(True)

def copy_among_buckets(from_bucket, from_key, to_bucket, to_key, ovr):
    if ovr or not key_exists(to_bucket, to_key):
        s3_resource.meta.client.copy({'Bucket': from_bucket, 'Key': from_key}, 
                                        to_bucket, to_key)        
        print('File %s saved to S3 bucket %s' %(to_key, to_bucket))
    else:
        print('File %s already exists in S3 bucket %s' %(to_key, to_bucket))

def s3_select(bucket, key, statement):
    import io

    s3_select_results = s3.select_object_content(
        Bucket=bucket,
        Key=key,
        Expression=statement,
        ExpressionType='SQL',
        InputSerialization={'CSV': {"FileHeaderInfo": "Use"}},
        OutputSerialization={'JSON': {}},
    )

    for event in s3_select_results['Payload']:
        if 'Records' in event:
            df = pd.read_json(io.StringIO(event['Records']['Payload'].decode('utf-8')), lines=True)
        elif 'Stats' in event:
            print("Scanned: %5.2f MB" %(int(event['Stats']['Details']['BytesScanned'])/1024/1024))            
            print("Processed: %5.2f MB" %(int(event['Stats']['Details']['BytesProcessed'])/1024/1024))
            print("Returned: %5.2f MB" %(int(event['Stats']['Details']['BytesReturned'])/1024/1024))
    return (df)

def uploadFileFromLocal(filename, bucket, key):
    s3.upload_file(filename, bucket, key)

def duplicateBucket(origBucket, newBucket):
    for key in s3.list_objects(Bucket=origBucket)['Contents']:
        files = key['Key']
        copy_source = {'Bucket': origBucket,'Key': files}
        s3_resource.meta.client.copy(copy_source, newBucket, files)
