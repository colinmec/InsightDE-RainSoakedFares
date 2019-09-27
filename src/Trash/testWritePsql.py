import sys
import boto3
import postgres
import S3ToSpark as S3
import pandas as pd
from pyspark.sql import SparkSession

s3 = boto3.client('s3')

if __name__ == "__main__":
    #if len(sys.argv) != 2:
    #    print("Usage: wordcount <file>", file=sys.stderr)
    #    sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("testWritePsql")\
        .getOrCreate()


    bucket = 'nyc-tlc'
    key = 'trip data/green_tripdata_2017-12.csv'
    data_source = {
            'Bucket': bucket,
            'Key': key
        }
    # Generate the URL to get Key from Bucket
    url = s3.generate_presigned_url(
        ClientMethod = 'get_object',
        Params = data_source
    )

    data = spark.read.csv('s3a://nyc-tlc/trip data/green_tripdata_2017-12.csv')
    


    #pd = S3.preview_csv_dataset(session='spark',bucket='nyc-tlc',key='trip data/green_tripdata_2017-12.csv')
    #lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    #counts = lines.flatMap(lambda x: x.split(' ')) \
    #              .map(lambda x: (x, 1)) \
    #              .reduceByKey(add)
    #output = counts.collect()
    #for (word, count) in output:
    #    print("%s: %i" % (word, count))
    print(data.head(25))

    mode = "append"
    connector = postgres.PostgresConnector()
    connector.write(data, 'testWrite', mode)

    spark.stop()


#spark = SparkSession.builder.appName("testWritePsql").getOrCreate()
#pd = S3.preview_csv_dataset(bucket='nyc-tlc',key='trip data/green_tripdata_2017-12.csv',rows=25)


#print(pd.head(10))
