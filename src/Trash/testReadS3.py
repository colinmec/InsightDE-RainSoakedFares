import pandas as pd

def read_csv_from_s3(bucket, filename):
    print("start the read csv function:")
    #loadData = LoadData.DataIngestion(self.date)
    #data_file = loadData.data_file
    #s3_bucket = self.bucket

    filepath = "s3a://" + bucket+"/" + filename
        #mode = "PERMISSIVE"
    print('Reading file: ' + filepath)

    return pd.read_csv(filepath)
    #return self.spark.read.csv(file_name, header = True, mode = mode, schema = self.schema)

df = read_csv_from_s3('colinmec-test','trip data/green_tripdata_2017-12.csv')
df.head(20)
