import boto3
import botocore
import pandas as pd
#s3 = boto3.resource('s3')
s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')
s3_resource.create_bucket(Bucket='cloudwarriors-rides-detail-bucket')
#S3 bucket creation
my_s3_bucket = "cloudwarriors-rides-detail-bucket"
#tripdata and lookdata folder creation inside s3 bucket created in step1
folder_name = "tripdata/"
s3.put_object(Bucket=my_s3_bucket, Key=(folder_name+'/'))
lookupfolder = "tripdata-lookup/"
s3.put_object(Bucket=my_s3_bucket, Key=(lookupfolder+'/'))

# we are loading data for 2020
year = 2020


# Copying data from aws repositry to my s3 bucket
# yellow, green, for-hire-vehice, high-volume-for-hire vehice trip records are fetched from aws repo
for taxitype in ('yellow','green','fhvhv','fhv'):
    for month in range(8,9):
        source_path = f"trip data/{taxitype}_tripdata_{year}-0{month}.csv"
        dest_path = f"tripdata/{taxitype}taxitriprecords/{taxitype}_tripdata_{year}-0{month}.csv".format(taxitype,year,month)
        copy_source = {
            'Bucket': 'nyc-tlc',
            'Key': source_path
        }
        print(f"Copying File from {source_path} to {dest_path}")
        s3_resource.meta.client.copy(copy_source, my_s3_bucket, dest_path)
print("Copy Completed")
print(f"Below is the list of all files present in bucket {my_s3_bucket}")
bucket_resource = s3_resource.Bucket(my_s3_bucket)
for key in bucket_resource.objects.all():
    print(f"Objects Inside buckets are : {key.key} and their size is {(key.size)/1024/1024} MB")