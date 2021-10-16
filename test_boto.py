# Create a bucket
import boto3
s3_client = boto3.client('s3')
s3_client.create_bucket(Bucket="hgonzalezb1")