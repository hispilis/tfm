import io
import sys
from io import StringIO  # Python 3.x

import boto3


class S3_Dao:
    def __init__(self, s3_bucket_name):
        """Accessing the S3 buckets using boto3 client"""
        s3_client = boto3.client("s3")
        self.s3_bucket_name = s3_bucket_name
        self.s3 = boto3.resource(
            "s3",
            aws_access_key_id="AKIAYHU6VLETQLHFVZ7C",
            aws_secret_access_key="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",
        )
        self.my_bucket = self.s3.Bucket(s3_bucket_name)

    def get_files(self, prefix, extension):
        bucket_list = []
        for file in self.my_bucket.objects.filter(Prefix=prefix):
            file_name = file.key
            if file_name.find(extension) != -1:
                bucket_list.append(file.key)
        length_bucket_list = print(bucket_list)
        return bucket_list

    def get_data(self, file):
        obj = self.s3.Object(self.s3_bucket_name, file)
        data = obj.get()["Body"].read()
        return io.BytesIO(data)
