import boto3
import os

def connect():
    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1', # глянути в аккауні с3
        aws_access_key_id='AKIAUCAUK76AZL5LEW5V', 
        aws_secret_access_key='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')
    return s3


my_bucket = connect().Bucket('my-first-data')

for file in my_bucket.objects.all():
    print(file.key)



# conn = connect()
    
# # for bucket in connect().buckets.all():
# #     for obj in connect().Bucket(f'{bucket.name}').get_all_keys():
# #         print(obj.key)

# # for obj in bucket.get_all_keys():
# #     print(obj.key)
# for bucket in connect().buckets.all():
#     for key in conn.list_objects(Bucket=bucket.name)['Contents']:
#         print(key['Key'])

# Print out bucket names
# for bucket in connect().buckets.all():
#     name = bucket.name
#     print(name)
# for obj in connect().Bucket(f'{name}').files.all():
#     lst.append(obj.key)
# print(lst)
# print(obj.name)
#s3 = connect()


# for bucket in s3.buckets.all():
#     name = bucket.name
# # mybucket = s3.Bucket(f'{name}')
# # if blank prefix is given, return everything)
# bucket_prefix="/2020-09-28/"
# objs = s3.Bucket(f'{name}').objects.filter(
#     Prefix = bucket_prefix)
# print(objs)

# for obj in objs:
#     path, filename = os.path.split(obj.key)
#     # boto3 s3 download_file will throw exception if folder not exists
#     try:
#         os.makedirs(path) 
#     except FileExistsError:
#         pass
#     mybucket.download_file(obj.key, obj.key)

# def connect(self):         		
#       client = boto3.client('s3', aws_access_key_id=self.AWS_ACCESS_KEY,aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY)
#       return client
# import os
# os.environ["AWS_DEFAULT_REGION"] = 'us-east-2'
# os.environ["AWS_ACCESS_KEY_ID"] = 'mykey'
# os.environ["AWS_SECRET_ACCESS_KEY"] = 'mysecretkey'

# for obj in connect().Bucket.objects.all():
#     key = obj.key
#     body = obj.get()['Body'].read()