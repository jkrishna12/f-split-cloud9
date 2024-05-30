import boto3
import pandas as pd
from datetime import datetime
import re
import io


# useful code
# test_obj = s3.get_object(Bucket = name, Key = key_1, Range = f'bytes={0}-{100}')

# file_content = test_obj['Body'].read().decode("utf-8")

# print(file_content)

name = 'imdb-load-kc'

keys = ['name.basics.tsv']

#['name.basics.tsv', 'title.crew.tsv', 'title.episode.tsv','title.ratings.tsv']

max_file_size = 1000000

def get_bucket_keys(bucket_name):
    """
    """
    
    bucket_keys = []
    
    key_size = []
    
    s3 = boto3.client('s3')
    
    objects = s3.list_objects_v2(Bucket = bucket_name)
    
    print(objects)
    
    for key in objects['Contents']:
        
        # print(f"Key {key['Key']}, size {key['Size']}")
        
        bucket_keys.append(key['Key'])
        
        key_size.append(key['Size'])
        
    
    return bucket_keys, key_size

def get_date():
    """
    Returns the relevant parts of the date
    
    Paramters:
        None
        
    Returns:
        year: string, year of today's date
        month: string, month of today's date
        day: string, day of today's date
    """
    
    today_date_year = datetime.today().strftime('%Y')
    
    today_date_month = datetime.today().strftime('%m')
    
    today_date_day = datetime.today().strftime('%d')

    return today_date_year, today_date_month, today_date_day

def get_obj(bucket_name, key_name):
    """
    Gets the contents from the key 
    
    Parameters:
        bucket_name: string, name of the bucket
        key_name: string, name of the key 
        
    Returns:
        obj: dict, containing information of the key
    """
    
    s3 = boto3.client('s3')
    
    obj = s3.get_object(Bucket = bucket_name, Key = key_name)
    
    return obj
    
def get_obj_size_loc(object_dict):
    """
    """
    
    obj_body = object_dict['Body']
    
    obj_size = object_dict['ContentLength']
    
    return obj_body, obj_size
    


year, month, day = get_date()

# bucket_keys, key_size = get_bucket_keys(name)

s3 = boto3.client('s3')

# test_obj_2 = s3.get_object(Bucket = name, Key = key_1)

# counter = 0

# for chunk in pd.read_csv(test_obj_2['Body'], chunksize = 1000, delimiter = '\t'):
    
#     print(chunk.head())
    
#     print(chunk.memory_usage(deep = True).sum())
    
#     print("////")
    
#     counter += 1
    
#     if counter == 5:
#         break

def dataframe_to_bytes(df):
    
    buffer = io.BytesIO()
    
    df.to_csv(buffer, index=False)
    
    return buffer.getvalue()
    
def key_name_generator(key_name_raw, index, year, month, day):
    """
    Function provides a string representing the key name of where the specific
    chunk will be written to
    Parameters:
        folder_name: string, name of the folder to be written to, this case split_files
        key_name_raw: string, raw name of the key_name
        index: integer, number representing the chunk of the file 
        year: string, current year
        month: string, current month
        day: string, current day
    Returns:
        key_name_write: string, location of where the file will be written to
    """
    
    file_name = re.match(r"^(.*?)\.tsv", key_name_raw).group(1)
    
    key_name_write = file_name+'/'+year+'/'+month+'/'+day+'/'+file_name+'_'+str(index)+'.tsv'
    
    return key_name_write
    
def file_splitter(bucket_name, key_name, bucket_name_write, row_size = 1000):
    """
    Function will take in a key from the specified bucket and returns the split
    file into 1000 line chunks
    Parameters:
        bucket_name: string, name of the bucket
        key_name: string, name of the file key
        row_size: integer, default = 1000 specifies how many lines in each chunk
    """
    # gets dictionary containing info about the key
    object_key = get_obj(bucket_name, key_name)
    
    # gets the streaming location of the body and its size
    obj_body, obj_size = get_obj_size_loc(object_key)
    
    # prints the streaming location of the body and its size 
    print(f"object body {obj_body}, object size {obj_size}")
    
    # iterates over the object body based on the chunk size
    for index, chunk in enumerate(pd.read_csv(obj_body, chunksize = row_size, delimiter = '\t')):
        
        # creates a boto3 s3 client resource that will be used to put objects
        write_client = boto3.client('s3')
        
        # gets today's year, month, day
        year, month, day = get_date()
        
        # string containing the key name of the chunk
        print(f"key name raw {key_name}")
        key_name_write = key_name_generator(key_name, index, year, month, day)
        
        print(f"key name {key_name_write}")
        
        print("/////////////")
        
        # writes the chunk to the specified s3 bucket
        response = write_client.put_object(Body = dataframe_to_bytes(chunk), Bucket = write_bucket, Key = key_name_write)
        
        # print(response)
        
        if index == 3:
            break
        
    print('/////////')
    
    return

    
for key in keys:
    
    write_bucket = 'imdb-load-split-kc'
    
    file_splitter(name, key, write_bucket)
    
