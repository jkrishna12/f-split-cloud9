import boto3
import os
import io
import math
from datetime import datetime
import pandas as pd
import re


src_bucket = 'imdb-load-kc'


src_key = 'title.episode.tsv'
# src_key = 'name.basics.tsv'

write_bucket = 'imdb-load-split-kc'

max_file_size = 52428800

keys = ['name.basics.tsv', 'title.episode.tsv','title.crew.tsv','title.ratings.tsv']

input_file = os.path.join(src_bucket, src_key)

print(input_file)

s3 = boto3.client('s3')

obj = s3.get_object(Bucket = src_bucket, Key = src_key)

body = obj['Body']

size = obj['ContentLength']

def line_counter(obj_body):
    """
    Returns the line count of a file
    Parameters:
        obj_body: StreamingBody, location of the file
    Returns:
        line_count: integer, line count of the file
    """
    
    #nitialise line_count to 0
    line_count = 0
    
    # use context manager to decode obj_body from bytes to string
    with io.TextIOWrapper(obj_body, encoding = 'utf-8') as file:
        
        # for loop loads line at a time into memory
        for line in file:
            
            # increment line_count by 1 each time
            line_count += 1
    
    
    return line_count

def chunkisze_set(file_lines, file_bytes, default_file_bytes):
    """
    Function defines how big the the chunksize will be depending on the 
    number of lines
    Parameters:
        file_lines: integer, line count of file
        file_bytes: integer, size of the file
        default_file_bytes: integer, max file size of the chunk
    """
    
    # defines a ratio between current file size and desired default file size
    ratio = file_bytes / default_file_bytes
    
    # conditions dependent on value of ratio determines size of chunk
    if ratio > 1:
        
        # calculates how many lines should be in each chunk to have default_file_bytes
        chunksize_float = file_lines / ratio
        
        # rounds the value up to the neares integer
        chunksize = math.ceil(chunksize_float)
        
    else:
        
        # if ratio less than 1 then chunk size is set to input file_lines
        chunksize = file_lines

    return chunksize
    
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
    
def get_obj_body(bucket_name, key_name):
    
    s3 = boto3.client('s3')
    
    obj = s3.get_object(Bucket = bucket_name, Key = key_name)
    
    obj_body = obj['Body']
    
    return obj_body
    
    
    
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
    
def file_splitter(bucket_name, key_name, bucket_name_write, default_file_size):
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
    
    # get the line count of file
    line_count = line_counter(obj_body)
    
    print(f"Line count of file {key_name} is: {line_count}")
    
    # gets the number of lines per chunk
    chunksize = chunkisze_set(line_count, obj_size, default_file_size)
    
    print(f"Chunksize of file {key_name} is: {chunksize}")
    
    object_body_only = get_obj_body(bucket_name, key_name)
    
    # iterates over the object body based on the chunk size
    for index, chunk in enumerate(pd.read_csv(object_body_only, chunksize = chunksize, delimiter = '\t')):
        
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
        
        if index == 2:
            break
        
    print('/////////')
    
    return


file_splitter(src_bucket, src_key, write_bucket, max_file_size)