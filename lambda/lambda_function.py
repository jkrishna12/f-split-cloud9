import json
import boto3
import os
import io
import math
from datetime import datetime
import pandas as pd
import re

def lambda_handler(event, context):

    max_file_size = 52428800
    
    threshold = 1.5 * max_file_size
    
    write_bucket = 'imdb-load-split-kc'

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    
    key_name = event['Records'][0]['s3']['object']['key']
    
    print(f'Bucket name {bucket_name}')
    
    print(f'Key name {key_name}')
    
    object_dict = get_obj(bucket_name, key_name)
    
    print(object_dict)
    
    obj_body, obj_size = get_obj_size_loc(object_dict)
    
    print(f'Object body {obj_body}, object size {obj_size}')
    
    # creates a boto3 s3 client resource that will be used to put objects
    write_client = boto3.client('s3')
    
    # gets today's year, month, day
    year, month, day = get_date()
    
    if obj_size <= threshold:
        
        df = pd.read_csv(obj_body, delimiter = '\t')
        
        key_name_write = key_name_generator(key_name, year, month, day, index = None)
        print(f"key name {key_name_write}")
        
        # writes the chunk to the specified s3 bucket
        response = write_client.put_object(Body = dataframe_to_bytes(df), Bucket = write_bucket, Key = key_name_write)
        
    else:
        line_count = line_counter(obj_body)
        
        print(f'Line count of file is {line_count}')
        
        chunksize = chunkisze_set(line_count, obj_size, max_file_size)
        
        print(f'Chunksize = {chunksize}')
        
        object_body_only = get_obj_body(bucket_name, key_name)
        
        # iterates over the object body based on the chunk size
        for index, chunk in enumerate(pd.read_csv(object_body_only, chunksize = chunksize, delimiter = '\t')):
            
            key_name_write = key_name_generator(key_name, year, month, day, index = index)
            print(f"key name {key_name_write}")
            
            # writes the chunk to the specified s3 bucket
            response = write_client.put_object(Body = dataframe_to_bytes(chunk), Bucket = write_bucket, Key = key_name_write)
            
            if index == 2:
                break
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
    
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
    """
    Returns object body of the object, necessary after using line_counter function
    Pointer is reintialised to the beginning of object
    """
    
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
    
def key_name_generator(key_name_raw, year, month, day, index = None):
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
    
    if index == None:
        key_name_write = file_name+'/'+year+'/'+month+'/'+day+'/'+file_name+'.tsv'
        
    else:
        key_name_write = file_name+'/'+year+'/'+month+'/'+day+'/'+file_name+'_'+str(index)+'.tsv'
    
    return key_name_write
    
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
    if ratio > 1.5:
        
        # calculates how many lines should be in each chunk to have default_file_bytes
        chunksize_float = file_lines / ratio
        
        # rounds the value up to the neares integer
        chunksize = math.ceil(chunksize_float)
        
    else:
        
        # if ratio less than 1 then chunk size is set to input file_lines
        chunksize = file_lines

    return chunksize
