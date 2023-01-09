#!/usr/bin/env python
# coding: utf-8

# # Notice
# 
# notebook allowing interactive work with batches

# # Imports

# In[1]:


import findspark
findspark.add_jars("/usr/local/hbase/lib/hbase-client-2.3.5.jar")
findspark.init()
#os.environ['PYSPARK_SUBMIT_ARGS'] = ("--repositories http://repo.hortonworks.com/content/groups/public/ " "--packages com.hortonworks:shc-core:1.1.1-1.6-s_2.10 ")


# In[2]:


import sys
import logging
from pyspark.sql import SparkSession
import happybase
import os
os.environ['HADOOP_CONF_DIR']='/usr/local/hadoop/etc/hadoop'
import subprocess

from pyspark.sql.functions import col, explode, lit, avg


# # Establish connection

# In[3]:


# Logging configuration
formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)


# In[4]:


spark = (
    SparkSession.builder
    .master("yarn")
    .appName("Hbase Spark")
    .getOrCreate()
)


# In[5]:


connection = happybase.Connection('localhost')


# # Utlis

# In[6]:


def get_latest_file(folder_path):
    cmd = 'hdfs dfs -ls ' + folder_path
    files = subprocess.check_output(cmd, shell=True)
    files = files.decode().strip().split('\\n')

    latest_file = files[0].split()[-1]
    
    return latest_file


# In[7]:


def get_files_list(folder_path):
    cmd = 'hdfs dfs -ls ' + folder_path
    files = subprocess.check_output(cmd, shell=True)
    files = files.decode().strip().split('\\n')
    files = files[0].split()

    only_files = []
    for name in files:
        if '.parquet' in name:
            only_files.append(name)

    return only_files


# In[8]:


# def create_or_empty_batchview(table_name, cols):
#     table_name = str.encode(table_name)
#     tables = set(connection.tables())
#     if table_name in tables:
#         connection.delete_table(table_name, disable=True)
#         tables.remove(table_name)
    
#     if table_name not in tables:
#         connection.create_table(
#             table_name,
#             cols
#         )


# In[9]:


def create_batchview_if_not_exists(table_name, cols):
    table_name = str.encode(table_name)
    tables = set(connection.tables())
    
    if table_name not in tables:
        connection.create_table(
            table_name,
            cols
        )


# In[10]:


def setup_column_families(columns, columns_mappings):
    for i in range(len(columns)):
        columns[i] = columns_mappings[columns[i]] + ':' + columns[i] 
        
    return columns



# In[12]:




# ## hourly statistics

# In[27]:


#get_files_list('/user/hive/hourly_weather')


# In[28]:


latest_file = sys.argv[1]


# In[29]:


weather_hourly = spark.read.option("header", "true").option("inferschema", "true").parquet("hdfs://localhost:8020" + latest_file)


# In[30]:


weather_hourly = weather_hourly.select("hourly.*")


# In[31]:




# In[32]:


columns = weather_hourly.columns
columns_mappings = {
    'time': 'time',
    'temperature_2m': 'temperature',
    'apparent_temperature': 'temperature',
    'precipitation': 'precipitation',
    'rain': 'precipitation',
    'showers': 'precipitation',
    'snowfall': 'precipitation',
    'windspeed_10m': 'wind',
    'windgusts_10m': 'wind'
}

columns = setup_column_families(columns, columns_mappings)


# In[33]:


table_name = 'weather_hourly'
cols = {'time': dict(),
         'temperature': dict(),
         'precipitation': dict(),
         'wind': dict(),
        }
create_batchview_if_not_exists(table_name, cols)
batchview = connection.table('weather_hourly')


# In[34]:


def write_hourly_weather_to_hbase(batchview, table, columns, latest_file):
    #retrieve an hour the file concerns
    h_index = int(latest_file.split('_')[4][:2])
    
    for row in table.collect():
        for value, column in zip(row, columns):
            batchview.put(str.encode(row['time'][h_index]), {str.encode(column): str.encode(str(value[h_index]))})


# In[35]:


write_hourly_weather_to_hbase(batchview, weather_hourly, columns, latest_file)

