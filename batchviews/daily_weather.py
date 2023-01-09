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
import subprocess
import sys

from pyspark.sql.functions import col, explode, lit, avg

print(os.getenv('HADOOP_CONF_DIR'))
os.environ['HADOOP_CONF_DIR']='/usr/local/hadoop/etc/hadoop'

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


# In[11]:


#def show_batchview_from_hbase(batchview):
#    for key, data in batchview.scan():
#        print(f'KEY: {key}')
#        for column, value in data.items():
#            print(f'\tCOLUMN: {column} VALUE: {value}')


# In[12]:


"""not working"""
# def setup_table_schema(namespace, table_name, row_key, key, columns_mappings):
    
#     columns_str = ''
#     for c in columns_mappings.keys():
#         #print(c, column_mappings[c])
#         columns_str += '"' + c + '":{"cf":"' + columns_mappings[c][0] +'", "col":"' + c + '", "type":"' + columns_mappings[c][1] + '"},'
        
        
#     catalog = '{\
#         "table":{"namespace":"' + namespace + '", "name":"' + table_name + '"},\
#         "' + row_key + '":"' + key + '",\
#         "columns":{\
#         ' + columns_str + '}\
#         }'
        
    
#     return catalog


# In[13]:


"""not working"""
# def write_to_hbase(df, catalog):
#     df.write\
#     .options(catalog=catalog)\
#     .option('hbase.config.resources', 'file:///usr/local/hbase/conf/hbase-site.xml') \
#     .format("org.apache.spark.sql.execution.datasources.hbase")\
#     .save()
    
#     #.option('hbase.config.resources', 'file:///usr/local/hbase/conf/hbase-site.xml') \


# # Weather

# ## daily statistics

# In[14]:


latest_file = sys.argv[1]


# In[15]:


weather_daily = spark.read.option("header", "true").option("inferschema", "true").parquet("hdfs://localhost:8020" + latest_file)


# In[16]:


weather_daily = weather_daily.select("daily.*")


# In[17]:


columns = weather_daily.columns
columns_mappings = {
    'time': 'time',
    'temperature_2m_max': 'temperature',
    'temperature_2m_min': 'temperature',
    'apparent_temperature_max': 'temperature',
    'apparent_temperature_min': 'temperature',
    'precipitation_sum': 'precipitation',
    'rain_sum': 'precipitation',
    'showers_sum': 'precipitation',
    'snowfall_sum': 'precipitation',
    'windspeed_10m_max': 'wind',
    'windgusts_10m_max': 'wind'
}

columns = setup_column_families(columns, columns_mappings)


# In[18]:


table_name = 'weather_daily'
cols = {'time': dict(),
         'temperature': dict(),
         'precipitation': dict(),
         'wind': dict(),
        }
create_batchview_if_not_exists(table_name, cols)
batchview = connection.table('weather_daily')


# In[19]:


def write_daily_weather_to_hbase(batchview, table, columns):
    for row in table.collect():
        for value, column in zip(row, columns):
            batchview.put(str.encode(row['time'][0]), {str.encode(column): str.encode(str(value[0]))})


# In[20]:


write_daily_weather_to_hbase(batchview, weather_daily, columns)


# In[21]:


#show_batchview_from_hbase(batchview)

