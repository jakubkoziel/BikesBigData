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


# In[11]:


# ## stations status live

# In[13]:


latest_file = sys.argv[1]


# In[14]:


status_live = spark.read.option("header", "true").option("inferschema", "true").parquet("hdfs://localhost:8020" + latest_file)


# In[15]:


status_live = status_live.select('data.stations')

status_live = status_live.withColumn("stations", explode("stations")).select(
"*",col("stations.*")
).select(['legacy_id', 'num_bikes_disabled', 'last_reported', 'station_id', 'is_renting', 'num_ebikes_available',
         'num_docks_disabled', 'is_installed', 'num_bikes_available', 'is_returning', 'station_status', 'num_docks_available'])


# In[16]:


columns = status_live.columns


# In[17]:


columns = status_live.columns
columns_mappings = {
    'legacy_id': 'id',
    'num_bikes_disabled': 'metric',
    'last_reported': 'id',
    'station_id': 'id',
    'is_renting': 'status',
    'num_ebikes_available': 'metric',
    'num_docks_disabled': 'metric',
    'is_installed': 'status',
    'num_bikes_available': 'metric',
    'is_returning': 'status',
    'station_status': 'status',
    'num_docks_available': 'metric'
}

columns = setup_column_families(columns, columns_mappings)


# In[18]:


table_name = 'status_live'
cols = {
    'id': dict(),
    'metric': dict(),
    'status': dict(),
        }
create_batchview_if_not_exists(table_name, cols)
batchview = connection.table('status_live')


# In[19]:


def write_station_status_live_to_hbase(batchview, table, columns):
    
    for row in table.collect():
        for value, column in zip(row, columns):
            batchview.put(str.encode(str(row['station_id'])), {str.encode(column): str.encode(str(value))})


# In[20]:


write_station_status_live_to_hbase(batchview, status_live, columns)




