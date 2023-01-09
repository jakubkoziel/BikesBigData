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


# ## station information daily

# In[37]:


latest_file = sys.argv[1]


# In[38]:


information_daily = spark.read.option("header", "true").option("inferschema", "true").parquet("hdfs://localhost:8020" + latest_file)


# In[39]:


information_daily = information_daily.select('data.stations')

information_daily = information_daily.withColumn("stations", explode("stations")).select(
"*",col("stations.*")
).select(['lon', 'external_id', 'legacy_id', 'station_id', 'has_kiosk',
         'capacity', 'lat', 'region_id', 'station_type', 'short_name', 'electric_bike_surcharge_waiver',
         'name'])


# In[40]:


columns = information_daily.columns
columns_mappings = {
    'lon': 'geo',
    'external_id': 'id',
    'legacy_id': 'id',
    'station_id': 'id',
    'has_kiosk': 'detail',
    'capacity': 'detail',
    'lat': 'geo',
    'region_id': 'id',
    'station_type': 'detail',
    'short_name': 'detail',
    'electric_bike_surcharge_waiver': 'detail',
    'name': 'detail'
}

columns = setup_column_families(columns, columns_mappings)


# In[41]:


table_name = 'information_daily'
cols = {
    'id': dict(),
    'geo': dict(),
    'detail': dict()
        }
create_batchview_if_not_exists(table_name, cols)
batchview = connection.table('information_daily')


# In[42]:


def write_station_information_to_hbase(batchview, table, columns, latest_file):
    #retrieve the day that the file concerns
    day = latest_file.split('_')[3]
    
    for row in table.collect():
        for value, column in zip(row, columns):
            if column == 'geo:lat' or column == 'geo:lon':
                batchview.put(str.encode(day + '_' + str(row['station_id'])), {str.encode(column): str.encode(str(value.member1))})
            else:
                batchview.put(str.encode(day + '_' + str(row['station_id'])), {str.encode(column): str.encode(str(value))})


# In[43]:


write_station_information_to_hbase(batchview, information_daily, columns, latest_file)




