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
#os.environ['PATH'] += ':/usr/local/hadoop/hadoop-2.7.0/bin'
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
    cmd = '/usr/local/hadoop/bin/hdfs dfs -ls ' + folder_path
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



# ## station status hourly

# In[17]:


all_files = get_files_list('/user/hive/station_status')


# In[18]:


# DAY = '2023-01-05'
# hours = ['00', '01', '02', '03', '04', '05', '06', '07', '08']#,'09', '10', '11', '12', '13', '14', '15']
# df = pd.DataFrame(columns=['time:time', 'id:legacy_id', 'id:station_id', 'metric:num_bikes_disabled', 'metric:num_ebikes_available','metric:num_docks_disabled',
#                           'metric:num_bikes_available', 'metric:num_docks_available'])



# In[19]:

latest_file = sys.argv[1]

time = latest_file.split('/')[4][15:28]
import datetime
time = datetime.datetime.strptime(time, "%Y-%m-%d_%H") - datetime.timedelta(hours=1, minutes=0)
time = time.strftime("%Y-%m-%d_%H")

time_files = []

for name in all_files:
    if time in name:
        time_files.append(name)
        


# In[20]:


whole_hour = None
for name in time_files:
    single_file = spark.read.option("header", "true").option("inferschema", "true").parquet("hdfs://localhost:8020"+name)
    single_file = single_file.select("data.stations")
    single_file = single_file.withColumn("stations", explode("stations")).select(
        "*",col("stations.*")
        ).select(['legacy_id', 'station_id', 'num_bikes_disabled', 'num_ebikes_available',
        'num_docks_disabled', 'num_bikes_available', 'num_docks_available'])

    columns = single_file.columns
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

    if whole_hour == None:
        whole_hour = single_file
    else:
        whole_hour = whole_hour.union(single_file)#sprawdzic w durga strone


status_hourly = whole_hour.groupBy("legacy_id", 'station_id').agg(avg("num_bikes_disabled").alias("num_bikes_disabled"), 
     avg("num_ebikes_available").alias("num_ebikes_available"), 
     avg("num_docks_disabled").alias("num_docks_disabled"),
     avg("num_bikes_available").alias("num_bikes_available"), 
     avg("num_docks_available").alias("num_docks_available") 
 )

columns = status_hourly.columns
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



# In[21]:


table_name = 'status_hourly'
cols = {
    'id': dict(),
    'metric': dict(),
    'status': dict()
        }
create_batchview_if_not_exists(table_name, cols)
batchview = connection.table('status_hourly')


# In[22]:


def write_station_status_hourly_to_hbase(batchview, table, columns, time):
    
    for row in table.collect():
        for value, column in zip(row, columns):
            batchview.put(str.encode(time + '_' + str(row['station_id'])), {str.encode(column): str.encode(str(value))})


# In[23]:


write_station_status_hourly_to_hbase(batchview, status_hourly, columns, time)


# In[24]:


