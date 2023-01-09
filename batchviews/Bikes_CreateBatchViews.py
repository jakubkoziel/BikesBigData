#!/usr/bin/env python
# coding: utf-8

# # Notice
# 
# notebook allowing interactive work with batches

# Current batchviews: weather_daily, weather_hourly, information_daily, status_hourly, status_live

# In[ ]:


#latest_file = sys.argv[1]


# In[25]:


"""WARNING this can empty batchviews, for testing etc. You probably don't want to call this cell"""
def empty_batchview(table_name):
    table_name = str.encode(table_name)
    tables = set(connection.tables())
    if table_name in tables:
        connection.delete_table(table_name, disable=True)
        tables.remove(table_name)
    
        
# empty_batchview('weather_daily')  
# empty_batchview('weather_hourly') 
# empty_batchview('information_daily') 
# empty_batchview('status_hourly') 
# empty_batchview('status_live') 


# In[25]:


# weather_hourly = connection.table('weather_hourly')
# show_batchview_from_hbase(weather_hourly)


# In[26]:


# weather_daily= connection.table('weather_daily')
# show_batchview_from_hbase(weather_daily)


# In[27]:


# information_daily = connection.table('information_daily')
# show_batchview_from_hbase(information_daily)


# In[28]:


# status_hourly = connection.table('status_hourly')
# show_batchview_from_hbase(status_hourly)


# In[29]:


# status_live = connection.table('status_live')
# show_batchview_from_hbase(status_live)


# In[ ]:





# # Imports

# In[4]:


import findspark
findspark.add_jars("/usr/local/hbase/lib/hbase-client-2.3.5.jar")
findspark.init()
#os.environ['PYSPARK_SUBMIT_ARGS'] = ("--repositories http://repo.hortonworks.com/content/groups/public/ " "--packages com.hortonworks:shc-core:1.1.1-1.6-s_2.10 ")


# In[5]:


import sys
import logging
from pyspark.sql import SparkSession
import happybase
import os
os.environ['HADOOP_CONF_DIR']='/usr/local/hadoop/etc/hadoop'
import subprocess
import datetime

from pyspark.sql.functions import col, explode, lit, avg


# # Establish connection

# In[6]:


# Logging configuration
formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)


# In[7]:


spark = (
    SparkSession.builder
    .master("yarn")
    .appName("Hbase Spark")
    .getOrCreate()
)


# In[ ]:


connection = happybase.Connection('localhost')


# # Utlis

# In[ ]:


def get_latest_file(folder_path):
    cmd = 'hdfs dfs -ls ' + folder_path
    files = subprocess.check_output(cmd, shell=True)
    files = files.decode().strip().split('\\n')

    latest_file = files[0].split()[-1]
    
    return latest_file


# In[ ]:


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


# In[ ]:


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


# In[ ]:


def create_batchview_if_not_exists(table_name, cols):
    table_name = str.encode(table_name)
    tables = set(connection.tables())
    
    if table_name not in tables:
        connection.create_table(
            table_name,
            cols
        )


# In[ ]:


def setup_column_families(columns, columns_mappings):
    for i in range(len(columns)):
        columns[i] = columns_mappings[columns[i]] + ':' + columns[i] 
        
    return columns


# In[ ]:


def show_batchview_from_hbase(batchview):
    for key, data in batchview.scan():
        print(f'KEY: {key}')
        for column, value in data.items():
            print(f'\tCOLUMN: {column} VALUE: {value}')


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


latest_file = get_latest_file('/user/hive/daily_weather')


# In[38]:


weather_daily = spark.read.option("header", "true").option("inferschema", "true").parquet("hdfs://localhost:8020" + latest_file)


# In[39]:


weather_daily = weather_daily.select("daily.*")


# In[40]:


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


# In[41]:


table_name = 'weather_daily'
cols = {'time': dict(),
         'temperature': dict(),
         'precipitation': dict(),
         'wind': dict(),
        }
create_batchview_if_not_exists(table_name, cols)
batchview = connection.table('weather_daily')


# In[42]:


def write_daily_weather_to_hbase(batchview, table, columns):
    for row in table.collect():
        for value, column in zip(row, columns):
            batchview.put(str.encode(row['time'][0]), {str.encode(column): str.encode(str(value[0]))})


# In[43]:


write_daily_weather_to_hbase(batchview, weather_daily, columns)


# In[44]:


show_batchview_from_hbase(batchview)


# #### the nicer way of writing the batchviews to hbase unfortunately doesn't work. Dependency cannot be resolved despite using official repository whie submmiting pyspark job

# In[22]:


# columns = weather_daily.columns
# columns_mappings = {
#     'time': ['time', 'string'],
#     'temperature_2m_max': ['temperature', 'double'],
#     'temperature_2m_min': ['temperature', 'double'],
#     'apparent_temperature_max': ['temperature', 'double'],
#     'apparent_temperature_min': ['temperature', 'double'],
#     'precipitation_sum': ['precipitation', 'double'],
#     'rain_sum': ['precipitation', 'double'],
#     'showers_sum': ['precipitation', 'double'],
#     'snowfall_sum': ['precipitation', 'double'],
#     'windspeed_10m_max': ['wind', 'double'],
#     'windgusts_10m_max': ['wind', 'double']
# }


# In[23]:


# table_name = 'weather_daily'
# cols = {'time': dict(),
#          'temperature': dict(),
#          'precipitation': dict(),
#          'wind': dict(),
#         }
# create_batchview_if_not_exists(table_name, cols)


# In[24]:


# namespace = 'bikes'
# row_key = 'time'
# key = 'time'

# catalog = setup_table_schema(namespace, table_name, row_key, key, columns_mappings)


# In[25]:


#!pyspark --packages com.hortonworks\:shc-core:1.1.1-2.1-s_2.11 --repositories http://repo.hortonworks.com/content/groups/public/


# In[26]:


#write_to_hbase(weather_daily, catalog)


# ## hourly statistics

# In[46]:


#get_files_list('/user/hive/hourly_weather')


# In[47]:





# In[15]:


"fill the missing period"
# files = ['/user/hive/hourly_weather/hourly_weather_2023-01-05_17-08-04-891.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-05_18-01-00-317.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-05_19-01-00-396.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-05_20-01-00-343.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-05_21-01-00-347.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-05_22-01-00-309.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-05_23-01-00-301.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_00-01-00-351.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_01-01-00-337.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_02-01-00-314.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_03-01-00-333.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_04-01-00-293.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_05-01-00-323.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_06-01-00-317.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_07-01-00-311.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_08-01-00-313.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_09-01-00-362.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_10-01-00-322.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_11-01-00-597.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_12-01-00-469.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_13-01-00-270.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_14-01-00-295.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_15-01-00-300.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_16-01-00-344.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_17-01-00-294.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_18-01-00-277.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_19-01-00-292.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_20-01-00-291.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_21-01-00-298.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_22-01-00-298.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-06_23-01-00-354.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_00-01-00-295.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_01-01-00-324.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_02-01-00-354.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_03-01-00-795.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_04-01-00-305.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_05-01-00-302.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_06-01-00-351.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_07-01-00-431.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_08-01-00-318.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_09-01-00-391.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_10-01-00-314.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_11-01-00-354.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_12-01-00-420.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_13-01-00-347.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_14-01-00-352.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_15-01-00-321.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_16-01-00-316.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_17-01-00-363.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_18-01-00-344.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_19-01-00-331.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_20-01-00-297.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_21-01-00-471.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_22-01-00-314.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-07_23-01-00-299.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-08_00-01-00-333.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-08_01-01-00-308.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-08_02-01-00-303.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-08_03-01-00-299.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-08_04-01-00-292.parquet',
#  '/user/hive/hourly_weather/hourly_weather_2023-01-08_05-01-00-306.parquet']

# for f in files:
#     latest_file = f
#     latest_file.split('/')[4][15:28]
#     weather_hourly = spark.read.option("header", "true").option("inferschema", "true").parquet("hdfs://localhost:8020" + latest_file)
#     weather_hourly = weather_hourly.select("hourly.*")
#     columns = weather_hourly.columns
#     columns_mappings = {
#         'time': 'time',
#         'temperature_2m': 'temperature',
#         'apparent_temperature': 'temperature',
#         'precipitation': 'precipitation',
#         'rain': 'precipitation',
#         'showers': 'precipitation',
#         'snowfall': 'precipitation',
#         'windspeed_10m': 'wind',
#         'windgusts_10m': 'wind'
#     }

#     columns = setup_column_families(columns, columns_mappings)
    
#     table_name = 'weather_hourly'
#     cols = {'time': dict(),
#              'temperature': dict(),
#              'precipitation': dict(),
#              'wind': dict(),
#             }
#     create_batchview_if_not_exists(table_name, cols)
#     batchview = connection.table('weather_hourly')
    
#     write_hourly_weather_to_hbase(batchview, weather_hourly, columns, latest_file)


# In[ ]:


latest_file = get_latest_file('/user/hive/hourly_weather')


# In[ ]:


latest_file


# In[ ]:


latest_file.split('/')[4][15:28]


# In[ ]:


weather_hourly = spark.read.option("header", "true").option("inferschema", "true").parquet("hdfs://localhost:8020" + latest_file)


# In[ ]:


weather_hourly = weather_hourly.select("hourly.*")


# In[ ]:


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


# In[ ]:


table_name = 'weather_hourly'
cols = {'time': dict(),
         'temperature': dict(),
         'precipitation': dict(),
         'wind': dict(),
        }
create_batchview_if_not_exists(table_name, cols)
batchview = connection.table('weather_hourly')


# In[14]:


def write_hourly_weather_to_hbase(batchview, table, columns, latest_file):
    #retrieve an hour the file concerns
    h_index = int(latest_file.split('_')[4][:2])
    
    for row in table.collect():
        for value, column in zip(row, columns):
            batchview.put(str.encode(row['time'][h_index]), {str.encode(column): str.encode(str(value[h_index]))})


# In[35]:


write_hourly_weather_to_hbase(batchview, weather_hourly, columns, latest_file)


# In[16]:


show_batchview_from_hbase(batchview)


# # Stations

# ## station information daily

# In[37]:


latest_file = get_latest_file('/user/hive/station_information')


# In[35]:


information_daily = spark.read.option("header", "true").option("inferschema", "true").parquet("hdfs://localhost:8020" + latest_file)


# In[36]:


information_daily = information_daily.select('data.stations')

information_daily = information_daily.withColumn("stations", explode("stations")).select(
"*",col("stations.*")
).select(['lon', 'external_id', 'legacy_id', 'station_id', 'has_kiosk',
         'capacity', 'lat', 'region_id', 'station_type', 'short_name', 'electric_bike_surcharge_waiver',
         'name'])


# In[37]:


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


# In[38]:


table_name = 'information_daily'
cols = {
    'id': dict(),
    'geo': dict(),
    'detail': dict()
        }
create_batchview_if_not_exists(table_name, cols)
batchview = connection.table('information_daily')


# In[39]:


def write_station_information_to_hbase(batchview, table, columns, latest_file):
    #retrieve the day that the file concerns
    day = latest_file.split('_')[3]
    
    for row in table.collect():
        for value, column in zip(row, columns):
            if column == 'geo:lat' or column == 'geo:lon':
                batchview.put(str.encode(day + '_' + str(row['station_id'])), {str.encode(column): str.encode(str(value.member1))})
            else:
                batchview.put(str.encode(day + '_' + str(row['station_id'])), {str.encode(column): str.encode(str(value))})


# In[40]:


write_station_information_to_hbase(batchview, information_daily, columns, latest_file)


# In[41]:


show_batchview_from_hbase(batchview)


# ## station status hourly

# In[16]:


all_files = get_files_list('/user/hive/station_status')


# In[18]:


# DAY = '2023-01-05'
# hours = ['00', '01', '02', '03', '04', '05', '06', '07', '08']#,'09', '10', '11', '12', '13', '14', '15']
# df = pd.DataFrame(columns=['time:time', 'id:legacy_id', 'id:station_id', 'metric:num_bikes_disabled', 'metric:num_ebikes_available','metric:num_docks_disabled',
#                           'metric:num_bikes_available', 'metric:num_docks_available'])


# In[13]:


# l = []

# s = datetime.datetime.strptime('2023-01-05_17', "%Y-%m-%d_%H")
# stop = datetime.datetime.strptime('2023-01-08_05', "%Y-%m-%d_%H")
# while s <= stop:
#     l.append(s.strftime("%Y-%m-%d_%H"))
#     s += datetime.timedelta(hours=1, minutes=0)

#l


# In[17]:


# def write_station_status_hourly_to_hbase(batchview, table, columns, time):
    
#     for row in table.collect():
#         for value, column in zip(row, columns):
#             batchview.put(str.encode(time + '_' + str(row['station_id'])), {str.encode(column): str.encode(str(value))})


# for time in l:
#     time_files = []

#     for name in all_files:
#         if time in name:
#             time_files.append(name)    
            
#     whole_hour = None
#     for name in time_files:
#         single_file = spark.read.option("header", "true").option("inferschema", "true").parquet("hdfs://localhost:8020"+name)
#         single_file = single_file.select("data.stations")
#         single_file = single_file.withColumn("stations", explode("stations")).select(
#             "*",col("stations.*")
#             ).select(['legacy_id', 'station_id', 'num_bikes_disabled', 'num_ebikes_available',
#             'num_docks_disabled', 'num_bikes_available', 'num_docks_available'])

#         columns = single_file.columns
#         columns_mappings = {
#             'legacy_id': 'id',
#             'num_bikes_disabled': 'metric',
#             'last_reported': 'id',
#             'station_id': 'id',
#             'is_renting': 'status',
#             'num_ebikes_available': 'metric',
#             'num_docks_disabled': 'metric',
#             'is_installed': 'status',
#             'num_bikes_available': 'metric',
#             'is_returning': 'status',
#             'station_status': 'status',
#             'num_docks_available': 'metric'
#         }

#         columns = setup_column_families(columns, columns_mappings)

#         if whole_hour == None:
#             whole_hour = single_file
#         else:
#             whole_hour = whole_hour.union(single_file)#sprawdzic w durga strone


#     status_hourly = whole_hour.groupBy("legacy_id", 'station_id').agg(avg("num_bikes_disabled").alias("num_bikes_disabled"), 
#          avg("num_ebikes_available").alias("num_ebikes_available"), 
#          avg("num_docks_disabled").alias("num_docks_disabled"),
#          avg("num_bikes_available").alias("num_bikes_available"), 
#          avg("num_docks_available").alias("num_docks_available") 
#      )

#     columns = status_hourly.columns
#     columns_mappings = {
#         'legacy_id': 'id',
#         'num_bikes_disabled': 'metric',
#         'last_reported': 'id',
#         'station_id': 'id',
#         'is_renting': 'status',
#         'num_ebikes_available': 'metric',
#         'num_docks_disabled': 'metric',
#         'is_installed': 'status',
#         'num_bikes_available': 'metric',
#         'is_returning': 'status',
#         'station_status': 'status',
#         'num_docks_available': 'metric'
#     }

#     columns = setup_column_families(columns, columns_mappings)

#     table_name = 'status_hourly'
#     cols = {
#         'id': dict(),
#         'metric': dict(),
#         'status': dict()
#             }
#     create_batchview_if_not_exists(table_name, cols)
#     batchview = connection.table('status_hourly')
    
#     write_station_status_hourly_to_hbase(batchview, status_hourly, columns, time)


# In[ ]:





# In[ ]:





# In[8]:


time = '2023-01-05_17'
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


# In[18]:


show_batchview_from_hbase(batchview)


# ## stations status live

# In[13]:


latest_file = get_latest_file('/user/hive/station_status')


# In[19]:


latest_file = '/user/hive/station_status/station_status_2023-01-08_05-12-23-817.parquet'


# In[20]:


status_live = spark.read.option("header", "true").option("inferschema", "true").parquet("hdfs://localhost:8020" + latest_file)


# In[21]:


status_live = status_live.select('data.stations')

status_live = status_live.withColumn("stations", explode("stations")).select(
"*",col("stations.*")
).select(['legacy_id', 'num_bikes_disabled', 'last_reported', 'station_id', 'is_renting', 'num_ebikes_available',
         'num_docks_disabled', 'is_installed', 'num_bikes_available', 'is_returning', 'station_status', 'num_docks_available'])


# In[22]:


columns = status_live.columns


# In[23]:


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


# In[24]:


table_name = 'status_live'
cols = {
    'id': dict(),
    'metric': dict(),
    'status': dict(),
        }
create_batchview_if_not_exists(table_name, cols)
batchview = connection.table('status_live')


# In[25]:


def write_station_status_live_to_hbase(batchview, table, columns):
    
    for row in table.collect():
        for value, column in zip(row, columns):
            batchview.put(str.encode(str(row['station_id'])), {str.encode(column): str.encode(str(value))})


# In[26]:


write_station_status_live_to_hbase(batchview, status_live, columns)


# In[27]:


show_batchview_from_hbase(batchview)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




