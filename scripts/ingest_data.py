"""
Week 1 - Coding Assignment
Specification : To ingest data in DuckDB Database
Author : Antara Saha
"""

# Import Libraries
import pandas as pd
import os
import duckdb

conn = duckdb.connect(database='main_DB_95797') # create a database

# Load all the csv files:
# 1. Loading central park weather data
filename = 'Dataset/central_park_weather.csv'
tmp_data = pd.read_csv(filename)

print("Loading File: central_park_weather.csv")
conn.execute("""
    CREATE TABLE central_park_weather AS SELECT * FROM tmp_data;
    """
)

# 2. Loading daily_citi_bike_trip_counts_and_weather     
filename = 'Dataset/daily_citi_bike_trip_counts_and_weather.csv'
tmp_data = pd.read_csv(filename)

print("Loading File: daily_citi_bike_trip_counts_and_weather.csv")
conn.execute("""
    CREATE TABLE daily_citi_bike_trip_counts_and_weather AS SELECT * FROM tmp_data;
""")

# 3. Loading fhv_bases
filename = 'Dataset/fhv_bases.csv'
tmp_data = pd.read_csv(filename)

print("Loading File: fhv_bases.csv")
conn.execute("""
    CREATE TABLE fhv_bases AS SELECT * FROM tmp_data;
    """
)

# 4. Loading Bike Data
# Get file path
folder_path = 'Dataset/bike'
file_list = os.listdir(folder_path)

# Create bike_data table in DuckDB
conn.execute("""
CREATE TABLE bike_data(
tripduration VARCHAR,starttime VARCHAR,stoptime VARCHAR,"start station id" VARCHAR,"start station name" VARCHAR,"start station latitude" VARCHAR,
"start station longitude" VARCHAR,"end station id" VARCHAR,"end station name" VARCHAR,"end station latitude" VARCHAR,"end station longitude" VARCHAR,
bikeid VARCHAR,usertype VARCHAR,"birth year" VARCHAR,gender VARCHAR,ride_id VARCHAR,rideable_type VARCHAR,started_at VARCHAR,ended_at VARCHAR,
start_station_name VARCHAR,start_station_id VARCHAR,end_station_name VARCHAR,end_station_id VARCHAR,start_lat VARCHAR,start_lng VARCHAR,end_lat VARCHAR,
end_lng VARCHAR,member_casual VARCHAR,filename VARCHAR);
""")

# Column name list
columns_lst =["tripduration","starttime","stoptime","start station id","start station name",
"start station latitude","start station longitude","end station id","end station name",
"end station latitude","end station longitude","bikeid","usertype","birth year","gender","ride_id",
"rideable_type","started_at","ended_at","start_station_name","start_station_id","end_station_name","end_station_id",
"start_lat","start_lng","end_lat","end_lng","member_casual","filename"]

# Insert rows from csv.gz file to bike_data table
for file_name in file_list:
    if file_name.endswith('.csv.gz'):
        file_path = os.path.join(folder_path, file_name)
        print("Loading File: ",file_path)
        tmp_data = pd.read_csv(file_path,compression='gzip',low_memory=False)
        tmp_data["filename"] = file_name[:-7]
        tmp_data = tmp_data.reindex(columns=columns_lst)
        print("Inserting rows")
        conn.execute("""
            INSERT INTO bike_data SELECT * FROM tmp_data;
            """)
     
# 5. Loading fhv_tripdata
print("Loading Parquet Files: fhv_tripdata")
conn.execute("""
    CREATE TABLE fhv_tripdata AS SELECT * FROM 'Dataset/taxi/fhv_tripdata_*.parquet';
    """
)

# 6. Loading fhvhv_tripdata
print("Loading Parquet Files: fhvhv_tripdata")
conn.execute("""
    CREATE TABLE fhvhv_tripdata AS SELECT * FROM 'Dataset/taxi/fhvhv_tripdata_*.parquet';
    """
)

# 7. Loading green_tripdata
print("Loading Parquet Files: green_tripdata")
conn.execute("""
    CREATE TABLE green_tripdata AS SELECT * FROM 'Dataset/taxi/green_tripdata_*.parquet';
    """
)

# 8. Loading yellow_tripdata
print("Loading Parquet Files: yellow_tripdata")
conn.execute("""
    CREATE TABLE yellow_tripdata AS SELECT * FROM 'Dataset/taxi/yellow_tripdata_*.parquet';
    """
)

# End Script