import pandas as pd
from pandas import json_normalize
import requests
import json
import psycopg2
import psycopg2.extras
import datetime
from datetime import timedelta, date


##connection to database

#setting credentials  -> only change credentals and execute
endpoint = "earthquake.cseqq9t7vc1x.us-east-1.rds.amazonaws.com"
dbname = "earthquakedata"
dbuser = "sandro"
password = "earthquake1"

#establish the connection by defining the cursor and autocommit
conn = psycopg2.connect("host="+endpoint+" dbname="+dbname+" user="+dbuser+" password="+password)
cur = conn.cursor()
conn.set_session(autocommit=True)

#dropping table before running it again
#cur.execute('DROP TABLE quakes')

#creation of a DB-table
cur.execute("""CREATE TABLE IF NOT EXISTS quakes (
id varchar(255), 
magnitude real,
time bigint,
type varchar(255),
title varchar(255),
coord_lat real,
coord_long real,
coord_height real
);
""")



#date definitions (because API breaks down if the time range is too big)
dates = []
day_delta = datetime.timedelta(days=1)
year_day_delta = datetime.timedelta(days=365)

#Function to generate the date-range
def create_date_instances():
    """creating the date range to loop over the API-URL to automate the import"""
    year = timedelta(days=365)
    start_date = date(2010, 1, 1)
    end_date = datetime.date.today()
    for i in range(round((end_date - start_date)/year)):
        dates.append(start_date + i * year_day_delta)
    dates.append(datetime.date.today())

create_date_instances()
print(dates)

# Helper function to split the coordinates into seperate columns (will be used in the following for-loop)
def splitCoords(coords, ax):
    coords = list(coords)
    if ax == 'lat':
        return coords[0]
    elif ax == 'long':
        return coords[1]
    else:
        return coords[2]

#Loop to write all earthquakes to the PostgreSQL-DB (takes approximately 1 hour)
for i in dates:
    #defining the URL with the prepared time-frames
    url = 'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime='+str(i)+'&endtime='+str(i+year_day_delta - day_delta)+'&minmagnitude=5'
    r = requests.get(url)
    r_json = json.loads(r.text)["features"]
    df = json_normalize(r_json)
    #application of the helper function to split up coordinates
    df['coord.lat'] = df.apply(lambda row: splitCoords(row['geometry.coordinates'], 'lat'), axis=1)
    df['coord.long'] = df.apply(lambda row: splitCoords(row['geometry.coordinates'], 'long'), axis=1)
    df['coord.height'] = df.apply(lambda row: splitCoords(row['geometry.coordinates'], 'height'), axis=1)
    df.drop(["geometry.coordinates"], axis=1, inplace=True)
    print(url)         #to have sort of logs while loop is running
    #Iterating over each row
    problemquakes = []
    for index, row in df.iterrows():
        try:
            cur.execute("""INSERT INTO quakes(id, magnitude, time, type, title, coord_lat, coord_long) 
            VALUES ('{}',{},{},'{}','{}',{},{});""".format(row['id'], row['properties.mag'], row['properties.time'],
                                                           row['properties.type'], row['properties.title'],
                                                           row['coord.lat'], row['coord.long']))
        except:
            problemquakes.append(row['id'])  # Save all the earthquakes that failed to fill into the DB in this list
            print("Problem with earthquake ({}, {})".format(row['id'], row['properties.title']))


cur.close()
conn.close()