Created on Tue Oct 20 01:37:24 2020

@author: mohamed

"""
#this Script extracts the data from multipe accidents data sources, transform it, and load it to the data warehouse

#import libraries
import requests
import pandas as pd
import mysql.connector
#import pymysql
from sqlalchemy import create_engine;
import io


#1- extract data
#get the first 10 rows of the death causes dataset 
url="https://data.cityofnewyork.us/resource/jb7j-dtam.csv"
r = requests.get(url)
rContent=r.content
pd.set_option('display.max_columns', None)
deathCausesdf = pd.read_csv(io.StringIO(rContent.decode('utf-8')),nrows=10);


## insert data into time dimension
#extract time from the dataset
url="https://data.cityofnewyork.us/resource/h9gi-nx95.csv?$select=crash_date,crash_time"
r = requests.get(url)
rContent=r.content
pd.set_option('display.max_columns', None)
#add the extracted columns to a data frame.
timeDf = pd.read_csv(io.StringIO(rContent.decode('utf-8')));
#Convert to datetime data type
timeDf['crash_date']=pd.to_datetime(timeDf['crash_date'])
# Add a year column
timeDf['year'] = [d.year for d in timeDf['crash_date']]
#Add a month column
timeDf['month'] = [d.month for d in timeDf['crash_date']]
#add column for the day of the week.
timeDf['day_number_of_week'] = [d.strftime("%w") for d in timeDf['crash_date']]
# add a time id
timeDf['timeId']=timeDf['crash_date'].astype(str)+" "+timeDf['crash_time']
#drop the duplicated rows
timeDf=timeDf.drop_duplicates(subset='timeId', keep="first")
#print(timeDf)
# here we create a connection to mysql db "nycautocollision"
engine = create_engine("mysql+pymysql://{user}:{pw}@127.0.0.1/{db}".format(user="root", pw="root", db="nycautocollision"))
#this line of code create table in the databse with the name time dimension and columns in df , and insert the data from the df 
timeDf.to_sql('time_dimension', con = engine, if_exists = 'replace', chunksize = 1000);

## Loading  data to Contributing Factor Dimension
# Extracting the columns we are intersted in
url="https://data.cityofnewyork.us/resource/h9gi-nx95.csv?$select=contributing_factor_vehicle_1,contributing_factor_vehicle_2,contributing_factor_vehicle_3,contributing_factor_vehicle_4,contributing_factor_vehicle_5"
r = requests.get(url)
rContent=r.content
pd.set_option('display.max_columns', None)
#adding the data  to a dtat frame 
contributingFactorDF = pd.read_csv(io.StringIO(rContent.decode('utf-8')));
frames=[contributingFactorDF['contributing_factor_vehicle_1'],contributingFactorDF['contributing_factor_vehicle_2'],contributingFactorDF['contributing_factor_vehicle_3'],contributingFactorDF['contributing_factor_vehicle_4'],contributingFactorDF['contributing_factor_vehicle_5']]
contributingFactorDF=pd.concat(frames, names='contributing_factor', ignore_index=True)
contributingFactorDF=pd.DataFrame(contributingFactorDF,columns=['contributing_factor'])
contributingFactorDF=contributingFactorDF.drop_duplicates(subset='contributing_factor')
contributingFactorDF['contributing_factor_id']=[(i+1000) for i in contributingFactorDF.index]

#Loading the contributing factor dimension to the datawraehouse  
contributingFactorDF.to_sql('contributing_factor', con = engine, if_exists = 'replace', chunksize = 100, index=False);


#vehicle dimension

url="https://data.cityofnewyork.us/resource/bm4k-52h4.csv?$select=vehicle_id,vehicle_type, vehicle_make,vehicle_model,vehicle_year"
r = requests.get(url)
rContent=r.content
pd.set_option('display.max_columns', None)
vehicle = pd.read_csv(io.StringIO(rContent.decode('utf-8')));
vehicle=vehicle.drop_duplicates();
#vehicle['vehicle_id']=vehicle['vehicle_type'].astype(str)+" "+vehicle['vehicle_make']+vehicle['vehicle_model']+vehicle['vehicle_year'].astype(str)

vehicle.to_sql('vehicle', con = engine, if_exists = 'replace', chunksize = 1000, index=False);



#place Dimension
url="https://data.cityofnewyork.us/resource/h9gi-nx95.csv?$select=borough,zip_code,latitude,longitude "
r = requests.get(url)
rContent=r.content
pd.set_option('display.max_columns', None)
place = pd.read_csv(io.StringIO(rContent.decode('utf-8')));
place['zip_code']=place['zip_code'].fillna(1)
place['latitude']=place['latitude'].fillna(1)
place['longitude']=place['longitude'].fillna(1)

place['place_id']=place['zip_code']+place['latitude']+place['longitude']

place=place.drop_duplicates()

#place=place.set_index(pd.Index(range(0,37)))
#print(place.head(10))

place.to_sql('place', con = engine, if_exists = 'replace', chunksize = 100, index=False);







### in the next code we are going to extract all files using APIs and combine them in one dataframe through outer joins
pd.set_option('display.max_columns', None)


#Extract collision dataset using Api(we passes the coulmn names parameters in the url select=..) 
url="https://data.cityofnewyork.us/resource/h9gi-nx95.csv?$select=zip_code,collision_id,crash_date,crash_time,contributing_factor_vehicle_1,contributing_factor_vehicle_2,contributing_factor_vehicle_3,borough,latitude,longitude,number_of_persons_injured,number_of_persons_killed,number_of_pedestrians_injured,number_of_pedestrians_killed,number_of_cyclist_injured,number_of_cyclist_killed,number_of_motorist_injured,number_of_motorist_killed"
r = requests.get(url)
rContent=r.content
#add to a python dataFrame
collisionCrashes = pd.read_csv(io.StringIO(rContent.decode('utf-8')));
collisionCrashes['crash_date']=pd.to_datetime(collisionCrashes['crash_date'])
collisionCrashes['year'] = [d.year for d in collisionCrashes['crash_date']]
collisionCrashes['month'] = [d.month for d in collisionCrashes['crash_date']]
collisionCrashes['day_number_of_week'] = [d.strftime("%w") for d in collisionCrashes['crash_date']]
collisionCrashes['timeId']=collisionCrashes['crash_date'].astype(str)+" "+collisionCrashes['crash_time']


collisionCrashes['zip_code']=collisionCrashes['zip_code'].fillna(1)
collisionCrashes['latitude']=collisionCrashes['latitude'].fillna(1)
collisionCrashes['longitude']=collisionCrashes['longitude'].fillna(1)

collisionCrashes['place_id']=collisionCrashes['zip_code']+collisionCrashes['latitude']+collisionCrashes['longitude']

#Extract collision vehicles dataset using Api(we passes the coulmn names parameters in the url select=..) 

url="https://data.cityofnewyork.us/resource/bm4k-52h4.csv?$select=vehicle_id,collision_id,vehicle_type, vehicle_make,vehicle_model,vehicle_year"

r = requests.get(url)
rContent=r.content
pd.set_option('display.max_columns', None)
vehicle = pd.read_csv(io.StringIO(rContent.decode('utf-8')));


#extract data about race

#url Endpoint
url="https://data.cityofnewyork.us/resource/kku6-nxdu.csv?$select=jurisdiction_name,percent_pacific_islander,percent_hispanic_latino,percent_american_indian,percent_asian_non_hispanic,percent_white_non_hispanic,percent_black_non_hispanic,percent_other_ethnicity"

r = requests.get(url)
rContent=r.content
pd.set_option('display.max_columns', None)
race = pd.read_csv(io.StringIO(rContent.decode('utf-8')));

race['zip_code']=race['jurisdiction_name'];


crashes = pd.merge(collisionCrashes, vehicle, how='outer', on=['collision_id', 'collision_id'])

#We will join the contributing facto data frame wit =h the crashesdemographs data frame to get the contributing factor's surregate key
crashesdemographs=pd.merge(crashes, race, how='outer', on=['zip_code'])
contributingFactorDF['contributing_factor_vehicle_1']=contributingFactorDF['contributing_factor']

crashesdemographs=pd.merge(crashesdemographs, contributingFactorDF, how='left', on=['contributing_factor_vehicle_1'])
crashesdemographs['contributing_factor1_id']=crashesdemographs['contributing_factor_id']
crashesdemographs=crashesdemographs.drop(['contributing_factor_id','contributing_factor'],axis=1)

contributingFactorDF['contributing_factor_vehicle_2']=contributingFactorDF['contributing_factor']
crashesdemographs=pd.merge(crashesdemographs, contributingFactorDF, how='left', on=['contributing_factor_vehicle_2'])
crashesdemographs['contributing_factor2_id']=crashesdemographs['contributing_factor_id']
crashesdemographs=crashesdemographs.drop(['contributing_factor_id','contributing_factor','contributing_factor_vehicle_1_y'],axis=1)

contributingFactorDF['contributing_factor_vehicle_3']=contributingFactorDF['contributing_factor']
crashesdemographs=pd.merge(crashesdemographs, contributingFactorDF, how='left', on=['contributing_factor_vehicle_3'])
crashesdemographs['contributing_factor3_id']=crashesdemographs['contributing_factor_id']
print(crashesdemographs.shape)
crashesdemographs.drop_duplicates()
print(crashesdemographs.shape)

CrashInjuryFactsDf=crashesdemographs[['vehicle_id','timeId','contributing_factor1_id','contributing_factor2_id','contributing_factor3_id','place_id','number_of_persons_injured','number_of_persons_killed','number_of_pedestrians_injured','number_of_pedestrians_killed','number_of_cyclist_injured','number_of_cyclist_killed','number_of_motorist_injured','number_of_motorist_killed']]
CrashInjuryFactsDf.to_sql('crash_injury_fact', con = engine, if_exists = 'replace', chunksize = 100, index=False);
