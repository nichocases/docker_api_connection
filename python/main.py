import requests as rqs
import psycopg2 as psql
import pandas as pd
from datetime import datetime
import time

'''
connection with DataBase in order to insert values or query the table
'''
def connection_data():
    try:
        connection=psql.connect(
            database="POLICEDB",
            user='docker',
            password='docker',
            host='127.0.0.1'
        )
    except Exception as e:
        print(e)
    return connection


'''
function in order to create the string with all the locations for the request for the API
'''
def get_locations(path):
    get_locations=pd.read_csv(path)
    list_locations=['lat='+str(x)+'&lng='+str(y) for x,y in zip(get_locations['latitude'],get_locations['longitude']) ]
    return list_locations


'''
Function that create the full URL to send the request
'''
def complete_url(partial_url):
    url='https://data.police.uk/api/crimes-street/all-crime?'
    full_url=[url+x for x in partial_url]
    return full_url


'''
Functions that execute the requests to the API
'''
def collections_apis(urls):    
    for i in urls:
        res=rqs.get(i).json()
        quantity=len(res)
        print(quantity)
        print(type(res))
        street_table(res,quantity)
        crime_table(res,quantity)        
        locations_table(res,quantity)


'''
Function that use the response from the API and then will be send to the table crime of the DataBase
'''
def crime_table(response,quantity):
    list_fields=['id','persistent_id','category','location_type','context','location_subtype','month']
    c=connection_data()
    cur=c.cursor()
    for i in range(quantity):
        part_a=[response[i].get(x) for x in list_fields]
        part_b=[response[i].get('location').get('street').get('id')]
        crime=part_a+part_b
        crime[6]=crime[6]+'-01'
        
        query="INSERT INTO crime (id,persistent_id,category,location_type,context,location_subtype,month_crime,street_id) values (%s,%s,%s,%s,%s,%s,DATE %s,%s ) ON CONFLICT DO NOTHING"""
        query1="INSERT INTO crime (id,persistent_id,category,location_type,context,location_subtype,month_crime,street_id) values ({},'{}','{}','{}','{}','{}',DATE '{}',{}) ON CONFLICT DO NOTHING"""
            
        try:
            cur.execute(query,crime)
        except Exception as err:
            print(err)
        c.commit()
    
        print(query1.format(*crime))
    cur.close()
    c.close()

'''
Function that use the response from the API and then will be send to the table street of the DataBase
'''
def street_table(response,quantity):
    list_fields=['id','name']    
    c=connection_data()
    cur=c.cursor()
    for i in range(quantity):
        street=[response[i].get('location').get('street').get(x) for x in list_fields]
        street[1]=''.join(["''" if x == "'"  else x for x in street[1]])
        query="INSERT INTO street (id,name) values (%s,%s) ON CONFLICT DO NOTHING RETURNING id;"
        query1="INSERT INTO street (id,name) values ({},'{}') ON CONFLICT DO NOTHING"""
        
        #b.append(street[1])
        try:
            cur.execute(query,street)
        except Exception as err:
            print(err)
        c.commit()
        
        print(query1.format(*street))        
        
    cur.close()
    c.close()

'''
Function that use the response from the API and then will be send to the table locations of the DataBase
'''
def locations_table(response,quantity):
    c=connection_data()
    cur=c.cursor()
    list_fields=['latitude','longitude']
    for i in range(quantity):
        locations=[response[i].get('location').get(x) for x in list_fields]
        part_a=[response[i].get('location').get('street').get('id')]
        part_b=[response[i].get('id')]
        locations=locations+part_a+part_b
        query="INSERT INTO locations (latitude,longitude,street_id,crime_id) values (%s,%s,%s,%s) ON CONFLICT DO NOTHING"""
        query1="INSERT INTO locations (latitude,longitude,street_id,crime_id) values ({},{},{},{}) ON CONFLICT DO NOTHING"""
        
        try:
            cur.execute(query,locations)
        except Exception as err:
            print(err)
        c.commit()
        
        
        print(query1.format(*locations))
    cur.close()
    c.close()
    
'''
Function that use the response from the API and then will be send to the table outcome of the DataBase
'''
def outcome_table(response,quantity):
    c=connection_data()
    cur=c.cursor()
    list_fields=['category','date']
    
    for i in range(quantity):
        if response[i].get('outcome_status') is None:
            pass
        else:
            outcome=[response[i].get('outcome_status').get(x) for x in list_fields]        
            part_a=[response[i].get('id')]
            outcome=outcome+part_a
            outcome[1]=outcome[1]+'-01'
            query="INSERT INTO outcome_status (category,MONTH_CRIME,crime_id) values (%s,DATE %s,%s) ON CONFLICT DO NOTHING"""
            query1="INSERT INTO outcome_status (category,MONTH_CRIME,crime_id) values ({},{},{}) ON CONFLICT DO NOTHING"""

            try:
                cur.execute(query,outcome)
            except Exception as err:
                print(err)
            c.commit()


            print(query1.format(*outcome))
    cur.close()
    c.close()
        

if __name__=='__main__':
    path_locations='C:/Users/Nicholas_Castaneda/Documents/Police/python_api/resources/LondonStations.csv'
    collections_apis(complete_url(get_locations(path_locations)))
    