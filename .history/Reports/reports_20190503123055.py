import requests
import psycopg2
import json
import os
settings = {}
settings_path = os.path.join(os.path.dirname(__file__), "..", "settings.json")
def main():
    with open(settings_path) as jsonData:
      settings = json.load(jsonData)
      jsonData.close()

    hostname = settings['postgres']['hostname']
    username = settings['postgres']['username']
    password = settings['postgres']['password']
    database = settings['postgres']['database']
    prefs = settings['prefs']
    conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
    cursor = conn.cursor()
    cursor.execute("SELECT racknum FROM racks where racknum='000003' ORDER BY racknum ASC")
    if cursor.rowcount > 0:
        racks = cursor.fetchall()
        for rack in racks:
            for i in range(3):
                url = settings['server']['local']+"/shelves/api/sendreport/"+i+"/"+rack[0]
                response = requests.request("GET", url)
                if response.status_code == 200:
                    print("Saved Report For :"+rack[0]+"Day->"+i)
            
    cursor.close()
    conn.close()