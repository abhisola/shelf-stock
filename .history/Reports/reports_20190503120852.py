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