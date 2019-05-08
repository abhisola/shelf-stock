# pip install Pillow requests
# pip install --upgrade google-cloud-vision
import psycopg2
import json
import boto3
import botocore
import time
import getopt
import os, sys, shutil
from os import path
from datetime import date, timedelta
import random
import subprocess
import requests
from io import BytesIO
from PIL import Image
# Imports the Google Cloud client library
from google.cloud import automl_v1beta1
from google.cloud.automl_v1beta1.proto import service_pb2
settings_path = os.path.join(os.path.dirname(__file__), "..", "settings.json")
with open(settings_path) as jsonData:
    settings = json.load(jsonData)
    jsonData.close()

hostname = settings['postgres']['hostname']
username = settings['postgres']['username']
password = settings['postgres']['password']
database = settings['postgres']['database']
aws = settings['s3']['url']
bucket_name = settings['s3']['bucket']
start_time = settings['prefs']['start_time']
end_time = settings['prefs']['end_time']
project_id = settings['vision']['projectId']
model_id = settings['vision']['modelId']
no_of_images = settings['vision']['no_of_images_per_shelf']
save_images_locally = settings['vision']['save_images_locally']
s3 = boto3.resource('s3')
bucket = s3.Bucket(bucket_name)
temp_folder = './vision/temp_image/s3/'
products = []

# set default date to yesterday
yesterday = date.today() - timedelta(1)
subjectDate = yesterday.strftime('%Y-%m-%d')
dateParts = subjectDate.split("-")
subjectYear = dateParts[0]
subjectMonth = dateParts[1]
subjectDay = dateParts[2]
date_path = subjectYear+'/'+subjectMonth+'/'+subjectDay
racknum = ''
user_selected_racknum = ''
configs = {}
conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)


def get_unique_shelfnums(rows):
    shelves = []
    for row in rows:
        if row[0] not in shelves:
            shelves.append(row[0])
    return shelves

def sort_rows(rows):
    shelves = get_unique_shelfnums(rows)
    sorted_rows = {}
    for shelf in shelves:
        shelf_data = []
        for row in rows:
            if row[0] == shelf:
                shelf_data.append(row)
        sorted_rows[shelf] = shelf_data
        shelf_data = []
    return sorted_rows

def create_folder_structure(path):
    os.makedirs(path, exist_ok=True)

# Unused Function -> Left here for future re
def download_image(aws_image_url, local_path):
    response = requests.get(aws_image_url)
    i = Image.open(BytesIO(response.content))
    i.save(local_path)
    del response

def google_image_prediction(aws_image_url, local_path):
    prediction_client = automl_v1beta1.PredictionServiceClient()
    name = 'projects/{}/locations/us-central1/models/{}'.format(project_id, model_id)
    response = requests.get(aws_image_url)
    if save_images_locally:
        i = Image.open(BytesIO(response.content))
        i.save(local_path)
    payload = {'image': {'image_bytes': response.content }}
    del response
    params = {}
    predicted_data = prediction_client.predict(name, payload, params)
    return predicted_data


def predictImages(rows):
    global racknum
    global date_path
    global configs
    prediction_details = {}
    predicted_image = {}
    prediction_list = []
    sorted_rows = sort_rows(rows)
    for shelf in sorted_rows.keys():
        if int(shelf) in configs[racknum]:
            prediction_list = []
            print('Shelf{0} -> Allowed'.format(shelf))
            s = set(sorted_rows[shelf])
            if len(s) > no_of_images:
                random_five = random.sample(s, no_of_images)
                array_size = no_of_images
            else:
                random_five = sorted_rows[shelf]
                array_size = len(s)

            for i in range(array_size):
                predicted_image = {}
                url = random_five[i][3]
                date_recorded = random_five[i][2]
                local_folder_structure = '{0}{1}/{2}/{3}/' .format(temp_folder, racknum, date_path, shelf)
                local_image_path = '{0}{1}/{2}/{3}/image{4}.jpg'.format(temp_folder, racknum, date_path, shelf,i)
                create_folder_structure(local_folder_structure)
                prediction = google_image_prediction(url, local_image_path)
                if prediction.payload:
                    name = prediction.payload[0].display_name
                    score = prediction.payload[0].classification.score
                    print('Predicted for shelf {0} : {1} -> {2} -> {3}'.format(shelf, name, score, url))
                    predicted_image['name'] = name
                    predicted_image['score'] = score
                    predicted_image['url'] = url
                    predicted_image['date_recorded'] = date_recorded
                    prediction_list.append(predicted_image)
            prediction_details[shelf] = prediction_list
    return prediction_details


def format_configs(configDb):
    config_formated = {}
    for config in configDb:
        if config[0] not in config_formated.keys():
            config_formated[config[0]] = []
        if config[2]:
            config_formated[config[0]].append(config[1])
    return config_formated

def get_product_id(label):
    for product in products:
        if product[2] == label:
            return int(product[0])

def fetch_product_details():
    global products
    print('Fetching Products')
    cursor = conn.cursor()
    get_products = 'Select * from products'
    cursor.execute(get_products)
    if cursor.rowcount > 0:
        products = cursor.fetchall()
        print('Found {0} Products.'.format(cursor.rowcount))
    else:
        print('No Products Found')
    cursor.close()

def fetch_shelf_configs():
    global configs
    print('Fetching Shelf Configurations')
    cursor = conn.cursor()
    get_shelf_config = 'Select racknum, shelfnum::integer, allow_detection from shelf_config ORDER BY racknum ASC, shelfnum ASC'
    cursor.execute(get_shelf_config)
    if cursor.rowcount > 0:
        config_db = cursor.fetchall()
        configs = format_configs(config_db)
        print(str(len(config_db))+' Configs Found.')
    else:
        print('No Configs Found')
    cursor.close()

def prepareData():
    global products
    global racknum

    start_date = '{0}-{1}-{2} {3}'.format(subjectYear, subjectMonth, subjectDay, start_time)
    end_date = '{0}-{1}-{2} {3}'.format(subjectYear, subjectMonth, subjectDay, end_time)
    print('Preparng Data For Rack {0}, For Date {1} -> {2}'.format(racknum, start_date, end_date))
    get_image_records = '''Select shelf_num, percent_full, date_recorded::text, url from shelf_stock 
    where date_recorded >= '{0}' AND date_recorded <= '{1}' AND racknum='{2}' ORDER BY shelf_num ASC , date_recorded ASC'''.format(start_date, end_date, racknum)
    cursor = conn.cursor()
    cursor.execute(get_image_records)
    if cursor.rowcount > 0:
        rows = cursor.fetchall()
        predicted = predictImages(rows)

        for shelf in predicted.keys():
            for predicted_image in predicted[shelf]:
                predicted_product_name = get_product_id(predicted_image['name'])
                insert_detection_data = '''INSERT INTO detection_details (racknum, shelfnum, pid, detected_label, score, url, date_recorded) 
                VALUES ('{0}', {1}, {2}, '{3}', {4}, '{5}', '{6}')'''.format(racknum, int(shelf), predicted_product_name ,predicted_image['name'], round(predicted_image['score'],4), predicted_image['url'], predicted_image['date_recorded'])
                update_detection_data = '''UPDATE detection_details SET pid={0}, detected_label='{1}', score={2} 
                WHERE url='{3}' '''.format(predicted_product_name, predicted_image['name'], round(predicted_image['score'],4), predicted_image['url'])
                check_if_already_predicted = '''SELECT * FROM detection_details WHERE url='{0}' '''.format(predicted_image['url'])
                cursor.execute(check_if_already_predicted)
                if cursor.rowcount > 0:
                    cursor.execute(update_detection_data)
                    if cursor.rowcount > 0:
                        print('Updated Shelf -> '+shelf)
                else:
                    cursor.execute(insert_detection_data)
                    if cursor.rowcount > 0:
                        print('Saved Shelf -> '+shelf)
        conn.commit()
    else:
        print('No Data On Date {0}, for racknum {1}'.format(subjectDate, racknum))
    print('\n\n')
    cursor.close()

def usage():
    print("python prediction.py -d YYYY-MM-DD -r Racknum")
    print("  -d YYYY-MM-DD : UTC date ex: 2016-11-28")
    print("  -r Racknum : ex: 000005")

def main(argv):
    global subjectDate
    global subjectYear
    global subjectMonth
    global subjectDay
    global racknum
    global date_path
    global configs
    global user_selected_racknum
    cursor = conn.cursor()
    # parse commandline parameters
    try:
        opts, args = getopt.getopt(argv, "d:r:h:")
    except getopt.GetoptError:
        usage()
        sys.exit(2)

    if opts is None:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-d':
            subjectDate = arg
            dateParts = subjectDate.split("-")
            subjectYear = dateParts[0]
            subjectMonth = dateParts[1]
            subjectDay = dateParts[2]
            date_path = subjectYear+'/'+subjectMonth+'/'+subjectDay
        if len(dateParts) != 3 or len(subjectYear) != 4 or len(subjectMonth) != 2 or len(subjectDay) != 2:
            print("Invalid date")
            usage()
            sys.exit(2)
        if opt == '-r':
            user_selected_racknum = arg
        if opt == '-h':
            usage()
            sys.exit(2)

    print('Prediction Program Started')

    fetch_shelf_configs()
    fetch_product_details()

    if user_selected_racknum != '':
        print('Detection By User Choice Of Rack')
        racknum = user_selected_racknum
        prepareData()
    else:
        print('Fetching Racks With Detection Allowed')
        get_racks = 'Select * from racks where allow_detection=true ORDER BY racknum ASC'
        cursor.execute(get_racks)
        racks = []
        if cursor.rowcount > 0:
            racks = cursor.fetchall()
            print('Found {0} Alowed Racks'.format(cursor.rowcount))
        else:
            print('No Racks Are Alowed For Detection')
        for rack in racks:
            racknum = rack[3]
            prepareData()

    cursor.close()
    conn.close()

def checkFileExists(file):
    return path.exists(file)

# call main function
if __name__ == "__main__":
    main(sys.argv[1:])
