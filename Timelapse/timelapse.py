# ffmpeg
# https://ffmpeg.zeranoe.com/builds/
# Python Dependencies
# pip install awscli psycopg2 boto3
# aws configure
# Enter You key, secret keep everything as default, make output format to json

import sys
import getopt
import boto3
import botocore
import json
import os, sys, shutil
from os import path
import psycopg2
from datetime import date, timedelta
import subprocess
import time
settings_path = os.path.join(os.path.dirname(__file__), "..", "settings.json")
temp_images_path = "/temp_images_timelapse/s3/"

with open(settings_path) as jsonData:
    settings = json.load(jsonData)
    jsonData.close()

hostname = settings['postgres']['hostname']
username = settings['postgres']['username']
password = settings['postgres']['password']
database = settings['postgres']['database']
aws = settings['s3']['url']
fps = settings['prefs']['timelapse_fps']
HIGH_COMPRESSION = settings['prefs']['high_compression']
LOW_COMPRESSION = settings['prefs']['low_compression']
bucket_name = settings['s3']['bucket']

s3 = boto3.resource('s3')
bucket = s3.Bucket(bucket_name)
racknums = []
rack_images = []
rackNum = ''

# set default date to yesterday
yesterday = date.today() - timedelta(1)
subjectDate = yesterday.strftime('%Y-%m-%d')
dateParts = subjectDate.split("-")
subjectYear = dateParts[0]
subjectMonth = dateParts[1]
subjectDay = dateParts[2]


def getNumShelves(files):
    num = []
    for file in files:
        name = file.split('-')
        if name[0] not in num:
            num.append(name[0])
    return num

def renameImages(dir):
    dict = {}
    i = 0
    images = os.listdir(dir)
    for image in images:
        if len(image) > 1:
            name = image.split('-')
            key = name[1][0:-4]
            dict[key] = image
    for key in sorted(dict):
        try:
            os.rename(dir + dict[key], dir + str(i) + '.jpg')
        except FileExistsError:
            pass
        i += 1


def processImages(rackNum):
    print('Getting Images For Racknum '+str(rackNum))
    path = str(rackNum) + "/" + subjectYear + "/" + subjectMonth + "/" + subjectDay + "/"
    command = "aws s3 sync s3://" + bucket_name + "/" + path + " " + temp_images_path + path
    try:
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
        process.wait(10)
        print(command)
        print('Images Synced for '+str(rackNum))
    except subprocess.TimeoutExpired:
        pass
    if checkFileExists(temp_images_path+path+'videos'):
        print('Removing Folder videos')
        shutil.rmtree(temp_images_path+path+'videos')
        time.sleep(5)
    files = os.listdir(temp_images_path + path)
    shelves = getNumShelves(files)
    print('Shelves ' + str(shelves))
    dict = {}
    for shelf in shelves:
        createFolder(temp_images_path + path + 'video-' + shelf)
        print('Processing Shelf ' + shelf)
        shelf_array = []
        for file in files:
            if "jpg" in file:
                name = file.split('-')
                if name[0] == shelf:
                    shelf_array.append(file)
                    shutil.move(temp_images_path + path + file, temp_images_path + path + 'video-' + shelf + '/' + file)

        renameImages(temp_images_path + path + 'video-' + shelf + '/')
        print("Shelf Images ")
        print(str(shelf_array))
        dict[shelf] = shelf_array
    videoImageFolders = os.listdir(temp_images_path + path)
    print('Found Folders ' + str(videoImageFolders))
    for folder in videoImageFolders:
        if os.path.isdir(temp_images_path + path + folder):
            videoName = 'Timelapse-' + folder.split('-')[1]
            image_path = temp_images_path + path + folder + '/' + '%01d.jpg'
            output_path = temp_images_path + path + folder + '/' + videoName + '-Raw.mp4'
            output_compressed_path = temp_images_path + path + folder + '/' + videoName + '.mp4'
            makeVideo = 'ffmpeg -r {0} -f image2 -start_number 0 -i {1} -codec:v prores -profile:v 2 {2}'.format(
                fps, image_path, output_path)
            compressVideo = 'ffmpeg -i {0} -c:v libx264 -preset slow -crf {1} -c:a copy -pix_fmt yuv420p {2}'.format(output_path, HIGH_COMPRESSION, output_compressed_path)
            print('Making Video ')
            print(makeVideo)
            process = subprocess.Popen(makeVideo, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            stdout, stderr = process.communicate()
            #print('Output- ')
            #print(output)
            #print('Error- ')
            #print(err)
            print(compressVideo)
            compress_process = subprocess.Popen(compressVideo, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            stdout, stderr = compress_process.communicate()
            #print('Compress_Output- ')
            #print(stdout)
            #print('Compress_Error- ')
            #print(stderr)
            s3_video_path = path + 'videos/' + videoName + '.mp4'
            uploadVideo(output_compressed_path, s3_video_path)
            data = {}
            data['racknum'] = rackNum
            data['url'] = aws + s3_video_path
            data['date_recorded'] = subjectDate
            data['shelf'] = folder.split('-')[1]
            saveToDatabase(data)
    print('Done Creating, Saving, Uploading')



def uploadVideo(local_path, s3_path):
    print('Uploading Video To S3')
    print('Uploading From {0} To {1}'.format(local_path, s3_path))
    if checkFileExists(local_path):
        if checkBucketExists():
            s3.Object(bucket_name, s3_path).put(Body=open(local_path, 'rb'))
        else :
            print('Bucket {0} doesnot exists'.format(bucket_name))
    else :
        print('File Doesnot Exists : '+local_path)


def saveToDatabase(data):
    conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
    cursor = conn.cursor()
    print('Checking If Already Saved')
    check_exist = "SELECT * from timelapse where racknum='{0}' AND shelf='{1}' AND date_recorded='{2}'".format(
                                                                                               data['racknum'],
                                                                                               data['shelf'],
                                                                                               data['date_recorded'])
    cursor.execute(check_exist)
    if cursor.rowcount <= 0:
        print('Saving To Database: ')
        print(data.items())
        # setup DB
        query = "INSERT INTO timelapse (racknum, url, shelf, date_recorded) VALUES('{0}', '{1}', '{2}', '{3}')".format(
                                                                                                data['racknum'],
                                                                                                data['url'],
                                                                                                data['shelf'],
                                                                                                data['date_recorded'])
    else:
        query = "UPDATE timelapse SET url='{0}' WHERE racknum='{1}' AND shelf='{2}' AND date_recorded='{3}'".format(
                                                                                                data['url'],
                                                                                                data['racknum'],
                                                                                                data['shelf'],
                                                                                                data['date_recorded']
        )
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()


def fetchRacks():
    global racknums
    conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
    cursor = conn.cursor()
    cursor.execute("SELECT racknum FROM racks ORDER BY racknum ASC")
    if cursor.rowcount > 0:
        racknums = cursor.fetchall()
    cursor.close()
    conn.close()


def checkFileExists(file):
    return path.exists(file)


def checkBucketExists():
    exists = True
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
    except botocore.exceptions.ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            exists = False
    return exists


def createFolder(directory):
    try:
        if not os.path.exists(directory):
            print('Creating Folder')
            os.makedirs(directory)
    except OSError:
        print('Error: Creating directory. ' + directory)

def removeTempFolder():
    print('Doing House Cleaning')
    shutil.rmtree(temp_images_path)
    time.sleep(5)

def emptyFolder(folder):
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            print('Removing ' + the_file)
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(e)

def usage():
    print("python processrackimages.py -d YYYY-MM-DD")
    print("  -d YYYY-MM-DD : UTC date ex: 2016-11-28")

def main(argv):
    global subjectDate
    global subjectYear
    global subjectMonth
    global subjectDay
    global rackNum
    if checkFileExists(temp_images_path):
        removeTempFolder()
    # parse commandline parameters
    try:
        opts, args = getopt.getopt(argv, "d:r:")
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
            if len(dateParts) != 3 or len(subjectYear) != 4 or len(subjectMonth) != 2 or len(subjectDay) != 2 :
                print("Invalid date")
                usage()
                sys.exit(2)
        elif opt == '-r':
            rackNum = arg

    selected_date = date(int(subjectYear), int(subjectMonth), int(subjectDay))
    if rackNum != '':
        print("Generating Time Lapse For Racknum:"+rackNum+". Date: "+selected_date.isoformat())
        processImages(rackNum)
    else:
        print("Generating Time Lapse For All Racks. Date: "+selected_date.isoformat())
        fetchRacks()
        if len(racknums) > 0:
            for racknum in racknums:
                processImages(racknum[0])


# call main function
if __name__ == "__main__":
    main(sys.argv[1:])
