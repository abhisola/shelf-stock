import getopt
import json
import subprocess
import psycopg2
from datetime import date, timedelta
import time
import os, sys, shutil
from os import path
import numpy as np
sys.path.append('./tools')
from recognizer import TriRecognizeParams, TriRecognizer

settings_path = os.path.join(os.path.dirname(__file__), "..", "settings.json")
temp_images_path = "/temp_images_processing/s3/"
with open(settings_path) as jsonData:
  settings = json.load(jsonData)
  jsonData.close()

hostname = settings['postgres']['hostname']
username = settings['postgres']['username']
password = settings['postgres']['password']
database = settings['postgres']['database']
aws = settings['s3']['url']
rack_config = {}
# main program entry point - decode parameters, act accordingly
def main(argv):
  # set default date to yesterday
  yesterday = date.today() - timedelta(1)
  subjectDate = yesterday.strftime('%Y-%m-%d')
  dateParts = subjectDate.split("-")
  subjectYear = dateParts[0]
  subjectMonth = dateParts[1]
  subjectDay = dateParts[2]

  # parse commandline parameters
  try:
    opts, args = getopt.getopt(argv, "r:d:")
  except getopt.GetoptError:
    usage()
    sys.exit(2)

  if opts is None:
    usage()
    sys.exit(2)

  for opt, arg in opts:
    if opt == '-r':
      rackNum = arg
    elif opt == '-d':
      subjectDate = arg
      dateParts = subjectDate.split("-")
      subjectYear = dateParts[0]
      subjectMonth = dateParts[1]
      subjectDay = dateParts[2]
      if len(dateParts) != 3 or len(subjectYear) != 4 or len(subjectMonth) != 2 or len(subjectDay) != 2 :
        print("Invalid date")
        usage()
        sys.exit(2)

  if rackNum == '' or subjectDate == '':
    usage()
    sys.exit(2)

  # grab images from S3
  path = rackNum + "/" + subjectYear + "/" + subjectMonth + "/" + subjectDay
  sys.stdout.write(path + " ")
  command = "aws s3 sync s3://"+settings['s3']['bucket']+"/" + path + " "+ temp_images_path + path
  try:
      process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
      process.wait(10)
  except subprocess.TimeoutExpired:
    pass
  if checkFileExists(temp_images_path+path+'/videos'):
        print('Removing Folder videos')
        shutil.rmtree(temp_images_path+path+'/videos')
        time.sleep(5)
  # setup DB
  conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
  cursor = conn.cursor()
  cursor.execute("SELECT * from shelf_config where racknum='"+rackNum+"' Order By shelfnum ASC")

  if cursor.rowcount > 0:
    shelves = cursor.fetchall()
    for shelf_config in shelves:
        rack_config[shelf_config[2]] = shelf_config[3]

  # process each image
  sys.stdout.flush()
  triangles = list()
  last_value = dict()

  for filename in sorted(os.listdir(temp_images_path + path)):

    filenameParts = filename.split("-")
    shelf_num = filenameParts[0]
    date_recorded = subjectYear + "-" + subjectMonth + "-" + subjectDay + " " + filenameParts[1]
    dateString = date_recorded[0:-4]
    if int(shelf_num) in rack_config:
        params = fillParams(rack_config[int(shelf_num)])
    else:
        params = TriRecognizeParams()
        params._set_defaults()
    recognizer = TriRecognizer()
    img_path = '''{0}{1}/{2}'''.format(temp_images_path, path, filename)
    out_data = recognizer.processImage(img_path, params)
    print('\n'+filename)

    # lookup if the image is already been processed
    check_if_exists = '''
    SELECT * from shelf_stock 
    WHERE shelf_num='{0}' and date_recorded='{1}' and racknum='{2}' '''.format(shelf_num, dateString, rackNum)
    querry = ''''''
    parameter = out_data['Parameters']
    percent_full = round(out_data['PercentFull'], 2)
    if percent_full < 0:
        percent_full = 0
    triangle_count = out_data['TriangleCount']
    triangles_expected = parameter['TrianglesExpected']
    image_url = aws + path + "/" + filename
    if shelf_num not in last_value:
      cursor.execute(check_if_exists)
      if cursor.rowcount > 0:
        rows = cursor.fetchall()
        row = rows[0]
        querry = '''
        UPDATE shelf_stock SET percent_full={0}, triangles_found={1}, triangles_expected={2}, url='{3}'
        WHERE shid={4}'''.format(percent_full, triangle_count, triangles_expected, image_url, row[10])
      else:
        querry = '''
        INSERT INTO shelf_stock(racknum, shelf_num, percent_full, raw_output, triangles_found, triangles_expected, date_recorded, url)
        VALUES('{0}','{1}',{2},'{3}',{4},{5},'{6}','{7}')
        '''.format(rackNum, shelf_num, percent_full, "{}", triangle_count, triangles_expected, dateString, image_url)
    cursor.execute(querry)
    conn.commit()
    #break

  cursor.close()
  conn.close()

def fillParams(configdata):
    params = TriRecognizeParams()
    params._set_defaults()
    if 'bounds' in configdata.keys():
        strBounds = configdata['bounds']['value']
        params.useBounds = True
        splitBounds = strBounds.split(",")
        if len(splitBounds) != 8:
            print("Invalid boundary coordinates")
            usage()
            sys.exit(2)
        for i in range(0, 4):
            coord = [int(splitBounds[i * 2]), int(splitBounds[i * 2 + 1])]
            params.bounds.append(coord)
        params.bounds = np.array([params.bounds], np.int32)

    if 'undistort' in configdata.keys():
        strDistort = configdata['undistort']['value']
        params.useDistort = True
        splitDistort = strDistort.split(",")
        if len(splitDistort) < 4:
            print("Invalid undistort coefficients")
            usage()
            sys.exit(2)
        for i in range(0, len(splitDistort)):
            coeff = float(splitDistort[i])
            params.distortCoeffs[i, 0] = coeff
    if 'sharpen' in configdata.keys():
        params.useSharpen = bool(configdata['sharpen']['value'])
    if 'arcmin' in configdata.keys():
        params.minArcLength = int(configdata['arcmin']['value'])
    if 'arcmax' in configdata.keys():
        params.maxArcLength = int(configdata['arcmax']['value'])
    if 'areamin' in configdata.keys():
        params.minArea = int(configdata['areamin']['value'])
    if 'areamax' in configdata.keys():
        params.maxArea = int(configdata['areamax']['value'])
    if 'legmin' in configdata.keys():
        params.minLegLength = int(configdata['legmin']['value'])
    if 'legmax' in configdata.keys():
        params.maxLegLength = int(configdata['legmax']['value'])
    if 'legvar' in configdata.keys():
        params.maxLegVar = int(configdata['legvar']['value'])
    if 'legratio' in configdata.keys():
        params.legRatio = int(configdata['legratio']['value'])
    if 'heightratio' in configdata.keys():
        params.heightLegRatio = int(configdata['heightratio']['value'])
    if 'paf' in configdata.keys():
        params.polyApproxFactor = int(configdata['paf']['value'])
    if 'state' in configdata.keys():
        params.outputState = bool(configdata['state']['value'])
    if 'nothresh' in configdata.keys():
        params.useThreshold = bool(configdata['nothresh']['value'])
    if 'thresh' in configdata.keys():
        params.useStaticThreshold = True
        params.staticThreshold = int(configdata['thresh']['value'])
    if 'expected' in configdata.keys():
        print('Expected:'+str(configdata['expected']['value']))
        params.baseTriangleCount = int(configdata['expected']['value'])
    if 'equhist' in configdata.keys():
        params.equalizeHist = bool(configdata['equhist']['value'])
    if 'color' in configdata.keys():
        params.colorCorr = bool(configdata['color']['value'])
    return params


def checkFileExists(file):
    return path.exists(file)

def usage():
    print("python processrackimages.py -r 000000 -d YYYY-MM-DD")
    print("  -r 000000 : unique rack ID number")
    print("  -d YYYY-MM-DD : UTC date ex: 2016-11-28")

# call main function
if __name__ == "__main__":
    main(sys.argv[1:])

