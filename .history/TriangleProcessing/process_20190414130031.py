import subprocess
import getopt
import sys
from datetime import date, timedelta
import psycopg2
import json
import shutil
import time
import os
settings = {}
racknum = ''
to_days = 1
settings_path = os.path.join(os.path.dirname(__file__), "..", "settings.json")
temp_images_path = "/temp_images_processing/s3"
process_rackimages_path = os.path.join(os.path.dirname(__file__), "processrackimages.py")


def main(argv):
    global racks, to_days, racknum
    # set default date to yesterday
    yesterday = date.today() - timedelta(1)
    subjectDate = yesterday.strftime('%Y-%m-%d')
    dateParts = subjectDate.split("-")
    subjectYear = dateParts[0]
    subjectMonth = dateParts[1]
    subjectDay = dateParts[2]
    # parse commandline parameters
    try:
        opts, args = getopt.getopt(argv, "d:r:t:")
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
            if len(dateParts) != 3 or len(subjectYear) != 4 or len(subjectMonth) != 2 or len(subjectDay) != 2:
                print("Invalid date")
                usage()
                sys.exit(2)
        elif opt == '-r':
            racknum = arg
        elif opt == '-t':
            to_days = int(arg)

    with open(settings_path) as jsonData:
      settings = json.load(jsonData)
      jsonData.close()

    hostname = settings['postgres']['hostname']
    username = settings['postgres']['username']
    password = settings['postgres']['password']
    database = settings['postgres']['database']
    querry = ''''''
    if racknum != '':
        querry = "SELECT racknum FROM racks where racknum = '{0}' ORDER BY racknum ASC".format(racknum)
    else:
        querry = '''SELECT racknum FROM racks ORDER BY racknum ASC'''
    print(querry)
    conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
    cursor = conn.cursor()
    cursor.execute(querry)
    if cursor.rowcount > 0:
        racks = cursor.fetchall()

    for rack in racks:
        i = 1
        found_rack = rack[0]
        selected_date = date(int(subjectYear), int(subjectMonth), int(subjectDay))
        print("Begining Date: "+selected_date.isoformat())
        while i <= to_days:
            formated_date = selected_date.strftime('%Y-%m-%d')
            command = '''python {0} -r {1} -d {2}'''.format(process_rackimages_path, found_rack, formated_date)
            print('\n' + 'Processing Rack ' + found_rack + ' For Date:'+selected_date.isoformat())
            print('Days:'+str(i))
            try:
                process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                output, err = process.communicate()
                print(output)
                print(err)
                process.wait(1)
            except subprocess.TimeoutExpired:
                print('Timed out')
                pass

            selected_date = selected_date+timedelta(days=1)
            i += 1

    cursor.close()
    conn.close()

def usage():
    print("python process.py -d YYYY-MM-DD")
    print("python process.py -d YYYY-MM-DD -r racknum")
    print("python process.py -d YYYY-MM-DD -r 000003 -days 6")
    print("  -d YYYY-MM-DD : UTC date ex: 2019-01-01")

def checkFileExists(file):
    return os.path.exists(file)

# call main function
if __name__ == "__main__":
    # clean up tmp files
    if checkFileExists(temp_images_path):
        print('Removing Old Images')
        shutil.rmtree(temp_images_path)
        time.sleep(5)
    main(sys.argv[1:])

