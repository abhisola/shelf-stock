import json
from datetime import date, timedelta 
import os
import getopt
import sys
import psycopg2

settings_path = os.path.join(os.path.dirname(__file__), "..", "settings.json")

with open(settings_path) as jsonData:
    settings = json.load(jsonData)
    jsonData.close()

racknum = ''
to_days = 1

hostname = settings['postgres']['hostname']
username = settings['postgres']['username']
password = settings['postgres']['password']
database = settings['postgres']['database']

def main(argv):
    global racks, to_days, racknum, settings
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
            start_date = selected_date.strftime('%Y-%m-%d %H:%M:%S')
            end_date = selected_date.strftime('%Y-%m-%d %H:%M:%S')
            print('\n' + 'Generate Data {0} Form Date: {1} To {2}'.format(found_rack, start_date, end_date))
            print('Days:'+str(i))
            

            selected_date = selected_date+timedelta(days=1)
            i += 1

    cursor.close()
    conn.close()

def usage():
    print("python summary_empty_shelves.py -d YYYY-MM-DD")
    print("python summary_empty_shelves.py -d YYYY-MM-DD -r racknum")
    print("python summary_empty_shelves.py -d YYYY-MM-DD -r 000003 -days 6")
    print("  -d YYYY-MM-DD : UTC date ex: 2019-01-01")

# call main function
if __name__ == "__main__":
    main(sys.argv[1:])