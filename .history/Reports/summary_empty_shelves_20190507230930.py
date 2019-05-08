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

conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)

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
    
    cursor = conn.cursor()
    cursor.execute(querry)
    if cursor.rowcount > 0:
        racks = cursor.fetchall()

    for rack in racks:
        i = 1
        found_rack = '000003' #rack[0]
        found_store = ''#rack[10]
        selected_date = date(int(subjectYear), int(subjectMonth), int(subjectDay))
        print("Begining Date: "+selected_date.isoformat())
        while i <= to_days:
            start_date = selected_date.strftime('%Y-%m-%d')+' 00:00:00'
            end_date = selected_date.strftime('%Y-%m-%d')+' 23:59:59'
            print('\n' + 'Generate Data {0} Form Date: {1} To {2}'.format(found_rack, start_date, end_date))
            print('Days:'+str(i))
            
            generateData(start_date, end_date, found_rack, found_store)

            selected_date = selected_date+timedelta(days=1)
            i += 1
        break

    cursor.close()
    conn.close()

def formatData(row):
    
    return []

def generateData(start, end, racknum, storenum):
    get_stock_details = '''Select shelf_num, percent_full, date_recorded::text from shelf_stock 
    WHERE racknum='{0}' AND date_recorded >= '{1}' AND date_recorded <= '{2}' ORDER BY shelf_num'''.format(racknum, start, end)
    
    check_if_exists_100 = ''' Select * from report_summary_100 where racknum='{0}' AND shelfnum={1} AND date_recorded={3} '''
    check_if_exists_50 = ''' Select * from report_summary_50 where racknum='{0}' AND shelfnum={1} AND date_recorded={3} '''

    insert_summary_100 = ''' Insert into report_summary_100(storenum, racknum, shelfnum, date_recorded, emptyness) 
    Values('{0}','{1}',{2},'{3}',{4})'''
    insert_summary_50 = ''' Insert into report_summary_50(storenum, racknum, shelfnum, date_recorded, emptyness) 
    Values('{0}','{1}',{2},'{3}',{4})'''

    update_summary_100 = ''' Update report_summary_100 SET(emptyness='{0}') Where racknum='{1}' AND shelfnum={2} AND date_recorded={3}'''
    update_summary_50 = ''' Update report_summary_50 SET(emptyness='{0}') Where racknum='{1}' AND shelfnum={2} AND date_recorded={3}'''

    cursor = conn.cursor()
    cursor.execute(get_stock_details)
    if cursor.rowcount > 0:
        stock_details = cursor.fetchall()
        emptyness_100 = []
        emptyness_50 = []
        for stock in stock_details:
            emptyness = (stock[1]*100) - 100
            shelfnum = stock[0]
            date_recorded = stock[2]
            if emptyness >= 96:
                emptyness_100.append([storenum, racknum, shelfnum , date_recorded, emptyness])
            elif emptyness >= 50 and emptyness <=95:
                emptyness_50.append([storenum, racknum, shelfnum , date_recorded, emptyness])

        for row in emptyness_100:
            cursor.execute(check_if_exists_100.format(racknum, stock))
        



    
    cursor.close()

def usage():
    print("python summary_empty_shelves.py -d YYYY-MM-DD")
    print("python summary_empty_shelves.py -d YYYY-MM-DD -r racknum")
    print("python summary_empty_shelves.py -d YYYY-MM-DD -r 000003 -days 6")
    print("  -d YYYY-MM-DD : UTC date ex: 2019-01-01")

# call main function
if __name__ == "__main__":
    main(sys.argv[1:])