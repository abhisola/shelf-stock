import json
from datetime import date, timedelta 


settings_path = os.path.join(os.path.dirname(__file__), "..", "settings.json")

with open(settings_path) as jsonData:
    settings = json.load(jsonData)
    jsonData.close()

hostname = settings['postgres']['hostname']
username = settings['postgres']['username']
password = settings['postgres']['password']
database = settings['postgres']['database']

yesterday = date.today()  - timedelta(1)
day_of_week = yesterday.weekday()
start_of_week = yesterday - timedelta(day_of_week)
week_no = yesterday.

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

def usage():
    print("python summary_empty_shelves.py -d YYYY-MM-DD")
    print("python summary_empty_shelves.py -d YYYY-MM-DD -r racknum")
    print("python summary_empty_shelves.py -d YYYY-MM-DD -r 000003 -days 6")
    print("  -d YYYY-MM-DD : UTC date ex: 2019-01-01")

# call main function
if __name__ == "__main__":
    main(sys.argv[1:])