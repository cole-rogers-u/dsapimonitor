from shelve import DbfilenameShelf
import sqlite3
import schedule
import datetime
import requests
import sys
import pandas as pd

FILENAME = "pi.db"
def get_pi(url):
    try:
        pidata = requests.get(url)
    except Exception as e: 
        print(e)
        sys.exit(1)    
    return pidata.json()


def save_json(filename,pi_dat):
    '''
    filename: the sqllite filename (full path)
    pi_dat dict version of json
    '''
    con = sqlite3.connect(filename)
    row = pd.json_normalize(pi_dat)
    print(row)
    try:
        row.to_sql('pidata', con, if_exists='append')
    except sqlite3.OperationalError:
        row.to_sql('pidata', con, if_exists='replace')
    except Exception as e:
        print(e)
    con.close()
    
def job():
    print(FILENAME)
    pi_data = get_pi('https://4feaquhyai.execute-api.us-east-1.amazonaws.com/api/pi')
    save_json(FILENAME, pi_data)


schedule.every(1).minutes.until(datetime.timedelta(hours=1)).do(job)
while True:
    schedule.run_pending()