from bs4 import BeautifulSoup
from google.cloud import bigquery
from configparser import ConfigParser, NoOptionError
import mysql.connector, datetime, re, os

config = ConfigParser()
config.read('config.properties')

try:
  DB_HOST = config.get('db_info', 'db.host')
  DB_USER = config.get('db_info', 'db.user')
  DB_PASSWORD = config.get('db_info', 'db.password')
  DB_NAME = config.get('db_info', 'db.name')
  DB_TABLE = config.get('db_info', 'db.table')
except NoOptionError:
  DB_HOST = ''
  DB_USER = ''
  DB_PASSWORD = ''
  DB_NAME = ''

try:
  BQ_TABLE = config.get('big_query', 'table.name')
  BQ_KEY = config.get('big_query', 'privateKey')
except NoOptionError:
  BQ_TABLE = ''
  BQ_KEY = ''

TAG_NAME = config.get('tag_info', 'tag.name').split(', ')
TAG_INNER = config.get('tag_info', 'tag.inner').split(', ')
TAG_CONTENT = config.get('tag_info', 'tag.content').split(', ')
TAG_COLUMN = config.get('tag_info', 'tag.column').split(', ')
TIMESTAMP = config.get('timestamp', 'timestamp')

LEN = len(TAG_COLUMN)

with open('./dump/dump', 'r') as file:
    html = file.read()

def custom_filter(tag):
    for i in range(len(TAG_NAME)):
        if tag.name == TAG_NAME[i]:
            return tag.get(TAG_INNER[i]) in TAG_CONTENT[i].split(',')
    return False

soup = BeautifulSoup(html, 'html.parser')
segments = str(soup).split("Recno::")
segments = segments[1:]
    
for segment in segments:
    seg = BeautifulSoup(segment, 'html.parser')
    tags = seg.find_all(custom_filter)
    contents = [str(tag) + '\n' for tag in tags]

    output_file = './result.html'
    with open(output_file, 'w') as file:
        file.write('\n'.join(contents))

    with open(output_file, 'r') as file:
        html = file.read()

    empty = html.strip()
    soup = BeautifulSoup(empty, 'html.parser')
    
    arr = str(soup).splitlines()
    result = [''] * LEN
    
    if LEN <= len(arr):
        for i in range(LEN):
            result[i - 1] = arr[i - 1]
        ####
        arr = arr[LEN - 1:]
        result[LEN - 1] = ""
        for a in arr:
            end = BeautifulSoup(a, 'html.parser')
            process = end.find_all('span') + end.find_all('strong')
            for p in process:
                p.unwrap()
            result[LEN - 1] += str(end)
        ####
    else:
        continue
    
    timestamp = datetime.datetime.now() + datetime.timedelta(hours = 9)
    
    if DB_HOST != '':
        ALL_COLUMNS = ', '.join(TAG_COLUMN)
        INSERT = ', '.join(['%s'] * len(TAG_COLUMN))
        sql = f'INSERT INTO {DB_TABLE} ({ALL_COLUMNS}) VALUES ({INSERT})'
        
        data = ()
        for i in range(0, LEN):
            data += (result[i],)
        
        if TIMESTAMP == 'True':
            ALL_COLUMNS += ', timestamp'
            INSERT += ', %s'
            sql = f'INSERT INTO {DB_TABLE} ({ALL_COLUMNS}) VALUES ({INSERT})'
            data += (timestamp,)
            
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor()
        cursor.execute(sql, data)
        conn.commit()
        conn.close()
    
    if BQ_KEY != '':
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BQ_KEY
        client = bigquery.Client()
        table_id = BQ_TABLE
        
        data = [{tag: res for tag, res in zip(TAG_COLUMN[:LEN], result[:LEN])}]
        if TIMESTAMP == 'True':
            data[0]['timestamp'] = str(timestamp)
        
        client.insert_rows_json(table_id, data)
