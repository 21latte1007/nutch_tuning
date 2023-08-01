from google.cloud import storage, bigquery
import mysql.connector
import os, json, configparser

config = configparser.ConfigParser()
config.read('config.properties')

KEY_PATH = config.get('GOOGLE_CLOUD', 'KEY_PATH')

BUCKET_STR = config.get('GOOGLE_CLOUD', 'BUCKET')
BUCKET = [i for i in BUCKET_STR.split(',')]

BIG_QUERY_TABLE_STR = config.get('GOOGLE_CLOUD', 'BIG_QUERY_TABLE')
BIG_QUERY_TABLE = [i for i in BIG_QUERY_TABLE_STR.split(',')]

CLOUD_SQL_STR = config.get('GOOGLE_CLOUD', 'CLOUD_SQL')
CLOUD_SQL = [i for i in CLOUD_SQL_STR.split(',')]

TEMP_SAVE_FOLDER_PATH_STR = config.get('FILE_SAVE', 'TEMP_SAVE_FOLDER_PATH')
TEMP_SAVE_FILE_TYPE_STR = config.get('FILE_SAVE', 'TEMP_SAVE_FILE_TYPE')
TEMP_SAVE_FILE_NAME_RULE_STR = config.get('FILE_SAVE', 'TEMP_SAVE_FILE_NAME_RULE')
TEMP_SAVE_FOLDER_PATH = [i for i in TEMP_SAVE_FOLDER_PATH_STR.split(',')]
TEMP_SAVE_FILE_TYPE = [i for i in TEMP_SAVE_FILE_TYPE_STR.split(',')]
TEMP_SAVE_FILE_NAME_RULE = [i for i in TEMP_SAVE_FILE_NAME_RULE_STR.split(',')]

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH
storage_client = storage.Client.from_service_account_json(KEY_PATH)
bigquery_client = bigquery.Client()

def local_save(json_data, html_data):
  elements, counts = count_elements(TEMP_SAVE_FILE_TYPE)
  for element in elements:
    if element == '.json' or element == '.JSON':
      create_file_json(TEMP_SAVE_FOLDER_PATH[elements.index(element)] + '/{0}.json', json_data[TEMP_SAVE_FILE_NAME_RULE[elements.index(element)]].replace('/', '\''), json_data)
    elif element == '.html' or element == '.HTML':
      create_file_html(TEMP_SAVE_FOLDER_PATH[elements.index(element)] + '/{0}.html', json_data['url'].split(TEMP_SAVE_FILE_NAME_RULE[elements.index(element)])[1], html_data)

def upload_big_query(json_data):
  if len(BIG_QUERY_TABLE) != 0:
    for i in range(len(BIG_QUERY_TABLE)):
      bigquery_client.insert_rows_json(BIG_QUERY_TABLE[i], json_data)
  
def upload_cloud_storage():
	if len(BUCKET) != 0 and len(BUCKET) == len(TEMP_SAVE_FOLDER_PATH):
		for i in range(len(BUCKET)):
			folder_upload(BUCKET[i], TEMP_SAVE_FOLDER_PATH[i], TEMP_SAVE_FILE_TYPE[i])

def upload_cloud_sql(json_data):
  if len(CLOUD_SQL) == 5 and CLOUD_SQL[0] != '':
    conn = mysql.connector.connect(
      host=CLOUD_SQL[0],
      user=CLOUD_SQL[1],
      password=CLOUD_SQL[2],
      database=CLOUD_SQL[3],
    )
    cursor = conn.cusor()
    json_keys = list(json_data.keys())
    column_name = ', '.join(json_keys)
    column_basic = ', '.join(['%s'] * len(json_keys))
    sql = 'INSERT INTO {0} ({1}) VALUES ({2})'.format(CLOUD_SQL[4], column_name, column_basic)
    data = tuple(json_data.values())
    cursor.execute(sql, data)
    conn.commit()
    conn.close()

def record_delete():
	if len(TEMP_SAVE_FOLDER_PATH) != 0:
		for i in range(len(TEMP_SAVE_FOLDER_PATH)):
			delete_all_files_in_folder(TEMP_SAVE_FOLDER_PATH[i])

def file_upload(bucket_name, file_name):
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(file_name)
  blob.upload_from_filename(file_name)

def folder_upload(bucket_name, folder_path, file_type):
  bucket = storage_client.get_bucket(bucket_name)
  for root, dirs, files in os.walk(folder_path):
    for file_name in files:
      file_path = os.path.join(root, file_name)
      if file_path.lower().endswith(file_type):
        relative_path = os.path.relpath(file_path, folder_path)
        blob = bucket.blob(relative_path)
        blob.upload_from_filename(file_path)

def delete_all_files_in_folder(folder_path):
  for filename in os.listdir(folder_path):
    file_path = os.path.join(folder_path, filename)
    try:
      if os.path.isfile(file_path):
        os.unlink(file_path)
      # elif os.path.isdir(file_path):
      #   shutil.rmtree(file_path)
    except Exception:
      pass

def count_elements(arr):
  counted_elements = []
  count = []
  for element in arr:
    if element not in counted_elements:
      counted_elements.append(element)
      count.append(arr.count(element))
  return counted_elements, count

def create_file_html(save_path, file_name, content):
  save_file_path = save_path.format(file_name)
  with open(save_file_path, 'w', encoding='utf-8') as file:
    file.write(str(content))

def create_file_json(save_path, file_name, content):
  save_file_path = save_path.format(file_name)
  with open(save_file_path, 'w', encoding='utf-8') as json_file:
    json.dump(content, json_file, ensure_ascii=False)
