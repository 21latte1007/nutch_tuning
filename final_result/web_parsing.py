from bs4 import BeautifulSoup
from google.cloud import storage
from selenium import webdriver
import datetime, os, time, json, configparser

### tmp config
# TAG_LIST = [('part', 'topic', 'h2', 'class', 'screen_out', 2), ('one', 'title', 'meta', 'property', 'og:title'), ('one', 'regDate', 'meta', 'property', 'og:regDate'), ('one', 'author', 'meta', 'property', 'og:article:author'), ('one', 'url', 'meta', 'property', 'og:url'), ('one', 'image', 'meta', 'property', 'og:image'), ('all', 'content', 'p', 'dmcf-ptype', 'general')]

# TIMESTAMP = 1

# DYNAMIC_TAG = 1
# DYNAMIC_URL_TAG = ['meta', 'property', 'og:url']
# DYNAMIC_PARSE_TAG = [('YES', 'all', 'div', 'class', 'jsx-2157231875 🎬_selection_label'), ('YES', 'all', 'span', 'class', 'jsx-2157231875 🎬_count_label'), ('YES', 'all', 'a', 'class', 'btn_ttalk')]
# DYNAMIC_CONVERT_JSON = [(''), (''), ('comment count')]

# KEY_PATH = './key.json'
# BUCKET = ['news_emotion', 'news_json']
# TEMP_SAVE_FOLDER_PATH = ['./html', './json']
# TEMP_SAVE_FILE_TYPE = ['.html', '.json']
# TEMP_SAVE_FILE_NAME_RULE = ['/v/', 'title']
### tmp config

### read properties
config = configparser.ConfigParser()
config.read('config.properties')

TAG_LIST_STR = config.get('TAG', 'TAG_LIST')
TAG_LIST = [i.split(',') for i in TAG_LIST_STR.split(', ')]

TIMESTAMP = config.get('TIMESTAMP', 'TIMESTAMP')

DYNAMIC_TAG = config.get('DYNAMIC', 'DYNAMIC_TAG')
DYNAMIC_URL_TAG_STR = config.get('DYNAMIC', 'DYNAMIC_URL_TAG')
DYNAMIC_PARSE_TAG_STR = config.get('DYNAMIC', 'DYNAMIC_PARSE_TAG')
DYNAMIC_CONVERT_JSON_STR = config.get('DYNAMIC', 'DYNAMIC_CONVERT_JSON')
DYNAMIC_URL_TAG = [i for i in DYNAMIC_URL_TAG_STR.split(',')]
DYNAMIC_PARSE_TAG = [inner.split(',') for inner in DYNAMIC_PARSE_TAG_STR.split(', ')]
DYNAMIC_CONVERT_JSON = [i for i in DYNAMIC_CONVERT_JSON_STR.split(',')]

KEY_PATH = config.get('GOOGLE_CLOUD', 'KEY_PATH')
BUCKET_STR = config.get('GOOGLE_CLOUD', 'BUCKET')
TEMP_SAVE_FOLDER_PATH_STR = config.get('GOOGLE_CLOUD', 'TEMP_SAVE_FOLDER_PATH')
TEMP_SAVE_FILE_TYPE_STR = config.get('GOOGLE_CLOUD', 'TEMP_SAVE_FILE_TYPE')
TEMP_SAVE_FILE_NAME_RULE_STR = config.get('GOOGLE_CLOUD', 'TEMP_SAVE_FILE_NAME_RULE')
BUCKET = [i for i in BUCKET_STR.split(',')]
TEMP_SAVE_FOLDER_PATH = [i for i in TEMP_SAVE_FOLDER_PATH_STR.split(',')]
TEMP_SAVE_FILE_TYPE = [i for i in TEMP_SAVE_FILE_TYPE_STR.split(',')]
TEMP_SAVE_FILE_NAME_RULE = [i for i in TEMP_SAVE_FILE_NAME_RULE_STR.split(',')]
### read properties

storage_client = storage.Client.from_service_account_json(KEY_PATH)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH

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

def tag_filter_one(html, tag, class_type, class_name):
  result = html.find(tag, {class_type: class_name})['content']
  return result

def tag_filter_all(html, tag, class_type, class_name):
  result = ""
  contents = html.find_all(tag, attrs={class_type: class_name})
  for content in contents:
    result += content.get_text(strip=True) + '\n'
  return result

def tag_filter_part(html, tag, class_type, class_name, index):
  result = tag_filter_all(html, tag, class_type, class_name).split('\n')[int(index) - 1]
  return result

def tag_unwrap(html_line, tag):
  result = ""
  html_arr = html_arr[6:]
  for html_line in html_arr:
    process = BeautifulSoup(html_line, 'html.parser').find_all(tag)
    for p in process:
      p.unwrap()
      result += str(process)
  return result

def dynamic_parse_arr(html):
  url = tag_filter_one(html, DYNAMIC_URL_TAG[0], DYNAMIC_URL_TAG[1], DYNAMIC_URL_TAG[2])
  driver = webdriver.Firefox()
  driver.get(url)
  time.sleep(5)
  HTML = driver.page_source
  create_file_html(TEMP_SAVE_FOLDER_PATH[0] + '/{0}.html', url.split(TEMP_SAVE_FILE_NAME_RULE[0])[1], HTML)
  driver.quit()
  html = BeautifulSoup(HTML, 'html.parser')
  dynamic_tag_parse = []
  try:
    for i in range(len(DYNAMIC_PARSE_TAG)):
      if DYNAMIC_PARSE_TAG[i][1] == 'one':
        dynamic_tag_parse.append(tag_filter_one(html, DYNAMIC_PARSE_TAG[i][2], DYNAMIC_PARSE_TAG[i][3], DYNAMIC_PARSE_TAG[i][4]))
      elif DYNAMIC_PARSE_TAG[i][1] == 'all':
        dynamic_tag_parse.append(tag_filter_all(html, DYNAMIC_PARSE_TAG[i][2], DYNAMIC_PARSE_TAG[i][3], DYNAMIC_PARSE_TAG[i][4]).split('\n')[:-1])
      elif DYNAMIC_PARSE_TAG[i][1] == 'part':
        dynamic_tag_parse.append(tag_filter_part(html, DYNAMIC_PARSE_TAG[i][2], DYNAMIC_PARSE_TAG[i][3], DYNAMIC_PARSE_TAG[i][4], DYNAMIC_PARSE_TAG[i][5]).split('\n')[:-1])
    return dynamic_tag_parse
  except AttributeError:
    pass

def TODO_dynamic_data_control(dynamic_tag_parse, json_data):
  for i in range(len(dynamic_tag_parse)):
    if DYNAMIC_PARSE_TAG[i][0] == 'NO':
      for j in range(len(dynamic_tag_parse[i])):
        json_data[DYNAMIC_CONVERT_JSON[i][j]] = dynamic_tag_parse[i][j]
    elif DYNAMIC_PARSE_TAG[i][0] == 'YES':
      #### TODO ####
      emotion_total = 0
      if len(dynamic_tag_parse[0]) == 5 and len(dynamic_tag_parse[1]) == 5:
        for j in range(5):
          dynamic_tag_parse[0][j] = str(dynamic_tag_parse[0][j])
          dynamic_tag_parse[1][j] = str(dynamic_tag_parse[1][j])
          emotion_total += int(dynamic_tag_parse[1][j])
          TODO_daum_emotion_refine(dynamic_tag_parse[0], dynamic_tag_parse[1], emotion_total, json_data)
      if len(dynamic_tag_parse[2]) == 1:
        json_data[DYNAMIC_CONVERT_JSON[2][0]] = dynamic_tag_parse[2][0]
      else:
        json_data[DYNAMIC_CONVERT_JSON[2][0]] = '0'
      ##############

def TODO_daum_emotion_refine(feel, num, emotion_total, json_data):
  rateConfig = []
  rate = []
  if(len(feel) == 5 and len(num) == 5):
    for i in range(5):
      rateConfig.append(feel[i] + "_rate")
      emotion_figure = num[i]
      if(emotion_total == 0 or emotion_figure == 0):
        rate.append("0")
      else:
        rete_value = round(float(emotion_figure) / emotion_total, 5)
        if rete_value == 0:
          rate.append(str(0))
        else:
          rate.append(str(rete_value))
      json_data[feel[i]] = num[i]
      json_data[rateConfig[i]] = rate[i]
  json_data['emotion_total'] = str(emotion_total)

def create_json(key_arr, value_arr):
  data = {}
  for i in range(len(key_arr)):
    data[key_arr[i][1]] = value_arr[i]
  return data

def create_file_html(save_path, file_name, content):
  save_file_path = save_path.format(file_name)
  with open(save_file_path, 'w', encoding='utf-8') as file:
      file.write(str(content))

def create_file_json(save_path, file_name, content):
  save_file_path = save_path.format(file_name)
  with open(save_file_path, 'w', encoding='utf-8') as json_file:
      json.dump(content, json_file, ensure_ascii=False)

with open('./dump/dump', 'r') as file:
  html = file.read()
soup = BeautifulSoup(html, 'html.parser')
segments = str(soup).split("Recno::")
segments = segments[1:]

for segment in segments:
  seg = BeautifulSoup(segment, 'html.parser')
  parsing_tag = []
  for i in range(len(TAG_LIST)):
    if TAG_LIST[i][0] == 'one':
      parsing_tag.append(tag_filter_one(seg, TAG_LIST[i][2], TAG_LIST[i][3], TAG_LIST[i][4]))
    elif TAG_LIST[i][0] == 'all':
      parsing_tag.append(tag_filter_all(seg, TAG_LIST[i][2], TAG_LIST[i][3], TAG_LIST[i][4]))
    elif TAG_LIST[i][0] == 'part':
      parsing_tag.append(tag_filter_part(seg, TAG_LIST[i][2], TAG_LIST[i][3], TAG_LIST[i][4], TAG_LIST[i][5]))
  
  if len(parsing_tag) == len(TAG_LIST):
    json_data = create_json(TAG_LIST, parsing_tag)
    if DYNAMIC_TAG == 'TRUE':
      dynamic_data = dynamic_parse_arr(seg)
      TODO_dynamic_data_control(dynamic_data, json_data)
    if TIMESTAMP == 'TRUE':
      timestamp = datetime.datetime.now() + datetime.timedelta(hours = 9)
      json_data['timestamp'] = str(timestamp)
    create_file_json(TEMP_SAVE_FOLDER_PATH[1] + '/{0}.json', json_data[TEMP_SAVE_FILE_NAME_RULE[1]], json_data)

if len(BUCKET) != 0 and len(BUCKET) == len(TEMP_SAVE_FOLDER_PATH):
  for i in range(len(BUCKET)):
    folder_upload(BUCKET[i], TEMP_SAVE_FOLDER_PATH[i], TEMP_SAVE_FILE_TYPE[i])
if len(TEMP_SAVE_FOLDER_PATH) != 0:
  for i in range(len(TEMP_SAVE_FOLDER_PATH)):
    delete_all_files_in_folder(TEMP_SAVE_FOLDER_PATH[i])
