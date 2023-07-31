from bs4 import BeautifulSoup
from selenium import webdriver
import datetime, time, configparser

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

def tag_list_parse(seg):
  parsing_tag = []
  for i in range(len(TAG_LIST)):
    if TAG_LIST[i][0] == 'one':
      parsing_tag.append(tag_filter_one(seg, TAG_LIST[i][2], TAG_LIST[i][3], TAG_LIST[i][4]))
    elif TAG_LIST[i][0] == 'all':
      parsing_tag.append(tag_filter_all(seg, TAG_LIST[i][2], TAG_LIST[i][3], TAG_LIST[i][4]))
    elif TAG_LIST[i][0] == 'part':
      parsing_tag.append(tag_filter_part(seg, TAG_LIST[i][2], TAG_LIST[i][3], TAG_LIST[i][4], TAG_LIST[i][5]))
  return parsing_tag

def output_data(parsing_tag, seg):
  #### TODO ####
  if len(parsing_tag) == len(TAG_LIST):
    json_data = {TAG_LIST[i][1]: parsing_tag[i] for i in range(len(TAG_LIST))}
    if DYNAMIC_TAG == 'TRUE':
      dynamic_data, seg = dynamic_parse_arr(seg)
      TODO_dynamic_data_control(dynamic_data, json_data)
    if TIMESTAMP == 'TRUE':
      timestamp = datetime.datetime.now() + datetime.timedelta(hours = 9)
      json_data['timestamp'] = str(timestamp)
    return json_data, seg

def dynamic_parse_arr(html):
  url = tag_filter_one(html, DYNAMIC_URL_TAG[0], DYNAMIC_URL_TAG[1], DYNAMIC_URL_TAG[2])
  driver = webdriver.Firefox()
  driver.get(url)
  time.sleep(5)
  HTML = driver.page_source
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
    return dynamic_tag_parse, HTML
  except AttributeError:
    pass

def TODO_dynamic_data_control(dynamic_tag_parse, json_data):
  print(dynamic_tag_parse)
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
        json_data[DYNAMIC_CONVERT_JSON[2]] = dynamic_tag_parse[2][0].split('톡')[1].split('개')[0]
      else:
        json_data[DYNAMIC_CONVERT_JSON[2]] = '0'
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
