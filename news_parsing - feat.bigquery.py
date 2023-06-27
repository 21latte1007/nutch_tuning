from bs4 import BeautifulSoup
from google.cloud import bigquery
import datetime, re, os

with open('./dump/dump', 'r') as file:
  html = file.read()

def custom_filter(tag):
  if tag.name == 'meta':
    return tag.get('property') in ['og:title', 'og:regDate', 'og:article:author', 'og:url', 'og:image', 'og:description']
  elif tag.name == 'p':
    return tag.get('dmcf-ptype') == 'general'
  else:
    return False

soup = BeautifulSoup(html, 'html.parser')
segments = str(soup).split("Recno::")
segments = segments[1:]

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./key.json"
client = bigquery.Client()
table_id = <<BQ_TABLE_ID>>

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

  with open(output_file, 'w') as file:
    file.write(empty)

  with open(output_file, 'r') as file:
    html = file.read()

  soup = BeautifulSoup(html, 'html.parser')

  arr = str(soup).splitlines()

  title = arr[0]
  regDate = arr[1]
  author = arr[2]
  url = arr[3]
  image = arr[4]
  if len(arr) >= 6:
    description = arr[5]

  content = ""
  timestamp = datetime.datetime.now() + datetime.timedelta(hours = 9)

  if len(arr) >= 7:
    arr = arr[6:]
    for a in arr:
      end = BeautifulSoup(a, 'html.parser')
      process = end.find_all('span')
      for p in process:
        p.unwrap()
      process = end.find_all('strong')
      for p in process:
        p.unwrap()
      content += str(end)
      
  data = (
    'title': title,
    'regDate': regDate,
    'author': author,
    'url': url,
    'image': image,
    'description': description,
    'content': content,
    'timestamp': str(timestamp)
  )

  client.insert_rows_json(table_id, data)
