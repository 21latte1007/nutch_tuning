from bs4 import BeautifulSoup
import mysql.connector, datetime, re

with open('./dump/dump', 'r') as file:
  html = file.read()

def custom_filter(tag):
  if tag.name == 'meta':
    return tag.get('property') in ['og:title', 'og:regDate', 'og:article:author', 'og:url', 'og:image', 'og:description']
  elif tag.name == 'p':
    return tag.get('dmcf-ptype') == 'general'
  else:
    return False

conn = mysql.connector.connect(
  host=<IP>,
  user=<USERNAME>,
  password=<PASSWORD>,
  database=<DBNAME>
)
cursor = conn.cursor()

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
      
  sql = 'INSERT INTO news (title, regDate, author, url, image, description, content, timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
  
  data = (
    title,
    regDate,
    author,
    url,
    image,
    description,
    p_content
  )

  cursor.execute(sql, data)
  conn.commit()
conn.close()
