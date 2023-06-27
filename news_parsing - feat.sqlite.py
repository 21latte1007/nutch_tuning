from bs4 import BeautifulSoup
import sqlite3

with open('./dump/dump', 'r') as file:
  html = file.read()

soup = BeautifulSoup(html, 'html.parser')

def custom_filter(tag):
  if tag.name == 'meta':
    return tag.get('property') in ['og:title', 'og:regDate', 'og:article:author', 'og:url', 'og:image', 'og:description']
  elif tag.name == 'p':
    return tag.get('dmcf-ptype') == 'general'
  else:
    return False

tags = soup.find_all(custom_filter)
contents = [str(tag) + '\n' for tag in tags]

output_file = './result.html'
with open(output_file, 'w') as file:
  file.write('\n'.join(contents))

with open(output_file, 'r') as file:
  html = file.read()

soup = BeautifulSoup(html, 'html.parser')

for tag in soup.find_all('p', attrs={'dmcf-pid': True}):
  del tag['dmcf-pid']
modified_html = str(soup).replace('</p>\n<p dmcf-ptype="general">', '\n')

segments = modified_html.split('</p>')

for segment in segments:
  empty = segment.strip()

  with open(output_file, 'w') as file:
    file.write(empty)

  with open(output_file, 'r') as file:
    html = file.read()

  soup = BeautifulSoup(html, 'html.parser')

  conn = sqlite3.connect('./test/db')
  cursor = conn.cursor()
  cursor.execute('''
      CREATE TABLE IF NOT EXISTS news(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        regDate TEXT,
        author TEXT,
        url TEXT,
        image TEXT,
        description TEXT,
        content TEXT
      )
  ''')

  title_tag = soup.find('meta', {'property': 'og:title'})
  date_tag = soup.find('meta', {'property': 'og:regDate'})
  author_tag = soup.find('meta', {'property': 'og:article:author'})
  url_tag = soup.find('meta', {'property': 'og:url'})
  image_tag = soup.find('meta', {'property': 'og:image'})
  description_tag = soup.find('meta', {'property': 'og:description'})
  p_tag = soup.find('p', {'dmcf-ptype': 'general'})

  title = title_tag['content'] if title_tag else ''
  regDate = date_tag['content'] if date_tag else ''
  author = author_tag['content'] if author_tag else ''
  url = url_tag['content'] if url_tag else ''
  image = image_tag['content'] if image_tag else ''
  description = description_tag['content'] if description_tag else ''
  p_content = p_tag.get_text() if p_tag else ''

  data = (
    title,
    regDate,
    author,
    url,
    image,
    description,
    p_content
  )

  cursor.execute('''INSERT INTO news (title, regDate, author, url, image, description, content) VALUES (?, ?, ?, ?, ?, ?, ?)''', data)
  
conn.commit()
conn.close()
