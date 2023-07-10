from bs4 import BeautifulSoup
from google.cloud import bigquery
from selenium import webdriver
import datetime, re, os, time

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
table_id = 'qnacom-service.News_Crawling.news_emotion'

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

    if len(arr) < 7:
        pass
    else:
        title = arr[0]
        regDate = arr[1]
        author = arr[2]
        url = arr[3]
        image = arr[4]
        description = arr[5]
        content = ""
        timestamp = datetime.datetime.now() + datetime.timedelta(hours = 9)
        
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
        
        driver = webdriver.Firefox()
        driver.get(url.split('"')[1])
        time.sleep(3)
        HTML = driver.page_source
        user = BeautifulSoup(HTML, 'html.parser')
        emotion = []
        comment = ""
        try:
            label = user.find_all('div', class_='jsx-2157231875 _selection_label')
            count = user.find_all('span', class_='jsx-2157231875 _count_label')
            for i in range(5):
                feel = str(label[i - 1]).split('">')[1].split("</")[0]
                num = str(count[i - 1]).split('">')[1].split("</")[0]
                emotion.append({feel: int(num)})
        except AttributeError:
            pass
        try:
            comment = user.find('a', class_='btn_ttalk').text
        except AttributeError:
            pass

        driver.quit()
        if 'og:title' in title and 'og:regDate' in regDate and 'og:article:author' in author and 'og:url' in url and 'og:image' in image and 'og:description' in description and 'general' in content:
            data = [{
                'id': int(url.split('"')[1].split("/v/")[1]),
                'title': title,
                'regDate': regDate,
                'author': author,
                'url': url,
                'image': image,
                'comment': comment,
                'description': description,
                'content': content,
                'userEmotion': str(emotion),
                'timestamp': str(timestamp)
            }]
            print(title)
            client.insert_rows_json(table_id, data)