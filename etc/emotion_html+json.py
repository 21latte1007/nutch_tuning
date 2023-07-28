from bs4 import BeautifulSoup
from google.cloud import storage
from selenium import webdriver
import datetime, os, time, json

key_path = './key.json'
bucket_html = 'news_emotion'
bucket_json = 'news_json'
folder_path = './html'
storage_client = storage.Client.from_service_account_json(key_path)

def custom_filter(tag):
    if tag.name == 'meta':
        return tag.get('property') in ['og:title', 'og:regDate', 'og:article:author', 'og:url', 'og:image', 'og:description']
    elif tag.name == 'p':
        return tag.get('dmcf-ptype') == 'general'
    else:
        return False

def class_filter(tag):
    if tag.name == 'h2':
        return tag.get('class') == ['screen_out']

def upload(bucket_name, folder_path):
    bucket = storage_client.get_bucket(bucket_name)
    for root, dirs, files in os.walk(folder_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            if file_path.lower().endswith('.html'):
                relative_path = os.path.relpath(file_path, folder_path)
                blob = bucket.blob(relative_path)
                blob.upload_from_filename(file_path)

with open('./dump/dump', 'r') as file:
    html = file.read()

soup = BeautifulSoup(html, 'html.parser')
segments = str(soup).split("Recno::")
segments = segments[1:]

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
bucket = storage_client.get_bucket(bucket_json)

for segment in segments:
    seg = BeautifulSoup(segment, 'html.parser')

    tags = seg.find_all(custom_filter)
    contents = [str(tag) + '\n' for tag in tags]
    _screens = seg.find_all(class_filter)
    topic = _screens[1]
    
    topic = str(topic).split('>')[1].split('<')[0]

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
        driver.get(url.split('"')[1].split('"')[0])
        time.sleep(5)
        HTML = driver.page_source
        
        file_path = './html/{0}.html'.format(url.split('"')[1].split('"')[0].split("/v/")[1])
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(html)
        driver.quit()        
        
        user = BeautifulSoup(HTML, 'html.parser')
        kinds = ["추천해요", "좋아요", "감동이에요", "화나요", "슬퍼요"]
        feel = []
        num = []
        rateConfig = []
        rate = []
        emotion_total = 0
        comment = ""
        try:
            label = user.find_all('div', class_='jsx-2157231875 🎬_selection_label')
            count = user.find_all('span', class_='jsx-2157231875 🎬_count_label')
            if(len(label) == 5 and len(count) == 5):
                for i in range(5):
                    emotion_find = str(label[i]).split('">')[1].split("</")[0]
                    emotion_figure = str(count[i]).split('">')[1].split("</")[0]
                    feel.append(emotion_find)
                    num.append(emotion_figure)
                    emotion_total += float(emotion_figure)
                for i in range(5):
                    rateConfig.append(feel[i] + "-rate")
                    emotion_figure = str(count[i]).split('">')[1].split("</")[0]
                    if(emotion_total == 0 or emotion_figure == 0):
                        rate.append("0")
                    else:
                        rate.append(str(round(float(emotion_figure) / emotion_total, 5)))
        except AttributeError:
            pass

        try:
            comment = user.find('a', class_='btn_ttalk').text
        except AttributeError:
            pass

        driver.quit()

        if 'og:title' in title and 'og:regDate' in regDate and 'og:article:author' in author and 'og:url' in url and 'og:image' in image and 'og:description' in description and 'general' in content:
            id = url.split('"')[1].split('"')[0].split("/v/")[1]
            title = BeautifulSoup(title, 'html.parser').find('meta', {'property': 'og:title'})['content']
            regDate = BeautifulSoup(regDate, 'html.parser').find('meta', {'property': 'og:regDate'})['content']
            author = BeautifulSoup(author, 'html.parser').find('meta', {'property': 'og:article:author'})['content']
            url = BeautifulSoup(url, 'html.parser').find('meta', {'property': 'og:url'})['content']
            image = BeautifulSoup(image, 'html.parser').find('meta', {'property': 'og:image'})['content']
            p_tags = BeautifulSoup(content, 'html.parser').find_all('p')
            content = '\n'.join([p.get_text() for p in p_tags])
            data = {
                'id': int(id),
                'topic': topic,
                'title': title,
                'regDate': regDate,
                'author': author,
                'url': url,
                'image': image,
                'comment': comment,
                'content': content,
                feel[0]: num[0],
                rateConfig[0]: rate[0],
                feel[1]: num[1],
                rateConfig[1]: rate[1],
                feel[2]: num[2],
                rateConfig[2]: rate[2],
                feel[3]: num[3],
                rateConfig[3]: rate[3],
                feel[4]: num[4],
                rateConfig[4]: rate[4],
                'timestamp': str(timestamp),
            }
            json_file_name = '{0}.json'.format(id)
            with open(json_file_name, 'w', encoding='utf-8') as json_file:
                json.dump(data, json_file, ensure_ascii=False)
            
            blob = bucket.blob(json_file_name)
            blob.upload_from_filename(json_file_name)
            os.remove(json_file_name)

upload(bucket_html, folder_path)
