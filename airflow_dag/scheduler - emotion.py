import pendulum
import shutil, time, os, re
from google.cloud import bigquery, storage
from datetime import datetime, date, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from bs4 import BeautifulSoup
from selenium import webdriver

# timezone 한국시간으로 변경
kst = pendulum.timezone("Asia/Seoul")

# python Operator에서 사용할 함수 정의
def news_crawling():
    #### Action 1. Latest News URL ParseData
    def generate_urls(base_url, num_urls, file_path):
        with open(file_path, 'w') as file:
            for i in range(1, num_urls + 1):
                url = base_url + str(i)
                file.write(url + "\n")

    user_base_url = "https://news.daum.net/breakingnews?page="
    num_urls = 5
    file_path = "../nutch/urls/freegen.txt"
    os.system('ls')
    generate_urls(user_base_url, num_urls, file_path)

    print('FreeGenerator Start')
    os.system('../nutch/bin/nutch freegen ../nutch/urls/freegen.txt ../nutch/crawl-test/segments/')

    print('Path memory')
    folder_path = '../nutch/crawl-test/segments/'
    subdir = [name for name in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, name))]
    sort = sorted(subdir, reverse=True)
    recent = '../nutch/crawl-test/segments/' + sort[0]

    print('FetchList Fetch Start')
    os.system('../nutch/bin/nutch fetch {0}'.format(recent))

    print('Parse Start')
    os.system('../nutch/bin/nutch parse {0}'.format(recent))

    print('ParseData Getting')
    os.system('../nutch/bin/nutch readseg -dump {0} ../nutch/dump -nofetch -nogenerate -noparse -noparsetext -nocontent'.format(recent))

    #### Action 2. Parse Outlink and Anchor
    dump = "../nutch/dump/dump"
    parse = "../nutch/dump/output.txt"
    pattern = r"toUrl: (.*?) anchor: (.*?)$"

    print('anchor, outlink parse')
    with open(dump, "r") as infile, open(parse, "w") as outfile:
        for line in infile:
            match = re.search(pattern, line)
            if match:
                outlink = match.group(1)
                anchor = match.group(2)

                outfile.write(f"{outlink}, {anchor}\n")

    #### Action 3. Delete Duplication Link
    print('duplication Cut')
    def remove_duplicates(data):
        unique_anchors = set()
        filtered_data = []

        for line in data:
            outlink, anchor = line

            if anchor not in unique_anchors:
                filtered_data.append((outlink, anchor))
                unique_anchors.add(anchor)
    
        return filtered_data

    def read_data(file_path):
        data = []
        with open(file_path, 'r') as file:
            for line in file:
                line = line.strip()
                if line:
                    line_parts = line.split(', ')
                    outlink = line_parts[0]
                    anchor = ', '.join(line_parts[1:])
                    data.append((outlink, anchor))
        return data

    def write_data(file_path, data):
        with open(file_path, 'w') as file:
            for outlink, anchor in data:
                file.write(f"{outlink}, {anchor}\n")

    input_path = "../nutch/dump/output.txt"
    output_path = "../nutch/dump/result.txt"

    data = read_data(input_path)
    
    filtered_data = remove_duplicates(data)

    unique_outlinks = set()
    final_data = []

    for outlink, anchor in filtered_data:
        if outlink not in unique_outlinks:
            final_data.append((outlink, anchor))
            unique_outlinks.add(outlink)

    write_data(output_path, final_data)

    #### Action 4. Lastest News Crawling
    print('new SeedList Crawling')
    today = date.today()
    if(os.path.exists("./urls/{0}".format(today)) == False):
        os.makedirs("./urls/{0}".format(today))
        open('./urls/{0}/seed.txt'.format(today), 'w').close()
    seed = "./urls/{0}/seed.txt".format(today)
    compelted = "./urls/complete_{0}.txt".format(today)
    if(os.path.exists(compelted) == False):
        shutil.copyfile('./urls/complete.txt', compelted)

    with open(output_path, "r") as infile, open(seed, "w") as outfile:
        for line in infile:
            link = line.split(',')[0]
            outfile.write(f"{link}\n")

    def read_file(file_path):
        with open(file_path, 'r') as file:
            return file.readlines()
    def write_file(file_path, lines):
        with open(file_path, 'w') as file:
            file.writelines(lines)

    new_urls = read_file(seed)
    complete_urls = read_file(compelted)

    unique_urls = [line for line in new_urls if line not in complete_urls]
    combined_urls = complete_urls + unique_urls

    write_file(seed, unique_urls)
    write_file(compelted, combined_urls)

    url_path = './urls/{0}/seed.txt'.format(today)
    key_path = './key.json'
    bucket_name = 'news_emotion'
    folder_path = './html'

    def upload(key_path, bucket_name, file_path):
        storage_client = storage.Client.from_service_account_json(key_path)
        bucket = storage_client.get_bucket(bucket_name)
    
        for root, dirs, files in os.walk(folder_path):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                if file_path.lower().endswith('.html'):
                    relative_path = os.path.relpath(file_path, folder_path)
                    blob = bucket.blob(relative_path)
                    blob.upload_from_filename(file_path)
                    print(file_name)

    with open(url_path, 'r') as file:
        lines = file.readlines()
    
    for line in lines:
        print(line.strip())
        driver = webdriver.Firefox()
        driver.get(line)
        time.sleep(10)
        html = driver.page_source
        file_path = './html/{0}.html'.format(line.split('/v/')[1].split('\n')[0])
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(html)
        driver.quit()

    upload(key_path, bucket_name, folder_path)

    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):
            os.remove(file_path)

# 기본 args 생성
default_args = {
    'owner' : 'Hello',
}

# DAG 생성
with DAG(
    dag_id='news_emotions',
    default_args=default_args,
    start_date=datetime(2023, 7, 12, tzinfo=kst),
    description='news_crawling',
    schedule_interval='0 */6 * * *',
    tags=['article']
) as dag:
    t1 = PythonOperator(
        task_id='news_crawling',
        python_callable=news_crawling,
        provide_context=True
    )

    t1
