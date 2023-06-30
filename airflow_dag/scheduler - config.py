import pendulum
import mysql.connector, shutil, os, re
from google.cloud import bigquery
from datetime import datetime, date, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from bs4 import BeautifulSoup
from configparser import ConfigParser, NoOptionError

# timezone 한국시간으로 변경
kst = pendulum.timezone("Asia/Seoul")

# python Operator에서 사용할 함수 정의
def news_crawling():
    PATH = './Downloads/nutch15/'
    
    config = ConfigParser()
    config.read(f'{PATH}config.properties')
    CONFIG_URL = config.get('url_info', 'url')
    CONFIG_NUM = int(config.get('url_info', 'num'))
    
    #### Action 1. Latest News URL ParseData
    def generate_urls(base_url, num_urls, file_path):
        with open(file_path, 'w') as file:
            for i in range(1, num_urls + 1):
                url = base_url + str(i)
                file.write(url + "\n")

    user_base_url = CONFIG_URL
    num_urls = CONFIG_NUM
    file_path = f"{PATH}urls/freegen.txt"

    generate_urls(user_base_url, num_urls, file_path)

    os.system(f'{PATH}bin/nutch freegen {PATH}urls/freegen.txt {PATH}crawl-test/segments/')

    folder_path = f'{PATH}crawl-test/segments/'
    subdir = [name for name in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, name))]
    sort = sorted(subdir, reverse=True)
    recent = f'{PATH}crawl-test/segments/' + sort[0]

    os.system(f'{PATH}bin/nutch fetch {recent}')
    os.system(f'{PATH}bin/nutch parse {recent}')
    os.system(f'{PATH}bin/nutch readseg -dump {recent} {PATH}dump -nofetch -nogenerate -noparse -noparsetext -nocontent')

    #### Action 2. Parse Outlink and Anchor
    dump = f"{PATH}dump/dump"
    parse = f"{PATH}dump/output.txt"
    pattern = r"toUrl: (.*?) anchor: (.*?)$"

    with open(dump, "r") as infile, open(parse, "w") as outfile:
        for line in infile:
            match = re.search(pattern, line)
            if match:
                outlink = match.group(1)
                anchor = match.group(2)

                outfile.write(f"{outlink}, {anchor}\n")

    #### Action 3. Delete Duplication Link
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

    input_path = f"{PATH}dump/output.txt"
    output_path = f"{PATH}dump/result.txt"

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
    today = date.today()
    if(os.path.exists(f"{PATH}urls/{today}") == False):
        os.makedirs(f"{PATH}urls/{today}")
        open(f'{PATH}urls/{today}/seed.txt', 'w').close()
    seed = f"{PATH}urls/{today}/seed.txt"
    compelted = f"{PATH}urls/complete_{today}.txt"
    if(os.path.exists(compelted) == False):
        shutil.copyfile(f'{PATH}urls/complete.txt', compelted)

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

    os.system(f'{PATH}bin/crawl -i -s {PATH}urls/{today}/ {PATH}crawl-test/ 1')

    folder_path = f'{PATH}crawl-test/segments/'
    subdir = [name for name in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, name))]
    sort = sorted(subdir, reverse=True)
    recent = f'{PATH}crawl-test/segments/' + sort[0]

    os.system(f'{PATH}bin/nutch readseg -dump {PATH}crawl-test/segments/{sort[0]} {PATH}dump -nofetch -nogenerate -noparse -noparsetext -noparsedata')

    shutil.rmtree(f'{PATH}crawl-test/crawldb')
    shutil.rmtree(f'{PATH}crawl-test/linkdb')
    
def news_parsing():
    PATH = './Downloads/nutch15/'
    config = ConfigParser()
    config.read(f'{PATH}config.properties')
    
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

    with open(f'{PATH}dump/dump', 'r') as file:
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
        print(contents)

        output_file = f'{PATH}result.html'
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
            
        print(result)
        timestamp = datetime.now() + timedelta(hours = 9)
        
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

# 기본 args 생성
default_args = {
    'owner' : 'Hello',
}

# DAG 생성
with DAG(
    dag_id='news_crawling4',
    default_args=default_args,
    start_date=datetime(2023, 6, 29, tzinfo=kst),
    description='news_crawling',
    schedule_interval='30 */12 * * *',
    tags=['article']
) as dag:
    t1 = PythonOperator(
        task_id='news_crawling',
        python_callable=news_crawling,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id='news_parsing',
        python_callable=news_parsing,
        provide_context=True
    )

    t1 >> t2
