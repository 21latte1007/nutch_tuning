import pendulum
import shutil, os, re
from google.cloud import bigquery
from datetime import datetime, date, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from bs4 import BeautifulSoup

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

    # user_base_url = input("Crawling URL: ")
    user_base_url = "https://news.daum.net/breakingnews?page="
    # num_urls = int(input("Page of 1 Job: "))
    num_urls = 1
    file_path = "./Downloads/nutch15/urls/freegen.txt"

    generate_urls(user_base_url, num_urls, file_path)

    print('FreeGenerator Start')
    os.system('./Downloads/nutch15/bin/nutch freegen ./Downloads/nutch15/urls/freegen.txt ./Downloads/nutch15/crawl-test/segments/')

    print('Path memory')
    folder_path = './Downloads/nutch15/crawl-test/segments/'
    subdir = [name for name in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, name))]
    sort = sorted(subdir, reverse=True)
    recent = './Downloads/nutch15/crawl-test/segments/' + sort[0]

    print('FetchList Fetch Start')
    os.system('./Downloads/nutch15/bin/nutch fetch {0}'.format(recent))

    print('Parse Start')
    os.system('./Downloads/nutch15/bin/nutch parse {0}'.format(recent))

    print('ParseData Getting')
    os.system('./Downloads/nutch15/bin/nutch readseg -dump {0} ./Downloads/nutch15/dump -nofetch -nogenerate -noparse -noparsetext -nocontent'.format(recent))

    #### Action 2. Parse Outlink and Anchor
    dump = "./Downloads/nutch15/dump/dump"
    parse = "./Downloads/nutch15/dump/output.txt"
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

    input_path = "./Downloads/nutch15/dump/output.txt"
    output_path = "./Downloads/nutch15/dump/result.txt"

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
    if(os.path.exists("./Downloads/nutch15/urls/{0}".format(today)) == False):
        os.makedirs("./Downloads/nutch15/urls/{0}".format(today))
        open('./Downloads/nutch15/urls/{0}/seed.txt'.format(today), 'w').close()
    seed = "./Downloads/nutch15/urls/{0}/seed.txt".format(today)
    compelted = "./Downloads/nutch15/urls/complete_{0}.txt".format(today)
    if(os.path.exists(compelted) == False):
        shutil.copyfile('./Downloads/nutch15/urls/complete.txt', compelted)

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

    os.system('./Downloads/nutch15/bin/crawl -i -s ./Downloads/nutch15/urls/{0}/ ./Downloads/nutch15/crawl-test/ 1'.format(today))

    folder_path = './Downloads/nutch15/crawl-test/segments/'
    subdir = [name for name in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, name))]
    sort = sorted(subdir, reverse=True)
    recent = './Downloads/nutch15/crawl-test/segments/' + sort[0]

    os.system('./Downloads/nutch15/bin/nutch readseg -dump ./Downloads/nutch15/crawl-test/segments/{0} ./Downloads/nutch15/dump -nofetch -nogenerate -noparse -noparsetext -noparsedata'.format(sort[0]))

    shutil.rmtree('./Downloads/nutch15/crawl-test/crawldb')
    shutil.rmtree('./Downloads/nutch15/crawl-test/linkdb')
    
def news_parsing():
    with open('./Downloads/nutch15/dump/dump', 'r') as file:
        html = file.read()

    def custom_filter(tag):
        if tag.name == 'meta':
            return tag.get('property') in ['og:title', 'og:regDate', 'og:article:author', 'og:url', 'og:image', 'og:description']
        elif tag.name == 'p' or tag.name == 'div':
            return tag.get('dmcf-ptype') == 'general'
        else:
            return False
        
    soup = BeautifulSoup(html, 'html.parser')
    segments = str(soup).split("Recno::")
    segments = segments[1:]
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./key.json"
    client = bigquery.Client()
    table_id = <<BIG_QUERY_ID>>
    
    for segment in segments:
        seg = BeautifulSoup(segment, 'html.parser')
        tags = seg.find_all(custom_filter)
        contents = [str(tag) + '\n' for tag in tags]

        output_file = './Downloads/nutch15/result.html'
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
        timestamp = datetime.now() + timedelta(hours = 9)

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
                
        data = [
            {
                'title': title,
                'regDate': regDate,
                'author': author,
                'url': url,
                'image': image,
                'description': description,
                'content': content,
                'timestamp': str(timestamp)
            }
        ]
        
        client.insert_rows_json(table_id, data)

# 기본 args 생성
default_args = {
    'owner' : 'Hello',
}

# DAG 생성
with DAG(
    dag_id='news_crawling3',
    default_args=default_args,
    start_date=datetime(2023, 6, 29, tzinfo=kst),
    description='news_crawling',
    schedule_interval='0 */12 * * *',
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
