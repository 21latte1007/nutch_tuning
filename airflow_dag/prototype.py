import pendulum
import shutil, os, re
import mysql.connector, sqlite3
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
    print(recent)
    print(sort[0])
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

    output_file = './Downloads/nutch15/result.html'
    with open(output_file, 'w') as file:
        file.write('\n'.join(contents))

    with open(output_file, 'r') as file:
        html = file.read()

    soup = BeautifulSoup(html, 'html.parser')

    # for tag in soup.find_all('p', attrs={'dmcf-pid': True}):
    #     del tag['dmcf-pid']
    # modified_html = str(soup).replace('</p>\n<p dmcf-ptype="general">', '\n')

    # segments = modified_html.split("</p>\n<meta content=")
    segments = str(soup).split("</p>\n<meta")
    # segments = segments[:-1]
    
    for segment in segments:
        segment = '<meta' + segment
        empty = segment.strip()
    
        with open(output_file, 'w') as file:
            file.write(empty)

        with open(output_file, 'r') as file:
            html = file.read()

        soup = BeautifulSoup(html, 'html.parser')
        conn = mysql.connector.connect(
            host=<<DB_IP>,
            user=<<USER_NAME>>,
            password=<<>>,
            database='crawling'
        )
    
        cursor = conn.cursor()

        arr = str(soup).splitlines()
    
        title = arr[0]
        regDate = arr[1]
        author = arr[2]
        url = arr[3]
        image = arr[4]
        description = arr[5]
        content = ""

        arr = arr[6:]
    
        for a in arr:
            content += a
        
        # title_tag = soup.find('meta', {'property': 'og:title'})
        # date_tag = soup.find('meta', {'property': 'og:regDate'})
        # author_tag = soup.find('meta', {'property': 'og:article:author'})
        # url_tag = soup.find('meta', {'property': 'og:url'})
        # image_tag = soup.find('meta', {'property': 'og:image'})
        # description_tag = soup.find('meta', {'property': 'og:description'})
        # p_tag = soup.find('p', {'dmcf-ptype': 'general'})
    
        # title = title_tag['content'] if title_tag else ''
        # regDate = date_tag['content'] if date_tag else ''
        # author = author_tag['content'] if author_tag else ''
        # url = url_tag['content'] if url_tag else ''
        # image = image_tag['content'] if image_tag else ''
        # description = description_tag['content'] if description_tag else ''
        # p_content = p_tag.get_text() if p_tag else ''
        
        # title = '<meta content="' + title + '" property="og:title"/>'
        # regDate = '<meta content="' + regDate + '" property="og:regDate"/>'
        # author = '<meta content="' + author + '" property="og:article:author"/>'
        # url = '<meta content="' + url + '" property="og:url"/>'
        # image = '<meta content="' + image + '" property="og:image"/>'
        # description = '<meta content="' + description + '" property="og:description"/>'
    
        sql = 'INSERT INTO news (title, regDate, author, url, image, description, content) VALUES (%s, %s, %s, %s, %s, %s, %s)'

        data = (
            title,
            regDate,
            author,
            url,
            image,
            description,
            content
        )

        cursor.execute(sql, data)
    
        conn.commit()
        conn.close()

# 기본 args 생성
default_args = {
    'owner' : 'Hello News',
}

# DAG 생성
with DAG(
    dag_id='news_crawling',
    default_args=default_args,
    start_date=datetime(2023, 6, 29, tzinfo=kst),
    description='news_crawling',
    schedule_interval='0 */6 * * *',
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
