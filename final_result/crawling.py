from datetime import date
from bs4 import BeautifulSoup
import shutil, os, re, configparser, requests, time

config = configparser.ConfigParser()
config.read('config.properties')

TYPE = config.get('CRAWLING', 'TYPE')

BASE_LIST_URL = config.get('CRAWLING', 'BASE_LIST_URL')
LIST_NUM = config.get('CRAWLING', 'LIST_NUM')
SEARCH = config.get('CRAWLING', 'SEARCH')
SEARCH_PATTERN = config.get('CRAWLING', 'SEARCH_PATTERN')

EXCEPT_URL_STR = config.get('CRAWLING', 'EXCEPT_URL')
EXCEPT_URL = [i for i in EXCEPT_URL_STR.split(',')]

dump_storage = "./dump/dump"
generate_seed_path = "./urls/freegen.txt"
complete_url_path = "./urls/complete.txt"

#### Latest News URL ParseData
def type_verify():
  return TYPE

def generate_list_urls():
  with open(generate_seed_path, 'w') as file:
    for i in range(1, int(LIST_NUM) + 1):
      url = BASE_LIST_URL + str(i)
      file.write(url + "\n")

def list_page_crawling():
  os.system('bin/nutch freegen ./urls/freegen.txt ./crawl-test/segments/')
  folder_path = './crawl-test/segments/'
  subdir = [name for name in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, name))]
  sort = sorted(subdir, reverse=True)
  recent = 'crawl-test/segments/' + sort[0]
  os.system('bin/nutch fetch {0}'.format(recent))
  os.system('bin/nutch parse {0}'.format(recent))
  os.system('bin/nutch readseg -dump crawl-test/segments/{0} dump -nofetch -nogenerate -noparse -noparsetext -nocontent'.format(sort[0]))

def search_word_converting():
  return SEARCH.replace(' ', '+')

def search_page_crawling():
  user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:65.0) Gecko/20100101 Firefox/65.0'
  headers = {'User-Agent': user_agent}
  with open(generate_seed_path, 'r') as file:
    url_list = [line.strip().replace('q=', 'q=' + search_word_converting()) for line in file]
  print(url_list)
  with open(dump_storage, 'w') as file:
    file.write('')
  for i in range(len(url_list)):
    response = requests.get(url_list[i], headers=headers)
    print(response)
    with open(dump_storage, 'a') as file:
      file.write('Recno::' + str(i + 1) + '\n' + response.text + '\n')
    time.sleep(2)

#### Parse Outlink and Anchor
def parse_outlink_ver_nutch(dump, parse):
  pattern = r"toUrl: (.*?) anchor: (.*?)$"
  with open(dump, "r") as infile, open(parse, "w") as outfile:
    for line in infile:
      match = re.search(pattern, line)
      if match:
        outlink = match.group(1)
        anchor = match.group(2)
        outfile.write(f"{outlink}, {anchor}\n")

def parse_outlink_ver_html(dump, parse):
  pattern = SEARCH_PATTERN
  with open(dump, "r") as infile:
    html = infile.read()
  soup = BeautifulSoup(html, 'html.parser')
  filtered_links = []
  for a_tag in soup.find_all('a', href=True):
    href = a_tag['href']
    if pattern in href:
      link_text = a_tag.get_text()
      filtered_links.append((href, link_text))
  with open(parse, "w") as outfile:
    for link in filtered_links:
      outfile.write(f"{link[0]}, {link[1]}\n")

def TODO_daum_news_url(http):
  with open(http, "r") as file:
    lines = file.readlines()
  modified_lines = []
  for line in lines:
    modified_line = line.replace('http', 'https')
    modified_lines.append(modified_line)
  with open(http, 'w') as file:
    file.writelines(modified_lines)

#### Delete Duplication Link
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

def final_data(filtered_data):
  unique_outlinks = set()
  final_data = []
  for outlink, anchor in filtered_data:
    if outlink not in unique_outlinks:
      final_data.append((outlink, anchor))
      unique_outlinks.add(outlink)
  return final_data

#### Lastest News Crawling
def generate_except_url():
  with open(complete_url_path, 'w') as file:
    for item in EXCEPT_URL:
      file.write(item + '\n')

def create_location(output_path):
  today = date.today()
  if(os.path.exists("./urls/{0}".format(today)) == False):
    os.makedirs("./urls/{0}".format(today))
    open('./urls/{0}/seed.txt'.format(today), 'w').close()
  seed = "./urls/{0}/seed.txt".format(today)
  compelted = "./urls/complete_{0}.txt".format(today)
  if(os.path.exists(compelted) == False):
    shutil.copyfile(complete_url_path, compelted)
  with open(output_path, "r") as infile, open(seed, "w") as outfile:
    for line in infile:
      link = line.split(',')[0]
      outfile.write(f"{link}\n")
  return seed, compelted

def read_file(file_path):
  with open(file_path, 'r') as file:
    return file.readlines()

def write_file(file_path, lines):
  with open(file_path, 'w') as file:
    file.writelines(lines)

def url_division(seed, compelted):
  new_urls = read_file(seed)
  complete_urls = read_file(compelted)
  return new_urls, complete_urls

def url_storage_organize(seed, compelted, new_urls, complete_urls):
  unique_urls = [line for line in new_urls if line not in complete_urls]
  combined_urls = complete_urls + unique_urls
  write_file(seed, unique_urls)
  write_file(compelted, combined_urls)

def url_list_crawling():
  os.system('bin/crawl -i -s urls/{0}/ crawl-test/ 1'.format(date.today()))
  folder_path = './crawl-test/segments/'
  subdir = [name for name in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, name))]
  sort = sorted(subdir, reverse=True)
  recent = 'crawl-test/segments/' + sort[0]
  os.system('bin/nutch readseg -dump {0} dump -nofetch -nogenerate -noparse -noparsetext -noparsedata'.format(recent))
  shutil.rmtree('./crawl-test/crawldb')
  shutil.rmtree('./crawl-test/linkdb')
