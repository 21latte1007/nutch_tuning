from datetime import date
import shutil, os, re

#### Latest News URL ParseData
def generate_urls(base_url, num_urls, file_path):
  with open(file_path, 'w') as file:
    for i in range(1, num_urls + 1):
      url = base_url + str(i)
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

#### Parse Outlink and Anchor
def parse_outlink_and_anchor(dump, parse):
  pattern = r"toUrl: (.*?) anchor: (.*?)$"
  with open(dump, "r") as infile, open(parse, "w") as outfile:
    for line in infile:
      match = re.search(pattern, line)
      if match:
        outlink = match.group(1)
        anchor = match.group(2)
        outfile.write(f"{outlink}, {anchor}\n")

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
def create_location(output_path):
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
