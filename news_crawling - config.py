from datetime import date
from configparser import ConfigParser
import shutil, os, re

config = ConfigParser()
config.read('config.properties')

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
file_path = "./urls/freegen.txt"

generate_urls(user_base_url, num_urls, file_path)

print('FreeGenerator Start')
os.system('bin/nutch freegen ./urls/freegen.txt ./crawl-test/segments/')

print('Path memory')
folder_path = './crawl-test/segments/'
subdir = [name for name in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, name))]
sort = sorted(subdir, reverse=True)
recent = 'crawl-test/segments/' + sort[0]

print('FetchList Fetch Start')
print(recent)
print(sort[0])
os.system('bin/nutch fetch {0}'.format(recent))

print('Parse Start')
os.system('bin/nutch parse {0}'.format(recent))

print('ParseData Getting')
os.system('bin/nutch readseg -dump crawl-test/segments/{0} dump -nofetch -nogenerate -noparse -noparsetext -nocontent'.format(sort[0]))

#### Action 2. Parse Outlink and Anchor
dump = "./dump/dump"
parse = "./dump/output.txt"
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

input_path = "./dump/output.txt"
output_path = "./dump/result.txt"

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
completed = "./urls/complete_{0}.txt".format(today)
if(os.path.exists(completed) == False):
    shutil.copyfile('./urls/complete.txt', completed)

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
complete_urls = read_file(completed)

unique_urls = [line for line in new_urls if line not in complete_urls]
combined_urls = complete_urls + unique_urls

write_file(seed, unique_urls)
write_file(completed, combined_urls)

os.system('bin/crawl -i -s urls/{0}/ crawl-test/ 1'.format(today))

folder_path = './crawl-test/segments/'
subdir = [name for name in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, name))]
sort = sorted(subdir, reverse=True)
recent = 'crawl-test/segments/' + sort[0]

os.system('bin/nutch readseg -dump crawl-test/segments/{0} dump -nofetch -nogenerate -noparse -noparsetext -noparsedata'.format(sort[0]))

shutil.rmtree('./crawl-test/crawldb')
shutil.rmtree('./crawl-test/linkdb')

os.system('python3 config-2.py')
