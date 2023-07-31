from bs4 import BeautifulSoup
import complete_crawling
import complete_parsing
import complete_load

###### Part Crawling
user_base_url = 'https://news.daum.net/breakingnews?page='
num_urls = 1

FREEGEN_PATH = "./urls/freegen.txt"
HTML_ARRAY_FILE = "./dump/dump"
INPUT_PATH = "./dump/output.txt"
OUTPUT_PATH = "./dump/result.txt"

complete_crawling.generate_urls(user_base_url, num_urls, FREEGEN_PATH)
complete_crawling.list_page_crawling()
complete_crawling.parse_outlink_and_anchor(HTML_ARRAY_FILE, INPUT_PATH)

data = complete_crawling.read_data(INPUT_PATH)
filtered_data = complete_crawling.remove_duplicates(data)
final_data = complete_crawling.final_data(filtered_data)
complete_crawling.write_data(OUTPUT_PATH, final_data)

seed, compelted = complete_crawling.create_location(OUTPUT_PATH)
new_urls, complete_urls = complete_crawling.url_division(seed, compelted)
complete_crawling.url_storage_organize(seed, compelted, new_urls, complete_urls)
complete_crawling.url_list_crawling()

###### Part Parsing
### TODO : HTML Array File Load ###
with open(HTML_ARRAY_FILE, 'r') as file:
  html = file.read()
  
soup = BeautifulSoup(html, 'html.parser')
segments = str(soup).split("Recno::")
segments = segments[1:]

for segment in segments:
  seg = BeautifulSoup(segment, 'html.parser')
  parsing_tag = complete_parsing.tag_list_parse(seg)
  json_data, html_data = complete_parsing.output_data(parsing_tag, seg)

  ###### Part Data Load
  complete_load.local_save(json_data, html_data)
  # complete_load.upload_big_query(json_data)
  # complete_load.upload_cloud_sql(json_data)

complete_load.upload_cloud_storage()
complete_load.record_delete()
