from bs4 import BeautifulSoup
import complete_crawling
import complete_parsing
import complete_load

###### Part 1. Crawling
html_array_file = complete_crawling.dump_storage
input_path = "./dump/output.txt"
output_path = "./dump/result.txt"

type = complete_crawling.type_verify()
complete_crawling.generate_list_urls()
if type == 'latest news':
  complete_crawling.list_page_crawling()
  complete_crawling.parse_outlink_ver_nutch(html_array_file, input_path)
elif type == 'search':
  complete_crawling.search_page_crawling()
  complete_crawling.parse_outlink_ver_html(html_array_file, input_path)
  complete_crawling.TODO_daum_news_url(input_path)

data = complete_crawling.read_data(input_path)
filtered_data = complete_crawling.remove_duplicates(data)
final_data = complete_crawling.final_data(filtered_data)
complete_crawling.write_data(output_path, final_data)

complete_crawling.generate_except_url()
seed, compelted = complete_crawling.create_location(output_path)
new_urls, complete_urls = complete_crawling.url_division(seed, compelted)
complete_crawling.url_storage_organize(seed, compelted, new_urls, complete_urls)
complete_crawling.url_list_crawling()

##### Part 2. Parsing
with open(html_array_file, 'r') as file:
  html = file.read()

soup = BeautifulSoup(html, 'html.parser')
segments = str(soup).split("Recno::")
segments = segments[1:]

for segment in segments:
  seg = BeautifulSoup(segment, 'html.parser')
  parsing_tag = complete_parsing.tag_list_parse(seg)
  json_data, html_data = complete_parsing.output_data(parsing_tag, seg)

  ###### Part 3. Data Load
  complete_load.local_save(json_data, html_data)
  # complete_load.upload_big_query(json_data)
  # complete_load.upload_cloud_sql(json_data)

# complete_load.upload_cloud_storage()
# complete_load.record_delete()
