import requests
from bs4 import BeautifulSoup

url = 'https://search.daum.net/search?w=news&nil_search=btn&DA=STC&enc=utf8&cluster=y&cluster_page=1&q=%EB%8C%80%ED%95%9C%ED%95%AD%EA%B3%B5&p={0}&show_dns=1'

for i in range(80):
    response = requests.get(url.format(i))

    html_text = response.text

    soup = BeautifulSoup(html_text, 'html.parser')

    a_tag = soup.find_all('a')

    with open('output.txt', 'a') as file:
        for a in a_tag:
            href = a.get('href')
            if 'http://v.daum.net/v/' in href:
                file.write(href + '\n')

filename = './output.txt'

# 중복을 제거한 URL을 저장할 리스트
unique_urls = []

# 파일을 읽어와서 중복을 제거한 URL을 unique_urls 리스트에 추가
with open(filename, 'r') as file:
    for line in file:
        url = line.strip()  # 줄바꿈 문자 제거
        if url not in unique_urls:
            unique_urls.append(url)

# 중복이 제거된 URL을 새로운 파일에 저장
new_filename = './output-2.txt'
with open(new_filename, 'w') as file:
    for url in unique_urls:
        file.write(url + '\n')
