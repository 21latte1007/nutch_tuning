from bs4 import BeautifulSoup

def extract_title_from_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    title_tag = soup.find('title')
    if title_tag:
        return title_tag.get_text()
    else:
        return None

with open('./nutch15/dump/dump', 'r') as file:
    html = file.read()

soup = BeautifulSoup(html, 'html.parser')
segments = str(soup).split("Recno::")
segments = segments[1:]

for i in range(len(segments)):
    html = segments[i].split("Content:")
    title = extract_title_from_html(html[2])
    if title:
        output_file_path = f"./nutch15/html/{title}.html"
        with open(output_file_path, 'w') as file:
            file.write(html[2])
