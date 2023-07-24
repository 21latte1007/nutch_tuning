# nutch_tuning
## apache nutch, airflow 등 오픈소스를 활용하여 대량의 웹 크롤링(ex. 다음 뉴스, 국가 정책 PDF)
**+ selenium을 곁들여 추가 크롤링**

### 요구사항
- 특정 분류 URL 수집
- 중복 데이터 재수집 방지
- 크롤링한 데이터 정제 후 DB 적재
- 주기적인 크롤링

### 프로세스, 아키텍쳐 구상
**Step 1. Nutch**
수집하고자 하는 뉴스 목록 URL, 페이지 개수 입력.
ex) 최신기사 URL - “yna.co.kr/news/{n}”
“bin/nutch freegen /seed /crawl” 명령어 실행.
Fetch List 생성.
sl = `ls -d crawl/segments/2023… | tail -1`
새로 생성한 segments 폴더의 폴더명 기억.
“bin/nutch fetch $s1”, “bin/nutch parse $s1” 명령어 실행.
생성한 Fetch List의 후속 작업 진행.
“bin/nutch readseg -dump crawl/segments/{s1} dump <parsedata>“
dump 파일을 생성하되 parsedata 부분만 추출.
parsedata의 outlink와 anchor 추출, {outlink, anchor} List 작성.
크롤링한 기사 목록 페이지에서 기사의 URL과 제목 추출.
List에서 이전에 수집한 기사와 동일한 URL이나 제목 제거.
중복 제거된 {outlink, anchor} List를 바탕으로 새로운 Seed 작성.
“bin/crawl -i -s <seed file path> crawl/ 1” 명령어 실행.

**Step 2. DB**
필요한 데이터(기사 제목, URL, 이미지, 본문 등) tag 기반으로 수집.
테이블 생성 후 DB에 적재.

**Step 3. Airflow**
pip install로 airflow 설치.
airflow 폴더 내부에 dags 폴더 생성 후 python 스크립트 작성.
A part와 B part로 Task를 나누어 스케쥴링.

**Local ubuntu 환경에서 구현**
```
Virtual Box - Ubuntu 20.04
Apache Nutch - 1.15
Airflow pip install
```

**GCP VM 환경에서 구현**
```
VM Instance에 GUI 적용.
VM과 Cloud SQL연동.
VM 인스턴스 제작 - HTTP 트래픽 허용.
외부 IP 주소 고정으로 설정.
Cloud SQL 생성 후 VM의 외부 IP 값 네트워크 추가.
“sudo apt-get update” / “sudo apt-get install \ default-mysql-server”
MySQL 바이너리 설치.
생성했던 VM ssh에 접속.
“mysql -h <<SQL_IP>> -u root -p”
password 입력.
“use <DB_NAME>;”
```

## 구현 과정
### 기본 아키텍쳐(ex. 최신 뉴스 크롤링)
![News Crawling 기초 아키텍쳐](https://github.com/21latte1007/nutch_tuning/assets/136875503/e01bf442-2c5f-4259-8ad7-7410d1b16d1e)
1. 최신 뉴스의 목록을 보여주는 웹페이지의 URL을 seed로 설정하고 크롤링을 진행한다.
2. 추출한 링크들 중 최신 뉴스의 본문으로 연결되는 URL만 추출해서 새로운 seed 파일을 생성한다.
3. 제목이나 URL이 동일한 링크를 제거해서 중복 크롤링을 방지하도록 한다.
4. 중복 제거 작업이 끝난 새로운 seed 파일로 다시 apache nutch를 실행해서 최신 뉴스 크롤링 작업을 완료한다.
5. 수집한 웹페이지에서 필요한 부분(제목, 본문, 게시일자 등)의 HTML 태그를 찾아 추출한다.
6. DB와 연결하고 테이블을 생성해 업로드한다.
7. 이후 데이터의 활용 방법은 자유롭게.
**config.properties 파일을 활용하여 URL, DB, HTML Tag 등의 설정을 조작 가능**

### 확장형 아키텍쳐
![News Crawling 확장형 아키텍쳐 구상도](https://github.com/21latte1007/nutch_tuning/assets/136875503/6f5d4a6a-f78a-4d39-a13e-d2d33eb99f30)

## Nutch 디렉터리 기본 구조
```
nutch
  ∟crawl
    ∟crawldb // 작업 완료 후 자동 삭제
    ∟linkdb // 작업 완료 후 자동 삭제
    ∟segment
      ∟20230609065544
      ∟20230609065655
      ∟....
  ∟dump
    ∟dump.txt
    ∟output.txt
    ∟result.txt
  ∟urls
    ∟2023-06-09
    ∟2023-06-10
    ∟....
    ∟complete.txt
    ∟complete_2023-06-09.txt
    ∟complete_2023-06-10.txt
    ∟freegen.txt
  ∟result.html
```

## Github 디렉터리 구조
### airflow_dag
- apache airflow를 활용해서 주기적으로 뉴스 크롤링을 진행하고 DB에 업로드하도록 자동화 작업을 구현한 파일.

### default
- news_crawling : apache nutch를 활용하여 다음 뉴스 등을 최신순, 혹은 검색어 기준으로 크롤링하는 작업을 구현.
- news_parsing : 위에서 크롤링한 뉴스 HTML에서 필요한 데이터를 추출하여 sqlite, bigquery, mysql(cloud sql) 등에 업로드하는 작업을 구현.

### feat
- mysql, sqlite, bigquery : 각각 해당하는 DB로 결과물을 Insert.
- emotion : apache nutch에 selenium을 함꼐 활용하여 동적 데이터인 유저 반응이나 댓글 관련 정보를 파싱하는 작업 구현.
- config : 위에서처럼 sqlite, bigquery 등 DB의 종류에 따라 코드를 새로 작성할 필요 없이, HTML에서 파싱해 올 태그가 바뀔 때마다 코드 전체를 손 볼 필요 없이 "config.properties" 파일에서 옵션 파라미터를 조정하는 것으로 바로 구현이 되도록 "확장성"을 염두하고 구현.

### other
- down_pdf : 첨부파일(ex. 국가 정책 PDF)을 대량으로 다운할 수 있는 작업 구현.
- emotion_gcs : 셀레니움으로 동적 데이터가 포함된 HTML을 크롤링 후 Google Cloud Storage에 업로드하는 작업 구현.
- html_name_config : 크롤링한 HTML의 파일명을 HTML 내부의 'title' 태그로 지정하는 작업 구현.
- upload_real_time_to_gcs : 어떤 파일을 가져오는(작성하는) 즉시 Google Cloud Storage에 업로드하는 작업 구현.
- replace_string : 문자열 수정 기본.
- web_crwaling : 웹 크롤링 기본.
