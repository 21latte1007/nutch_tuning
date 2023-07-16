# nutch_tuning
## apache nutch, airflow 등 오픈소스를 활용하여 대량의 웹 크롤링(ex. 다음 뉴스, 국가 정책 PDF)

## 구현 과정
### 기본 아키텍쳐(ex. 최신 뉴스 크롤링)
1. 최신 뉴스의 목록을 보여주는 웹페이지의 URL을 seed로 설정하고 크롤링을 진행한다.
2. 추출한 링크들 중 최신 뉴스의 본문으로 연결되는 URL만 추출해서 새로운 seed 파일을 생성한다.
3. 제목이나 URL이 동일한 링크를 제거해서 중복 크롤링을 방지하도록 한다.
4. 중복 제거 작업이 끝난 새로운 seed 파일로 다시 apache nutch를 실행해서 최신 뉴스 크롤링 작업을 완료한다.
5. 수집한 웹페이지에서 필요한 부분(제목, 본문, 게시일자 등)의 HTML 태그를 찾아 추출한다.
6. DB와 연결하고 테이블을 생성해 업로드한다.
7. 이후 데이터의 활용 방법은 자유롭게.

## 디렉터리 구조
### airflow_dag
apache airflow를 활용해서 주기적으로 뉴스 크롤링을 진행하고 DB에 업로드하도록 자동화 작업을 구현한 파일.

### default
news_crawling : apache nutch를 활용하여 다음 뉴스 등을 최신순, 혹은 검색어 기준으로 크롤링하는 작업을 구현.
news_parsing : 위에서 크롤링한 뉴스 HTML에서 필요한 데이터를 추출하여 sqlite, bigquery, mysql(cloud sql) 등에 업로드하는 작업을 구현.

### feat
- mysql, sqlite, bigquery : 각각 해당하는 DB로 결과물을 Insert.
- emotion : apache nutch에 selenium을 함꼐 활용하여 동적 데이터인 유저 반응이나 댓글 관련 정보를 파싱하는 작업 구현.
- config : 위에서처럼 sqlite, bigquery 등 DB의 종류에 따라 코드를 새로 작성할 필요 없이, HTML에서 파싱해 올 태그가 바뀔 때마다 코드 전체를 손 볼 필요 없이 "config.properties" 파일에서 옵션 파라미터를 조정하는 것으로 바로 구현이 되도록 "확장성"을 염두하고 구현.

### other
- down_pdf : 첨부파일(ex. 국가 정책 PDF)을 대량으로 다운할 수 있는 작업 구현.
- html_name_config : 크롤링한 HTML의 파일명을 HTML 내부의 'title' 태그로 지정하는 작업 구현.
- upload_real_time_to_gcs : 어떤 파일을 가져오는(작성하는) 즉시 Google Cloud Storage에 업로드할 수 있는 작업 구현.
- replace_string : 문자열 수정 기본.
- web_crwaling : 웹 크롤링 기본.
