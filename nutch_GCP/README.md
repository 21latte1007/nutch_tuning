## App Engine
java
-- com
---- google - enterprise - cloudsearch - sdk
        LocalFileCredentialFactory.java
-------- config
            Configuration.java
            ConfigValue.java
            Property.java
------ indexing - template
            FullTraversalConnector.java
            LocalFileCheckpointHandler.java
---- search - connector
        ConnectorApplication.java

App Engine 내부에서는 os 명령어를 실행하거나 Configuration이나 Checkpoint 파일을 다룰 수 없었다.
로컬에서 실행하던 디렉터리 구조로는 PrivateKey.json 파일을 읽어올 수조차 없어 GCP에 연동할 수조차 없었다.
단, 파일을 읽는 것은 경로를 조정하면 가능했다. 그래서 App Engine 내부에서 Root 폴더로 취급되는 resources 폴더에 PrivateKey.json 파일을 넣어 GCP와 연동하고,
Configuration이나 Checkpoint 파일은 java 형태로 변형하고 해당 설정을 관리하는 Connector의 SDK 파일 일부를 추출해 와 연결 구조를 변경해서 연동시켰다.
결과는 잘 동작한다.
마치 억지로 동작시킨 듯한 기분이라 이렇게 하는 게 정답은 아닌 것 같지만 아무튼 외부 DB로부터 데이터도 잘 읽어오고, Checkpoint도 잘 갱신한다.
언젠가 정답을 알게 되면 새로이 갱신하겠다.
