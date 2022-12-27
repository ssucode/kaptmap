# 공동주택정보
### Project URL
- http://kaptmap.ap-northeast-2.elasticbeanstalk.com
---
#### 1. 애플리케이션 구현
- Python Flask

#### 2. 서버환경 구성
- AWS Elastic Beanstalk
- Lambda
공공데이터포털 공동주택정보 API 데이터를 S3에 parquet 저장 - Python
Athena 사용하여 단지의 주소를 가져와 좌표값 API(goecode, 카카오) 데이터를 S3에 parquet 저장 - Python
Athena 사용하여 단지정보를 folium 라이블러리를 이용하여 지도 시각화 html 파일로 S3에 저장 - Python
- S3(parquet 파일 저장)
- AWS Glue(Crawler 를 통한 메타 테이블 생성)
- Athena(메타 테이블에 Query)

#### 3. 배포 자동화
- GitHub, GitHub Actions
