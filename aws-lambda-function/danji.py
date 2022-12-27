import requests
import xmltodict
import json
import pandas as pd
import awswrangler as wr

######   지역코드   ##########
# 11	서울특별시
# 26	부산광역시
# 27	대구광역시
# 28	인천광역시
# 29	광주광역시
# 30	대전광역시
# 31	울산광역시
# 36	세종특별자치시
# 41	경기도
# 42	강원도
# 43	충청북도
# 44	충청남도
# 45	전라북도
# 46	전라남도
# 47	경상북도
# 48	경상남도
# 50	제주특별자치도
###########################

url1 = 'http://apis.data.go.kr/1613000/AptListService2/getTotalAptList' # 전체 단지정보
url2 = 'http://apis.data.go.kr/1613000/AptBasisInfoService1/getAphusBassInfo' # 단지 기본정보
url3 = 'http://apis.data.go.kr/1613000/AptBasisInfoService1/getAphusDtlInfo' # 단지 상세정보
url4 = 'http://apis.data.go.kr/1613000/AptListService2/getSidoAptList' # 시/도 단지정보

SERVICE_KEY = "SERVICE KEY" # 공공데이터포털 API PUBLIC 키


def call_api(url, param):
    params = {'serviceKey' : SERVICE_KEY }
    params.update(param);
    r = requests.get(url, params=params)
    if r.status_code == 200:
        xmlData = r.content.decode('utf-8')
        parseData = xmltodict.parse(xmlData)
        return json.loads(json.dumps(parseData))
    else:
        return False


def get_kapt(sido):
    print('start!\n')
    total = get_apt_total(sido)
    pages = int(int(total)/100) + 1
    #pages = 1
    #print(f'pages : {pages}')
    aptDanjiData = get_apt_danji(sido, pages)
    dfAptDanjiData = pd.DataFrame(aptDanjiData)
    #print(f'dfAptDanjiData: {dfAptDanjiData}\n')

    #dfAptDanjiData.to_parquet(f"{sido}.parquet", engine='pyarrow', compression='snappy')

    # Storing data on Data Lake
    wr.s3.to_parquet(
        df=dfAptDanjiData,
        path=f"s3://test-kapt/data/{sido}.parquet",
    )

    print('finish!\n')


def get_apt_total(sido):
    jData = call_api(url4, {'sidoCode' : sido, 'pageNo' : '1', 'numOfRows' : '100' })
    if jData:
        # print data
        total = jData['response']['body']['totalCount']
        print(f'단지코드수 : {int(total):,} 개')
        return total
    else:
        print(f'Result Code : def call_api Fail!!')
        return False


def get_apt_danji(sido, pages):
    rowCnt = 1
    resJsonData = json.loads('[]')
    for i in range(1, pages + 1):
        jData = call_api(url4, {'sidoCode' : sido, 'pageNo' : i, 'numOfRows' : '100' })
        rowsData = jData['response']['body']['items']['item']
        #print(rowsData)
        #print(f'page: {i} End\n{"=" * 50}')
        try:
            for rowData in rowsData:
                kaptCode = rowData.get('kaptCode')
                rData = get_apt_danji_detail(kaptCode, rowData)
                resJsonData.append(rData)
                print(f'row_cnt: {rowCnt} / {kaptCode}\n')
                rowCnt += 1

        except Exception as e:
            print(f'Error: {e}\n')
            break
    return resJsonData;


def get_apt_danji_detail(kaptCode, data):
    jData = call_api(url2, {'kaptCode' : kaptCode })
    rData = jData['response']['body']['item']
    if rData:
        data.update(rData)
    # jData = call_api(url3, {'kaptCode' : kaptCode })
    # rData = jData['response']['body']['item']
    # if rData:
    #     data.update(rData)
    return data


def lambda_handler(event, context):
    get_kapt(41)
