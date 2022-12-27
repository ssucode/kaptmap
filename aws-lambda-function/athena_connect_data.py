import requests
import json
from pyathena import connect
from pyathena.cursor import DictCursor
from geopy.geocoders import Nominatim
import pandas as pd
import osmnx as ox

AWS_ACCESS_KEY_ID = "aws_access_key_id"
AWS_SECRET_ACCESS_KEY = "aws_secret_access_key"
KAKAO_API_KEY = "kakao_api_key"

# tags = {'amenity': True}
# demo = dict()

# goecode를 이용한 좌표 가져오기
def geocoding(address):
    geolocoder = Nominatim(user_agent='South Korea')
    #geo = geolocoder.geocode(address)
    geo = None
    if (geo == None):
        crd = geocoding_kakao(address)
    else:
        crd = (float(geo.latitude), float(geo.longitude))
    return crd

# 카카오 API로 좌표 가져오기
def geocoding_kakao(address):
    url = f'https://dapi.kakao.com/v2/local/search/address.json?query={address}'
    apiKey = KAKAO_API_KEY
    headers = {"Authorization": "KakaoAK " + apiKey}
    result = json.loads(str(requests.get(url, headers=headers).text))
    if (len(result['documents']) == 0):
        crd = (0, 0)
    else:
        match_first = result['documents'][0]['address']
        crd = (float(match_first['x']), float(match_first['y']))
    return crd

def get_loc_kapt(sido):
    cursor = connect(aws_access_key_id=AWS_ACCESS_KEY_ID,
                     aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                     s3_staging_dir="s3://datago-kapt/out_athena_result/",
                     region_name="ap-northeast-2",
                     cursor_class=DictCursor).cursor()
    cursor.execute(f"SELECT * FROM kapt.data WHERE bjdcode LIKE '{sido}%' ORDER BY dorojuso")
    rows = cursor.fetchall()
    print('start!\n')
    print(f'단지코드수 : {int(len(rows)):,} 개')
    rowCnt = 1
    locDictData = []
    for row in rows:
        kaptCode = row['kaptcode']
        address = row['dorojuso']
        if (address == None):
            address = row['kaptaddr']
        # 위도, 경도 파악
        crd = geocoding(address)
        print(f'row_cnt: {rowCnt} / {kaptCode} / {address}\n')
        print(f"위도 : {crd[0]} / 경도 : {crd[1]}\n")
        locDictData.append({'kaptCode':kaptCode, 'latitude':crd[0], 'longitude':crd[1]})
        rowCnt += 1

        # pois = ox.geometries.geometries_from_point(crd, tags=tags, dist=500)
        # poi_count = pois['amenity'].value_counts()
        # demo[address] = poi_count
        # print(poi_count)

    # address = "서울특별시 종로구 사직로8길 24"
    # crd = geocoding(address)
    # pois = ox.geometries.geometries_from_point(crd, tags=tags, dist=100)
    # poi_count = pois['amenity'].value_counts()
    # demo[address] = poi_count
    # print(poi_count)

    pdLocDictData = pd.DataFrame(locDictData)
    #print(f'{pdLocDictData}\n')
    pdLocDictData.to_parquet(f"{sido}_loc.parquet", engine='pyarrow', compression='snappy')
    print('finish!\n')

######   지역코드   ##########
# 11	서울특별시 -
# 26	부산광역시 -
# 27	대구광역시 -
# 28	인천광역시 -
# 29	광주광역시 -
# 30	대전광역시 -
# 31	울산광역시 -
# 36	세종특별자치시 -
# 41	경기도 -
# 42	강원도 -
# 43	충청북도 -
# 44	충청남도 -
# 45	전라북도 -
# 46	전라남도 -
# 47	경상북도 -
# 48	경상남도 -
# 50	제주특별자치도 -
###########################

# ex)
def lambda_handler(event, context):
    get_loc_kapt(50)