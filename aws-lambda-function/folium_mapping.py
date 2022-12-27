from pyathena import connect
from pyathena.cursor import DictCursor
import folium
from folium.plugins import MarkerCluster, Search
import boto3

AWS_ACCESS_KEY_ID = "aws_access_key_id"
AWS_SECRET_ACCESS_KEY = "aws_secret_access_key"


def put_map_s3():
    print('start!\n')
    # 시각화 결과물 템플릿을 html_map 변수에 담기 (단, 시각화 결과물객체 뒤에  ._repr_html_() 붙이기)
    html_map = drawing_map()._repr_html_()  # _repr_html_ : html화 하는 함수
    data = html_map.replace(
        '<div style="width:100%;"><div style="position:relative;width:100%;height:0;padding-bottom:60%;">',
        '<div style="width:100%;height: 100%;"><div style="position:relative;width:100%;height: 100%;">')

    # Creating Session With Boto3.
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    # Creating S3 Resource From the Session.
    s3 = session.resource('s3')
    object = s3.Object('datago-kapt', 'data_map/map.html')
    object.put(Body=data)
    print('finish!\n')


def get_kapt_list():
    # Athena 연결후 kapt 데이터 가져오기
    cursor = connect(aws_access_key_id=AWS_ACCESS_KEY_ID,
                     aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                     s3_staging_dir="s3://datago-kapt/out_athena_result/",
                     region_name="ap-northeast-2",
                     cursor_class=DictCursor).cursor()

    cursor.execute(
        "SELECT DISTINCT k.kaptcode AS dist_kaptcode, k.*, l.latitude, l.longitude FROM kapt.data_basic AS k JOIN kapt.data_coordinates AS l ON k.kaptcode = l.kaptcode AND l.latitude <> 0")

    return cursor.fetchall()


def drawing_map():
    # 리스트를 불러온다
    danji_list = get_kapt_list()
    # 지도 초기 화면 어떻게 보여줄지 설정 (위치, 확대, 가로/세로 길이 등)
    map = folium.Map(location=[36.501, 127.870], zoom_start=8, height='100%')
    mc = MarkerCluster()
    rowCnt = 1
    for danji in danji_list:
        danji_loc = [float(danji['longitude']), float(danji['latitude'])]
        iframe = folium.IFrame('<h2><b>' + str(danji['kaptname']) +
                               '</h2></b><br>단지코드 :' + str(danji['kaptcode']) +
                               '<br>법정동주소 : ' + str(danji['kaptaddr']) +
                               '<br>분양형태 : ' + str(danji['codesalenm']) +
                               '<br>난방방식 : ' + str(danji['codeheatnm']) +
                               '<br>동수 : ' + str(danji['kaptdongcnt']) +
                               '<br>세대수 : ' + str(danji['kaptdacnt']) +
                               '<br>관리사무소연락처 : ' + str(danji['kapttel']) +
                               '<br>관리사무소팩스 : ' + str(danji['kaptfax']) +
                               '<br>홈페이지주소 : ' + str(danji['kapturl']) +
                               '<br>단지분류 : ' + str(danji['codeaptnm']) +
                               '<br>도로명주소 : ' + str(danji['dorojuso']) +
                               '<br>호수 : ' + str(danji['hocnt']) +
                               '<br>관리방식 : ' + str(danji['codemgrnm']) +
                               '<br>복도유형 : ' + str(danji['codehallnm']) +
                               '<br>분양형태 : ' + str(danji['codeaptnm']) +
                               '<br>건축물대장상 연면적: ' + str(danji['kapttarea']))
        popup = folium.Popup(iframe, min_width=500, max_width=500)

        # 지도에 마커 찍기
        mc.add_child(
            folium.Marker(location=danji_loc, popup=popup, tooltip=str(danji['kaptname']), name=str(danji['kaptname'])))
        print(f'row_cnt: {rowCnt}\n')
        rowCnt += 1

    map.add_child(mc)

    # 검색바
    search = Search(layer=mc, geom_type="Point", placeholder="단지명 검색               ", collapsed=False,
                    search_label="name")
    map.add_child(search)

    return map


def lambda_handler(event, context):
    put_map_s3()