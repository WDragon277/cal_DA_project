from models.sea_freight.model import total_test
from models.sea_freight.repository import by_sea_route_tables
# 모델 호출
sea_freight_models = {key: value[8] for key, value in total_test.items()}
# 업로드를 위한 예측 데이터 생성
for key, value in sea_freight_models:
    # 추론에 사용할 데이터 불러오기
    data = by_sea_route_tables[key]
    # 추론에 이용할 데이터만 분리
    x_pred = data['cach_amt']

    # 예측데이터를 딕셔너리로 저장


# 생성된 데이터를 엘라스틱 서치에 데이터 저장
    # 기존의 엘라스틱 서치 데이터를 불러옴

    # 생성된 부분의 데이터를 불러온 데이터에 삽입함

    #