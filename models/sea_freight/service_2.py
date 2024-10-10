import pandas as pd
from models.sea_freight.repository import sea_freight_data
from models.sea_freight.model import SARIMAmodel
from common.utils.setting import EsSetting

from common.utils.utils import insert_index
from common.utils.utils import delete_index

# 데이터 로드
data = sea_freight_data
data = data.sort_values(by=['dptr_cnty', 'arvl_cnty', 'year_mon'])
# 출발지와 도착지별로 그룹화
routes = data.groupby(['data_cd', 'dptr_cnty', 'arvl_cnty'])
total_result = {}
pred_tmp_rows = []

for (data_cd, dptr_cnty, arvl_cnty), route_data in routes:

    # 모델 초기화를 위한 훈련 객체 선언
    model_trainer = SARIMAmodel()

    # 데이터 년월 단위 정렬
    route_data = route_data.set_index('year_mon')

    # 모델 훈련
    model_fit = model_trainer.model_fitting(route_data)

    # 훈련된 모델로 예측값 생성
    predict = model_trainer.prediction(model_fit)

    # 예측값에 인덱스 추가
    ## 마지막 날짜 확인
    last_date = route_data.index[-1]
    ## 날짜 데이터 타입 세팅
    start_date = pd.to_datetime(last_date, format='%Y%m') + pd.DateOffset(months=1)
    ## 마지막 년월을 기반으로 새로운 날짜 인덱스 생성
    predict_range_month = pd.date_range(start=start_date, periods=len(predict), freq='ME')
    predict_range_month = predict_range_month.strftime('%Y%m')

    # 예측값에 인덱스 추가
    predict.index = predict_range_month

    for idx, pred_value in predict.iterrows():
        pred_tmp_rows.append({
            "data_cd": data_cd,
            "dptr_cnty": dptr_cnty,
            "arvl_cnty": arvl_cnty,
            "year_mon": idx,
            "cach_amt": pred_value[0]
        })


# 예측된 모든 항로의 값을 데이터프레임으로 생성
df_result_predict = pd.DataFrame(pred_tmp_rows)
df_save_data = pd.concat([data, df_result_predict], ignore_index=True)

tmp_rows = []
esinfo = EsSetting()

save_index_name = esinfo.sea_save_freight

for idx, row in df_save_data.iterrows():
    tmp_rows.append({"_index": save_index_name,
                 "_source": {
                     "data_cd": row.loc['data_cd'],
                     "dptr_cnty": row.loc['dptr_cnty'],
                     "arvl_cnty": row.loc['arvl_cnty'],
                     "year_mon": row.loc['year_mon'],
                     "cach_amt": row.loc['cach_amt']
                 }})

dict_save_data = tmp_rows

def insert_sea_freight(index_name, dict_save_data):

# 기존 인덱스 내용 제거
    delete_index(index_name)
    # 인덱스에 데이터 삽입
    insert_index(index_name, dict_save_data)
