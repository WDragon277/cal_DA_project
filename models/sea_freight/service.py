import pandas as pd
import logging
logger = logging.getLogger(__name__)

from tqdm import tqdm
from sklearn.neural_network import MLPRegressor
from elasticsearch import Elasticsearch

from models.sea_freight.model import MLPRModel
from models.sea_freight.repository import by_sea_route_tables, key_list, sea_freight_data
from common.utils.setting import EsSetting
from common.utils.utils import insert_index
from common.utils.utils import delete_index

esinfo = EsSetting()
es = Elasticsearch(esinfo.IP, basic_auth=(esinfo.ID, esinfo.PW))
index_name = esinfo.sea_save_freight


total_test = {}

for i in tqdm(range(len(key_list))):

    # 모델 훈련을 실시할 객체를 생성
    model_trainer = MLPRModel()

    # 데이터 불러오기
    data = by_sea_route_tables[key_list[i]]

    # 데이터 인덱스 재설정
    data = data.reset_index(drop=True)
    data = data.set_index('year_mon')

    # 데이터 분리
    train_data, validation_data, predict_data = model_trainer.data_split(data)

    # 학습 데이터 분리 & 검증 데이터 분리 & 예측 활용 데이터 분리
    x_train, y_train = model_trainer.prepare_train_data(train_data)
    x_test, y_test = model_trainer.prepare_test_data(validation_data)
    x_predcit = model_trainer.prepare_predict_data(predict_data)

    # 모델 훈련 실시
    num_iterations = 30
    mae_results = []

    for _ in tqdm(range(num_iterations)):

        # 객체 내 모델 초기화
        model_trainer.model = MLPRegressor(hidden_layer_sizes=model_trainer.layer_size, activation='relu',
                                               solver='adam', max_iter=300)

        # 객체 내 모델 훈련
        model_trainer.fitting(x_train, y_train)

        # 객체 내 모델 평가
        # 최소 mae 값 모델 저장
        model_trainer.evaluate(x_test, y_test)
        logger.info('min_mae: {}'.format(model_trainer.current_min_mae))

    # 항로별 최소 mae 모델 저장
    total_test[key_list[i]] = [model_trainer.current_min_mae, model_trainer.model]

# ==== 업로드를 위한 예측 데이터 생성 ====
# 훈련된 항로별 모델을 활용 해서 예측값 저장
prediction_result = {}
for i in range(len(key_list)):
    min_mae_model = total_test[key_list[i]][1]

    # 해상운임 항로를 키로 하는 딕셔너리에 예측값 저장
    prediction_result[i] = min_mae_model.predict(x_predcit)

# 전체 데이터 데이터프레임으로 변환
df_prediction = pd.DataFrame(prediction_result)

# ==== 인덱스 추가하기 위한 작업 ==== #
# 마지막 날짜확인
last_date = predict_data.index[-1]
# 날짜의 데이터 타입 맞추기
last_date = pd.to_datetime(last_date, format='%Y%m')

# ==== 예측 시작 년월 세팅 ==== #
# 데이터의 마지막 년월 1개월 이후를 기준으로 삼음
predicted_start_month = last_date + pd.DateOffset(months=1)
# 예측 시작 년울 구간 인덱스로 생성
predict_range_month = pd.date_range(start=predicted_start_month, periods=6, freq='ME')
# 날짜 표시 형태 일치
predict_range_month = predict_range_month.strftime('%Y%m')

# 인덱스(예측 날짜) 설정
df_prediction = df_prediction.set_index(predict_range_month)
# 칼럼 설정
df_prediction.columns = key_list

# 예측데이터를 JSON으로 저장
rows = []

for c_idx, col in df_prediction.items():
    for r_idx, row in col.items():
        rows.append({
            "data_cd": c_idx[0],
            "dptr_cnty": c_idx[1],
            "arvl_cnty": c_idx[2],
            "year_mon": r_idx,
            "cach_amt": row
            })

df_result_predict = pd.DataFrame(rows)

# ==== 기존데이터와 예측데이터 합치기 ====
# 기존 데이터 불러오기
df_raw_data = sea_freight_data

# 기존 데이터와 예측 데이터 concat
df_save_data = pd.concat([df_raw_data, df_result_predict], ignore_index=True, join='inner')
# 데이터 정렬
df_save_data = df_save_data.sort_values(['data_cd', 'dptr_cnty', 'arvl_cnty', 'year_mon', 'cach_amt'])

rows = []

for idx, row in df_save_data.iterrows():
    rows.append({"_index": index_name,
                 "_source": {
                     "data_cd": row.iloc[0],
                     "dptr_cnty": row.iloc[1],
                     "arvl_cnty": row.iloc[2],
                     "year_mon": row.iloc[3],
                     "cach_amt": row.iloc[4]
                 }})

dict_save_data = rows

# 해상 운임 데이터 저장함수
def insert_sea_freight(index_name, dict_save_data):

# 기존 인덱스 내용 제거
    delete_index(index_name)
    # 인덱스에 데이터 삽입
    insert_index(index_name, dict_save_data)





