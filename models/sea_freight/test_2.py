from tqdm import tqdm
import logging
logger = logging.getLogger(__name__)
import numpy as np
import pandas as pd

from sklearn.neural_network import MLPRegressor
from sklearn.metrics import mean_absolute_error

from models.sea_freight.repository import by_sea_route_tables, key_list, sea_freight_data
from common.utils.setting import EsSetting

from elasticsearch import Elasticsearch, helpers

esinfo = EsSetting()
es = Elasticsearch(esinfo.IP, basic_auth=(esinfo.ID, esinfo.PW))
index_name = esinfo.sea_read_freight


class MLPRModel:
    def __init__(self, lags=12, for_period=6, test_period = 6, layer_size=(50,50,100,50,100)):
        self.lags = lags
        self.for_period = for_period
        self.test_period = test_period
        self.layer_size = layer_size
        self.validation_size = for_period + lags + test_period
        self.model = MLPRegressor(hidden_layer_sizes=layer_size, activation='relu', solver='adam', max_iter=300)
        self.mae_result = []
        self.min_model = None
        self.current_min_mae = float('inf')

    def data_split(self, data):
        train_data = data['cach_amt'][:-self.validation_size]
        validation_data = data['cach_amt'][-self.validation_size:]
        predict_data = data['cach_amt'][-self.for_period-self.lags:]
        return train_data, validation_data, predict_data

    def prepare_train_data(self, train_data):
        """훈련데이터를 입력(X_train)과 타깃(y_train)으로 분리"""

        x_train, y_train = [],[]

        for train_num in range(self.lags, len(train_data) - self.for_period):
            x_train.append(train_data.iloc[train_num - self.lags:train_num])
            y_train.append(train_data.iloc[train_num + self.for_period - 1])
        return np.array(x_train), np.array(y_train)

    def prepare_test_data(self, validation_data):
        """테스트 데이터를 입력(x_test)과 타깃(y_test)로 분리"""
        x_test, y_test = [],[]
        for val_num in range(self.lags, len(validation_data) - self.for_period):
            x_test.append(validation_data.iloc[val_num - self.lags:val_num])
            y_test.append(validation_data.iloc[val_num + self.for_period - 1])
        return np.array(x_test), np.array(y_test)

    def prepare_predict_data(self, predict_data):
        x_predcit = []
        for predict_num in range(self.for_period):
            x_predcit.append(predict_data.iloc[predict_num:predict_num + self.lags])
        return x_predcit

    def fitting(self, x_train, y_train):
        """모델 학습"""
        self.model.fit(x_train, y_train)

    def evaluate(self, x_test, y_test):
        """모델 평가 및 최소 MAE 값을 가지는 모델 저장"""
        predictions = self.model.predict(x_test)
        mae = mean_absolute_error(y_test, predictions)
        self.mae_result.append(mae)
        if mae < self.current_min_mae:
            self.current_min_mae = mae
            self.min_model = self.model

def insert_index(index_name, result_predict):

    es = Elasticsearch(esinfo.IP, basic_auth=(esinfo.ID, esinfo.PW))

    if not es.indices.exists(index=index_name):
        # Create the index
        es.indices.create(index=index_name)
        logger.info("새로운 인덱스가 생성되었습니다.")

    # Insert data into Elasicsearch
    helpers.bulk(es, result_predict)

def delete_index(index_name):

    es = Elasticsearch(esinfo.IP, basic_auth=(esinfo.ID, esinfo.PW))  # ops

    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        logger.info("데이터 수정/삽입을 위해 기존의 인덱스가 제거되었습니다.")

def date_setting(df_prediction, last_date, key_list):
    # ==== 인덱스 추가하기 위한 작업 ====
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
    return df_prediction



if __name__ == '__main__':

    total_test = {}
    prediction_result = {}
    last_date = float()

    for i in tqdm(range(len(key_list))):

        # 모델 훈련을 실시할 객체를 생성
        model_trainer = MLPRModel()

        # 데이터 불러오기
        data = by_sea_route_tables[key_list[i]]

        # 데이터 인덱스 재설정
        data = data.reset_index(drop=True)
        data = data.set_index('year_mon')

        # 마지막날 추출
        last_date = data.index[-1]

        # 데이터 분리
        train_data, validation_data, predict_data = model_trainer.data_split(data)


        # 학습 데이터 분리 & 검증 데이터 분리 & 예측 활용 데이터 분리
        x_train, y_train = model_trainer.prepare_train_data(train_data)
        x_test, y_test = model_trainer.prepare_test_data(validation_data)
        x_predcit = model_trainer.prepare_predict_data(predict_data)

        # 모델 학습
        num_iterations = 1
        mae_results = []

        for _ in tqdm(range(num_iterations)):

            # 객체 내 모델 초기화
            model_trainer.model = MLPRegressor(hidden_layer_sizes=model_trainer.layer_size, activation='relu', solver='adam', max_iter=300)

            # 객체 내 모델 훈련
            model_trainer.fitting(x_train, y_train)

            # 객체 내 모델 평가
            # 최소 mae 값 모델 저장
            model_trainer.evaluate(x_test, y_test)
            logger.info('min_mae: {}'.format(model_trainer.current_min_mae))

        # 항로별 학습 결과
        total_test[key_list[i]] = [model_trainer.current_min_mae, model_trainer.model]

        # ==== 업로드를 위한 예측 데이터 생성 ====
        # 훈련된 항로 모델을 활용 해서 예측값 저장
        min_mae_model = total_test[key_list[i]][1]

        # 해상운임 항로를 키로 세팅, 예측값 저장
        prediction_result[i] = min_mae_model.predict(x_predcit)

    # 전체 데이터 데이터프레임으로 변환
    df_prediction = pd.DataFrame(prediction_result)

    # ==== 예측값 날짜 인덱스 추가 작업 ==== #
    df_prediction = date_setting(df_prediction, last_date, key_list)

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


    # 기존데이터와 예측데이터 합치기

    # 기존 데이터 불러오기
    df_raw_data = sea_freight_data

    df_save_data = pd.concat([df_raw_data, df_result_predict], ignore_index=True, join='inner')
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



