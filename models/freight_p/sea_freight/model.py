import logging

import pandas as pd

logger = logging.getLogger(__name__)
import numpy as np

from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.neural_network import MLPRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.preprocessing import StandardScaler, MinMaxScaler

scaler = StandardScaler()

class MLPRModel:
    def __init__(self, lags=6, for_period=6, test_period=6, layer_size=(50, 50, 100, 50, 100)):
        self.lags = lags
        self.for_period = for_period
        self.test_period = test_period
        self.layer_size = layer_size
        self.validation_size = for_period + lags + test_period
        self.model = MLPRegressor(hidden_layer_sizes=layer_size, activation='relu', solver='adam', max_iter=3000)
        self.mae_result = []
        self.min_model = None
        self.current_min_mae = float('inf')

    def data_split(self, data):
        # data = data[-36:]
        data[['cach_amt']] = scaler.fit_transform(data[['cach_amt']])
        train_data = data['cach_amt'][:-self.validation_size]
        validation_data = data['cach_amt'][-self.validation_size:]
        predict_data = data['cach_amt'][-self.for_period - self.lags:]
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


class SARIMAmodel:
    def __init__(self):
        self.results = {}  # 결과 저장을 위한 딕셔너리

        # ARIMA 파라미터
        self.p, self.d, self.q = 6, 1, 6
        # 계절성 파라미터 (s=12는 12개월 계절성 가정)
        self.P, self.D, self.Q, self.s = 1, 1, 1, 12

        self.forcast = 3
        self.scaler = MinMaxScaler()

        # 하나의 항로의 운임데이터 SARIMA 모델 학습

    def model_fitting(self, route_data):
        # 정규화
        route_data['cach_amt'] = self.scaler.fit_transform(route_data[['cach_amt']])
        x_train = route_data['cach_amt']

        # SARIMA 모델 학습
        model = SARIMAX(x_train, order=(self.p, self.d, self.q),
                        seasonal_order=(self.P, self.D, self.Q, self.s),
                        enforce_stationarity=False,
                        enforce_invertibility=False)
        model_fit = model.fit(disp=False)

        return model_fit

    def prediction(self, model_fit):
        # 예측
        predictions = model_fit.forecast(steps=self.forcast)

        # 예측값을 원래 스케일로 복원
        predictions_inv = self.scaler.inverse_transform(predictions.values.reshape(-1, 1))
        predictions_inv = np.round(predictions_inv, 2)

        predictions_inv = pd.DataFrame(predictions_inv)

        return predictions_inv