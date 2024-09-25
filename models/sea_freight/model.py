# 항로별 데이터를 활용해 모델 생성하고 검토하는 for 문
# 모델을 여러번 생성하고 MAE 값을 측정해 p-value와 신뢰도 구간을 확인
# 최소값을 가지는 모델을 저장함
from tqdm import tqdm
import logging
logger = logging.getLogger(__name__)
import numpy as np
import pandas as pd
from scipy import stats

from sklearn.neural_network import MLPRegressor
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error

from common.utils.utils import searchAPI, switch_idx_data
from common.utils.setting import EsSetting

esinfo = EsSetting()

sea_freight_data = searchAPI(esinfo.sea_read_freight)

by_sea_route_tables = {}
key_list = []

# 학습용 데이터 생성
# key가 되는 컬럼(data_cd, dptr_cnty, arvl_cnty)으로 groupby 시행
for key, group in sea_freight_data.groupby(['data_cd', 'dptr_cnty', 'arvl_cnty']):

  # key별 데이터(group) year_mon로 오름차순 정렬 및 딕셔너리에 입력
  by_sea_route_tables[key] = group.sort_values(by='year_mon')
  # Lisy에 key 저장
  key_list.append(key)

total_test = {}
total_residual = {}

for i in tqdm(range(len(key_list))):
  try:
    # 항로 별로 나뉜 value 하나씩 불러오기
    data = by_sea_route_tables[key_list[i]]
    logger.info('key: {}'.format(key_list[i]))

    # 데이터 인덱스 재정렬
    data = data.reset_index(drop=True)

    # 과거 학습데이터 기간
    lags = 12
    # 예측 기간
    for_period = 12
    # 모델 테스트(검증) 기간
    test_period = 12
    # 검증용 데이터 크기
    validation_size = lags + for_period + test_period

    logger.info(f'trained_period: {lags} month')
    logger.info(f'for_prediction: {for_period} month')
    logger.info(f'test_period: {test_period} month')
    logger.info(f'validation_size: {validation_size} month')

    # 학습용, 검증용 데이터 분할하기
    train_data = data[:-validation_size]
    validation_data = data['cach_amt'][-validation_size:]

    # 모델 학습 횟수
    num_iterations = 30
    logger.info(f'Model trainning counts: {num_iterations}')

    # 결과 저장을 위한 리스트
    rmse_results = []
    mae_results = []
    residual = []

    # 모델 학습과 검증 반복으로 신뢰 구간 확인(파라미터 개선을 위한 성능 지표)
    for _ in tqdm(range(num_iterations)):

        # 모델 초기화
        layer_sizes = (50,50,100,50,100)
        model = MLPRegressor(hidden_layer_sizes = layer_sizes, activation='relu', solver='adam', max_iter=300)
        logger.info(f'layer_sizes: {layer_sizes}')

        # 훈련 데이터를 사용하여 모델 학습
        # MLPR 모델은 과거 값들을 입력으로 받아 미래 값을 예측합니다.
        # 따라서, 훈련 데이터를 입력과 출력으로 분리해야 합니다.
        X_train = []
        y_train = []

        # 훈련을 위해 2D 데이터로 데이터 분할
        for train_num in range(lags, len(train_data) - for_period):
            X_train.append(train_data['cach_amt'].iloc[(train_num - lags):train_num]) # lag 값 만큼의 학습 데이터(12개월)
            y_train.append(train_data['cach_amt'].iloc[(train_num + for_period - 1)]) # 학습의 타깃값
        X_train = np.array(X_train)
        y_train = np.array(y_train)
        model.fit(X_train, y_train)

        # 테스트 데이터 예측
        X_test = []
        y_test = []
        for val_num in range(lags, len(validation_data) - for_period):
            X_test.append(validation_data.iloc[(val_num - lags):val_num])
            y_test.append(validation_data.iloc[(val_num + for_period - 1)])

        X_test = np.array(X_test)
        y_test = np.array(y_test)
        predictions = model.predict(X_test)

        # MAE 계산
        mae = mean_absolute_error(y_test, predictions)
        mae_results.append(mae)

        # MAE 값이 최소면 모델 저장
        if len(mae_results) == 1:
            current_min_mae = mae
            min_model = model
        elif mae < current_min_mae:
            current_min_mae = mae
            min_model = model
        else:
          pass

    # 신뢰 수준 (예: 95%)
    confidence_level = 0.95

    # 자유도
    degrees_of_freedom = num_iterations - 1

    # MAE t-테스트 수행
    t_statistic_mae, p_value_mae = stats.ttest_1samp(mae_results, popmean=0)

    # MAE 신뢰구간 계산
    confidence_interval_mae = stats.t.interval(confidence_level, degrees_of_freedom,
                                          np.mean(mae_results),
                                          stats.sem(mae_results))
    # 최소 MAE 값
    # min_mae = scaler.inverse_transform(min(mae_results)) # 정규화 데이터 원상복구
    min_mae = min(mae_results)

    # 기초 통계량(평균, 분산)
    mean_data = np.mean(data['cach_amt'])
    std_data = np.std(data['cach_amt'])

    total_test[key_list[i]] = [mean_data, std_data, p_value_mae, confidence_interval_mae,
                               min_mae, min_model]

  except:
    print('error in', key_list[i])
    continue


print('\n',total_test)