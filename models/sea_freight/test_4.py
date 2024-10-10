import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from tensorflow.keras.optimizers import Adam

from models.sea_freight.repository import sea_freight_data

# 데이터 로드
data = sea_freight_data
data = data.sort_values(by=['year_mon'])

# 결과 저장을 위한 딕셔너리
results = {}

# 항로별로 그룹화
routes = data.groupby(['dptr_cnty', 'arvl_cnty'])

# 과거 데이터의 길이와 예측할 기간 설정
lags = 12
for_period = 3

# 각 항로별로 LSTM 모델 학습
for (dptr_cnty, arvl_cnty), route_data in routes:
    # 정규화
    scaler = MinMaxScaler()
    route_data['cach_amt'] = scaler.fit_transform(route_data[['cach_amt']])

    # 입력 데이터와 타깃 데이터 준비
    X, y = [], []
    for i in range(lags, len(route_data) - for_period):
        X.append(route_data['cach_amt'].values[i - lags:i])  # lags 기간의 데이터
        y.append(route_data['cach_amt'].values[i + for_period])  # for_period 시점의 값

    # numpy 배열로 변환
    X, y = np.array(X), np.array(y)

    # LSTM 입력 형식에 맞게 데이터 차원 조정 (samples, timesteps, features)
    X = X.reshape((X.shape[0], X.shape[1], 1))

    # 모델 구성
    model = Sequential()
    model.add(LSTM(50, activation='relu', input_shape=(X.shape[1], 1)))
    model.add(Dense(1))
    model.compile(optimizer=Adam(learning_rate=0.001), loss='mse')

    # 데이터 분할 (훈련 및 테스트)
    train_size = int(len(X) * 0.8)
    X_train, X_test = X[:train_size], X[train_size:]
    y_train, y_test = y[:train_size], y[train_size:]

    # 모델 학습
    print(f"훈련 중 - 출발: {dptr_cnty}, 도착: {arvl_cnty}")
    model.fit(X_train, y_train, epochs=50, batch_size=16, validation_data=(X_test, y_test), verbose=0)

    # 예측
    y_pred = model.predict(X_test)

    # 예측값을 원래 스케일로 복원
    y_test_inv = scaler.inverse_transform(y_test.reshape(-1, 1))
    y_pred_inv = scaler.inverse_transform(y_pred)

    # 결과 저장
    results[(dptr_cnty, arvl_cnty)] = {
        'model': model,
        'y_test': y_test_inv,
        'y_pred': y_pred_inv
    }

    # 각 항로에 대한 예측 결과 출력
    print(f"출발지: {dptr_cnty}, 도착지: {arvl_cnty}")
    for i in range(3):  # 처음 3개의 예측값과 실제값을 출력
        print(f"실제값: {y_test_inv[i][0]:.2f}, 예측값: {y_pred_inv[i][0]:.2f}")

print("모든 항로에 대해 모델 훈련이 완료되었습니다.")
