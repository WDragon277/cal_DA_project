import pandas as pd
import numpy as np
from statsmodels.tsa.statespace.sarimax import SARIMAX
import matplotlib.pyplot as plt

# 예제용 데이터 생성: 2020년 1월부터 2022년 12월까지의 월별 데이터
dates = pd.date_range(start='2020-01-01', end='2022-12-01', freq='MS')
data = np.random.rand(len(dates)) * 100  # 랜덤 데이터 생성
series = pd.Series(data, index=dates)

# 훈련 데이터: 2020년 1월부터 2022년 12월까지
train = series

# SARIMA 모델 구성 및 학습
model = SARIMAX(train, order=(1, 1, 1), seasonal_order=(1, 1, 1, 12), enforce_stationarity=False, enforce_invertibility=False)
model_fit = model.fit(disp=False)

# 12개월 동안의 예측 (2023년 1월 ~ 2023년 12월)
steps = 12
predictions = model_fit.forecast(steps=steps)

# 예측된 값을 그래프로 시각화
plt.figure(figsize=(10, 6))
plt.plot(series.index, series, label='훈련 데이터')
plt.plot(pd.date_range(start='2023-01-01', periods=steps, freq='MS'), predictions, label='예측 값', color='orange')
plt.title('SARIMA 모델을 사용한 미래 예측')
plt.xlabel('날짜')
plt.ylabel('값')
plt.legend()
plt.show()
