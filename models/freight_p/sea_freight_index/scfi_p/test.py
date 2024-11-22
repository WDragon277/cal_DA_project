import pandas as pd
from statsmodels.tsa.ar_model import AutoReg
from models.scfi_p.repository import scfi_raw_data
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score
import matplotlib.pyplot as plt

# 데이터 불러오기
df_data = scfi_raw_data()
df_data['rgsr_dt'] = pd.to_datetime(df_data['rgsr_dt'])
df_data = df_data.set_index('rgsr_dt')
# df_data = df_data[:'20230621']

df_data = df_data.astype(str)
df_data = df_data.applymap(lambda x: int(x) if x.isdigit() else float(x))

# 최신 데이터 부분만 선택
# df_data = df_data[int(len(df_data)*0.6):]
df_data = df_data[int(len(df_data)*0.8):]

interpolated_data = df_data.interpolate()
defined_data = interpolated_data.dropna()

# 학습과 테스트를 위한 데이터 분리
train_size = int(len(defined_data) * 0.8)
train_data, test_data = defined_data[:train_size], defined_data[train_size:]
train_data.index = pd.DatetimeIndex(train_data.index).to_period('D')
test_data.index = pd.DatetimeIndex(test_data.index).to_period('D')

# 일부 데이터로 학습
lag = 60

model = AutoReg(train_data['scfi_cach_expo'], lags=lag)
model_fit = model.fit()

predictions = model_fit.predict(start=len(train_data), end=len(train_data) + len(test_data) - 1)

RMSE = mean_squared_error(test_data['scfi_cach_expo'], predictions)
r2 = r2_score(test_data['scfi_cach_expo'], predictions)

print('rmse score: ', RMSE)
print('r2 score: ', r2)

# 그림으로 예측구간과 실제 구간확인

test_date = test_data.index.to_timestamp()

plt.figure(figsize=(10, 6))
plt.scatter(test_date, predictions, color='blue', label='Predicted')
plt.plot(test_date, test_data['scfi_cach_expo'], color='red', linewidth=2, label='Actual')
# plt.title(f'Linear Regression\nR-squared: {r2:.2f}')
plt.title(f'Linear Regression\nSCFI_Prediction\nlag={lag}\nR-squared: {r2:.3f}')
plt.xlabel('X')
plt.ylabel('y')
plt.legend()