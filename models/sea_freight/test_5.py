import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error
from models.sea_freight.repository import sea_freight_data

# 데이터 로드
data = sea_freight_data
data = data.sort_values(by=['year_mon'])

# 결과 저장을 위한 딕셔너리
results = {}

# 항로별로 그룹화
routes = data.groupby(['dptr_cnty', 'arvl_cnty'])

# 모델 파라미터 설정 (예시로 설정, 각 항로에 최적화 가능)
p, d, q = 5, 1, 0

# 각 항로별로 ARIMA 모델 학습
for (dptr_cnty, arvl_cnty), route_data in routes:
    # 정규화
    scaler = MinMaxScaler()
    route_data['cach_amt'] = scaler.fit_transform(route_data[['cach_amt']])

    # 학습 및 테스트 데이터 분리
    train_size = int(len(route_data) * 0.8)
    train, test = route_data['cach_amt'][:train_size], route_data['cach_amt'][train_size:]

    # ARIMA 모델 학습
    print(f"훈련 중 - 출발: {dptr_cnty}, 도착: {arvl_cnty}")
    model = ARIMA(train, order=(p, d, q))
    model_fit = model.fit()

    # 예측
    predictions = model_fit.forecast(steps=len(test))

    # 예측값을 원래 스케일로 복원
    test_inv = scaler.inverse_transform(test.values.reshape(-1, 1))
    predictions_inv = scaler.inverse_transform(predictions.values.reshape(-1, 1))

    # RMSE 계산
    rmse = mean_squared_error(test_inv, predictions_inv, squared=False)
    print(f"항로 ({dptr_cnty} -> {arvl_cnty}) RMSE: {rmse}")

    # 결과 저장
    results[(dptr_cnty, arvl_cnty)] = {
        'model': model_fit,
        'test': test_inv,
        'predictions': predictions_inv,
        'rmse': rmse
    }

print("모든 항로에 대해 ARIMA 모델 훈련이 완료되었습니다.")

# 모든 항로의 예측값과 실제값을 출력
for (dptr_cnty, arvl_cnty), result in results.items():
    print(f"\n항로 ({dptr_cnty} -> {arvl_cnty}) 예측 결과")
    test_values = result['test']
    predictions = result['predictions']

    # 처음 5개의 예측값과 실제값을 출력
    for i in range(min(5, len(test_values))):
        print(f"실제값: {test_values[i][0]:.2f}, 예측값: {predictions[i][0]:.2f}")

    # RMSE 출력
    print(f"RMSE: {result['rmse']:.2f}")
