from sklearn.preprocessing import MinMaxScaler
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_absolute_error
from models.sea_freight.repository import sea_freight_data

# 데이터 로드
data = sea_freight_data
data = data.sort_values(by=['dptr_cnty', 'arvl_cnty', 'year_mon'])

# 결과 저장을 위한 딕셔너리
results = {}

# 출발지와 도착지별로 그룹화
routes = data.groupby(['dptr_cnty', 'arvl_cnty'])

# SARIMA 모델 파라미터 설정
p, d, q = 3, 1, 3  # ARIMA 파라미터
P, D, Q, s = 1, 1, 1, 12  # 계절성 파라미터 (s=12는 12개월 계절성 가정)

# 각 항로별로 SARIMA 모델 학습
for (dptr_cnty, arvl_cnty), route_data in routes:
    # 정규화
    scaler = MinMaxScaler()
    route_data['cach_amt'] = scaler.fit_transform(route_data[['cach_amt']])
    route_data = route_data.set_index('year_mon')
    # 학습 및 테스트 데이터 분리
    train_size = int(len(route_data) * 0.9)
    train, test = route_data['cach_amt'][:train_size], route_data['cach_amt'][train_size:]

    # SARIMA 모델 학습
    print(f"훈련 중 - 출발: {dptr_cnty}, 도착: {arvl_cnty}")
    model = SARIMAX(train, order=(p, d, q), seasonal_order=(P, D, Q, s), enforce_stationarity=False,
                    enforce_invertibility=False)
    model_fit = model.fit(disp=False)

    # 예측
    predictions = model_fit.forecast(steps=len(test))

    # 예측값을 원래 스케일로 복원
    test_inv = scaler.inverse_transform(test.values.reshape(-1, 1))
    predictions_inv = scaler.inverse_transform(predictions.values.reshape(-1, 1))

    # RMSE 계산
    mae = mean_absolute_error(test_inv, predictions_inv)
    print(f"항로 ({dptr_cnty} -> {arvl_cnty}) MAE: {mae}")

    # 결과 저장
    results[(dptr_cnty, arvl_cnty)] = {
        'model': model_fit,
        'date': test.index,
        'test': test_inv,
        'predictions': predictions_inv,
        'rmse': mae
    }

    # 각 항로에 대한 예측 결과 출력 (처음 5개)
    print(f"출발지: {dptr_cnty}, 도착지: {arvl_cnty} 예측 결과")
    for i in range(1, 7):
        print(f"날짜: {test.index[i]} 실제값: {test_inv[i][0]:.2f}, 예측값: {predictions_inv[i][0]:.2f}")
    print("\n")

    # 각 항로에 대한 예측 결과를 확인하기 위해 첫 번째 항로만 실행
    # break

print("모든 항로에 대해 SARIMA 모델 훈련이 완료되었습니다.")
