import matplotlib
import pandas as pd
from pmdarima import auto_arima
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_squared_error, mean_absolute_error
import warnings

# 경고 메시지 무시
warnings.filterwarnings("ignore")
from models.sea_freight.repository import sea_freight_data

# 데이터 로드
data = sea_freight_data
data = data.sort_values(by=['year_mon'])

# 출발지와 도착지별로 데이터 그룹화
routes = data.groupby(['dptr_cnty', 'arvl_cnty'])

# SARIMA 모델의 최적 파라미터를 저장할 딕셔너리
best_params = {}

# 각 항로별로 적절한 ARIMA 파라미터 찾기
for (dptr_cnty, arvl_cnty), route_data in routes:
    print(f"\n항로: {dptr_cnty} -> {arvl_cnty}")

    # cach_amt 데이터 선택
    series = route_data['cach_amt']

    # auto_arima를 사용하여 최적의 파라미터 찾기
    arima_model = auto_arima(series,
                             seasonal=True,
                             m=12,  # 월 단위 계절성 설정
                             start_p=0,
                             start_q=0,
                             max_p=3,
                             max_q=3,
                             d=None,  # 자동으로 차분을 결정
                             D=1,  # 계절성 차분 1로 설정
                             start_P=0,
                             start_Q=0,
                             max_P=2,
                             max_Q=2,
                             trace=False,
                             error_action='ignore',
                             suppress_warnings=True,
                             stepwise=True)

    # 최적의 파라미터 저장
    best_params[(dptr_cnty, arvl_cnty)] = {
        'order': arima_model.order,
        'seasonal_order': arima_model.seasonal_order
    }

# 각 항로에 대해 SARIMA 모델을 학습하고 예측
results = {}

for (dptr_cnty, arvl_cnty), route_data in routes:
    print(f"\n항로: {dptr_cnty} -> {arvl_cnty} - 모델 학습 시작")

    # 학습 및 테스트 데이터 분리
    train_size = int(len(route_data) * 0.8)
    # 학습 및 테스트 데이터 분리 시 인덱스를 초기화
    train = route_data['cach_amt'][:train_size].reset_index(drop=True)
    test = route_data['cach_amt'][train_size:].reset_index(drop=True)

    # SARIMA 모델 구성
    order = best_params[(dptr_cnty, arvl_cnty)]['order']
    seasonal_order = best_params[(dptr_cnty, arvl_cnty)]['seasonal_order']

    # SARIMA 모델 생성 및 학습
    model = SARIMAX(train, order=order, seasonal_order=seasonal_order,
                    enforce_stationarity=False, enforce_invertibility=False)
    model_fit = model.fit(disp=False)

    # 예측
    predictions = model_fit.forecast(steps=len(test))

    # MAE 계산
    mae = mean_absolute_error(test, predictions)
    print(f"항로 ({dptr_cnty} -> {arvl_cnty}) MAE: {mae}")

    # 결과 저장
    results[(dptr_cnty, arvl_cnty)] = {
        'model': model_fit,
        'test': test,
        'predictions': predictions,
        'mae': mae
    }

    # 예측값과 실제값 출력
    length = min(len(test), len(predictions))  # 두 배열의 길이 중 작은 값 사용
    for i in range(1, min(7, length)):  # 최대 6개까지만 출력, 데이터가 부족하면 더 적게 출력
        print(f"실제값: {test.iloc[-i]:.2f}, 예측값: {predictions.iloc[-i]:.2f}")

print("\n모든 항로에 대해 SARIMA 모델 훈련이 완료되었습니다.")

# 12개월 lags를 잡고보면 3개월 예측까지는 괜찮은 결과가 나옴,
# 현재 7월까지 데이터가 적재되어이는것을 감안하면 이번달의 예측값을 제공할 수 있음