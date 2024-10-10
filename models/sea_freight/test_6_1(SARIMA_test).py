import pandas as pd
from pmdarima import auto_arima
import warnings

# 경고 메시지 무시
warnings.filterwarnings("ignore")
from models.sea_freight.repository import sea_freight_data

# 데이터 로드
data = sea_freight_data
data = data.sort_values(by=['year_mon'])

# 출발지와 도착지별로 데이터 그룹화
routes = data.groupby(['dptr_cnty', 'arvl_cnty'])

# SARIMA 파라미터를 저장할 딕셔너리
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
                             trace=True,
                             error_action='ignore',
                             suppress_warnings=True,
                             stepwise=True)

    # 최적의 파라미터 출력 및 저장
    print("Best ARIMA parameters:", arima_model.order)
    print("Best seasonal parameters:", arima_model.seasonal_order)

    best_params[(dptr_cnty, arvl_cnty)] = {
        'order': arima_model.order,
        'seasonal_order': arima_model.seasonal_order
    }

    # 각 항로에 대해 첫 번째 항로만 예시로 출력하고 종료
    break

# 모든 항로에 대해 적절한 파라미터를 찾으려면 break 문을 제거하십시오.
print("\n항로별 최적의 SARIMA 파라미터:", best_params)
