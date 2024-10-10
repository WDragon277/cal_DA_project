
import pandas as pd
from prophet import Prophet
from sklearn.metrics import mean_squared_error
import warnings

warnings.filterwarnings("ignore")

from models.sea_freight.repository import sea_freight_data

# 데이터 로드
data = sea_freight_data

data['year_mon'] = pd.to_datetime(data['year_mon'], format='%Y%m')
data.set_index('year_mon', inplace=True)

# 항로별 데이터 분할
routes = data.groupby(['dptr_cnty', 'arvl_cnty'])

# 결과 저장을 위한 리스트
results = []

for (dptr_cnty, arvl_cnty), route_data in routes:
    # Prophet은 'ds'와 'y'라는 컬럼명을 사용하므로 데이터프레임 변환
    df = route_data.reset_index().rename(columns={'year_mon': 'ds', 'cach_amt': 'y'})

    # 모델 정의 및 학습
    model = Prophet(daily_seasonality=False, yearly_seasonality=True, weekly_seasonality=False)
    model.fit(df)

    # 미래 예측 기간 설정 (12개월 예측)
    future = model.make_future_dataframe(periods=12, freq='MS')

    # 예측 수행
    forecast = model.predict(future)

    # 훈련 데이터에 대한 예측값 비교
    train_predictions = forecast[['ds', 'yhat']].iloc[:len(df)]
    train_predictions['actual'] = df['y'].values  # 실제 값 추가
    rmse = mean_squared_error(train_predictions['actual'], train_predictions['yhat'], squared=False)

    # 비교 결과를 데이터프레임 형태로 저장
    comparison_df = train_predictions[['ds', 'yhat', 'actual']]
    comparison_df.columns = ['날짜', '예측값', '실제값']

    # 결과 저장
    results.append({
        '항로': f"{dptr_cnty} -> {arvl_cnty}",
        'RMSE': rmse,
        '예측과 실제값 비교': comparison_df.tail(5).values,  # 마지막 5개 예측 및 실제값 비교
    })

# 결과를 데이터프레임으로 변환하여 확인
results_df = pd.DataFrame(results)
print(results_df)