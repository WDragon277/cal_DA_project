import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import warnings
warnings.filterwarnings("ignore")

from models.sea_freight.repository import sea_freight_data

# 데이터 로드
data = sea_freight_data
data['year_mon'] = pd.to_datetime(data['year_mon'], format='%Y%m')
data.set_index('year_mon', inplace=True)

# 항로별로 데이터 분할 및 예측 수행
routes = data.groupby(['dptr_cnty', 'arvl_cnty'])

# 결과 저장을 위한 리스트
results = []

for (dptr_cnty, arvl_cnty), route_data in routes:
    # 입력 데이터(X)와 타깃(y) 설정
    X = route_data.index.month.values.reshape(-1, 1)  # 월을 예측 인자로 사용
    y = route_data['cach_amt'].values

    # 학습 데이터와 테스트 데이터로 분할 (80:20 비율)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

    # XGBoost 회귀 모델 정의 및 학습
    model = XGBRegressor(
        objective='reg:squarederror',
        n_estimators=500,  # 트리 개수
        learning_rate=0.01,  # 학습률
        max_depth=3,  # 최대 깊이
        subsample=0.8,  # 서브샘플링 비율
        colsample_bytree=0.8,  # 컬럼 서브샘플링 비율
        reg_lambda=1,  # L2 정규화
        reg_alpha=0  # L1 정규화
    )
    model.fit(X_train, y_train)

    # 예측 수행
    y_pred = model.predict(X_test)

    # 음수 값을 0으로 클리핑
    y_pred_clipped = np.clip(y_pred, 0, None)

    # 모델 성능 평가 (RMSE)
    rmse = mean_squared_error(y_test, y_pred_clipped, squared=False)

    # 결과 저장
    results.append({
        '항로': f"{dptr_cnty} -> {arvl_cnty}",
        'RMSE': rmse,
        '예측값': y_pred_clipped[:-5],  # 처음 5개 예측값 예시로 저장
        '실제값': y_test[:-5]           # 처음 5개 실제값 예시로 저장
    })

# 결과를 데이터프레임으로 변환하여 확인
results_df = pd.DataFrame(results)
print(results_df)
