import pandas as pd
import matplotlib.pyplot as plt
from models.sea_freight.repository import sea_freight_data

# 데이터 로드
data = sea_freight_data

# 'year_mon'을 인덱스로 설정하여 시계열 데이터로 변환
data['year_mon'] = pd.to_datetime(data['year_mon'], format='%Y%m')
data.set_index('year_mon', inplace=True)

# 출발지와 도착지별로 그룹화
routes = data.groupby(['dptr_cnty', 'arvl_cnty'])

# 각 항로별 시계열 데이터 시각화
for (dptr_cnty, arvl_cnty), route_data in routes:
    plt.figure(figsize=(10, 5))
    plt.plot(route_data['cach_amt'], label=f'{dptr_cnty} -> {arvl_cnty} 항로')
    plt.title(f'{dptr_cnty} -> {arvl_cnty} 항로의 해상 운임 데이터')
    plt.xlabel('연월')
    plt.ylabel('운임 금액')
    plt.legend()
    plt.show()

    # 모든 항로를 시각화하지 않으려면 break를 사용
    break
