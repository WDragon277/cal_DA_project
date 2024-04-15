import pandas as pd
import numpy as np
import sys

from common.utils.utils import logger,searchAPI,switch_idx_data, interpolation
from sklearn.ensemble import GradientBoostingRegressor
from models.sea_freight.hrci_p.repository import hrci_redifined_data, shifted_data

def pred_hrci_model():

    # 원본데이터와 이동된 데이터 불러오기
    data = hrci_redifined_data()
    df_moved = shifted_data(data)

    # 이동 데이터 날짜를 원래대로 변경하기

    # 날짜 형식 맞춰 주기
    data['rgsr_dt'] = data['rgsr_dt'].apply(lambda x: x.strftime('%Y%m%d'))
    df_moved['rgsr_dt'] = df_moved['rgsr_dt'].apply(lambda x: x.strftime('%Y%m%d'))

    # 날짜를 인덱스로 설정
    data = data.set_index('rgsr_dt')
    df_moved = df_moved.set_index('rgsr_dt')

    # 데이터 정제를 위한 카피데이터 생성
    # data_copy = data.copy()
    data_copy = data.drop(['hrci_cach_expo'], axis=1).copy()

    # 두 데이터프레임 Join 및 na값 삭제
    train_data = pd.merge(data_copy, df_moved, left_index=True, right_index=True, how='outer')
    train_data = train_data.dropna(axis=0)

    # 훈련을 위한 변수와 라벨 분리
    X = train_data[['ccfi_cach_expo', 'scfi_cach_expo']]
    y = train_data[['hrci_cach_expo']]

    # 훈련과 테스트의 범위를 나눠서 세팅
    X_train = X[:-14]
    X_pred = X[-14:]
    y_train = y[:-14]

    # 모델 훈련 및 예측값 출력
    model = GradientBoostingRegressor()
    model.fit(X_train, y_train)
    y_pred = model.predict(X_pred)

    # 예측 값을 기존의 데이터와 합치기 위한 작업
    result_df = pd.DataFrame(data['hrci_cach_expo'])
    last_index_date = pd.to_datetime(data.index[-1])

    for row_data in y_pred:
        last_index_date = last_index_date + pd.DateOffset(days=1)
        new_row = last_index_date.strftime('%Y%m%d')
        result_df.loc[new_row] = row_data

    return result_df


# sys.path.append('C:\\Users\\0614_\\PycharmProjects\\DG3_Ocean_Index\\model\\hrci_p')
# sys.path.append('c:\\users\\0614_\\anaconda3\\envs\\dg3_ocean_index\\lib\\site-packages')

# def pred_hrci_model():
#     # 데이터 호출
#     df = searchAPI("dgl_idx_expo_lst")
#     logger.info('데이터 베이스에서 데이터를 불러오는데 성공했습니다.')
#
#     # 코드 데이터 칼럼화
#     df_total = switch_idx_data(df)
#     df_total = df_total.drop(['bdi_cach_expo','kcci_cach_expo'],axis = 1)
#
#     # 모델 작동을 위한 데이터 정제
#     data, shifted_data, shift_count = hrci_redifined_data()
#
#     #인덱스 설정
#     data = data.set_index('rgsr_dt')
#
#     # 인덱스
#     non_nan_indices = data[data['hrci_cach_expo'].notna()].index
#     last_index = non_nan_indices[-1]
#
#     # 이동된 데이터의 마지막 값의 인덱스
#     non_nan_indices_shfted = shifted_data[:shift_count].index
#     last_non_nan_index = non_nan_indices_shfted[-1]
#
#     # 훈련과 타겟용 데이터 세팅
#     X = np.array(data[['ccfi_cach_expo','scfi_cach_expo']])
#     y = np.array(shifted_data['hrci_cach_expo'].dropna())
#
#     # 훈련과 테스트의 범위를 나눠서 세팅
#     X_train = X[:last_non_nan_index-1]
#     X_pred = X[last_non_nan_index-1:]
#     y_train = y[:last_non_nan_index]
#
#     # 모델 훈련 및 예측값 출력
#     model = GradientBoostingRegressor()
#     model.fit(X_train, y_train)
#     y_pred = model.predict(X_pred)
#
#     # shift_count를 바탕으로 데이터 추가로 몇개 생성할 것인지 판단하는 코드 추가(?)
#     # working_days = 5
#     # pred_first_week = y_pred[:4]
#     # pred_second_week = y_pred[:4+working_days]
#
#     result_df = pd.DataFrame(data['hrci_cach_expo'])
#     last_index_date = pd.to_datetime(last_index)
#
#     for row_data in y_pred:
#         last_index_date = last_index_date + pd.DateOffset(days=1)
#         new_row = last_index_date.strftime('%Y%m%d')
#         result_df.loc[new_row] = row_data
#
#     return result_df

if __name__=='__main__':
    result = pred_hrci_model()
    print(result)