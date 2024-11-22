import pandas as pd
import datetime
from common.utils.utils import interpolation, df_corr_hrci, logger
from common.utils.utils import searchAPI, switch_idx_data, logger, make_dates

from common.utils.setting import EsSetting

esinfo = EsSetting()

def hrci_redifined_data():

    tmp = searchAPI(esinfo.sea_read_index)

    # Classify the index (for ccfi)
    df_total = switch_idx_data(tmp)

    # 각 지수로 분리된 데이터의 타입을 숫자형으로 변환시킴
    df_total['ccfi_cach_expo'] = pd.to_numeric(df_total['ccfi_cach_expo'])
    df_total['hrci_cach_expo'] = pd.to_numeric(df_total['hrci_cach_expo'])
    df_total['scfi_cach_expo'] = pd.to_numeric(df_total['scfi_cach_expo'])

    # HRCI에서 첫 번째와 마지막 Nan 값의 인덱스
    non_nan_indices = df_total[df_total['hrci_cach_expo'].notna()].index
    first_non_nan_index = non_nan_indices[0]
    last_non_nan_index = non_nan_indices[-1]

    # HRCI이 존재 하는 데이터 구간 및 칼럼 분리
    sliced_df = df_total.loc[first_non_nan_index:last_non_nan_index]
    sliced_df = sliced_df[['rgsr_dt', 'scfi_cach_expo', 'hrci_cach_expo', 'ccfi_cach_expo']]

    # 보간법 적용
    df_interpolated = interpolation(sliced_df)
    df_interpolated_filled = df_interpolated.fillna(method='bfill')
    df_interpolated_filled = df_interpolated_filled.dropna(axis=0)

    # 날짜 타입으로 변경
    df_interpolated_filled['rgsr_dt'] = pd.to_datetime(
                            df_interpolated_filled['rgsr_dt'], format='%Y%m%d')

    logger.info('보간법이 적용되었습니다.')


    return df_interpolated_filled

# 날짜 이동 함수 만들기 in = (SCFI, CCFI) out = (SCFI, CCFI)

def shifted_data(df):
    # 가장 상관도가 높은 기준 날짜로 이동된 데이터 프레임
    tmp_df = df.copy()
    # 30일 이내 상관도가 가장 높은 이동 일자 구함
    shifted_num = df_corr_hrci(tmp_df)[0]

    # 상관도가 가장 높은 이동 일자를 적용한 데이터프레임을 생성
    # df_shifted = df_interpolated_filled.shift(shifted_num)
    tmp_df['rgsr_dt'] = tmp_df['rgsr_dt'].\
        apply(lambda x: x + pd.DateOffset(days=shifted_num))
    logger.info(f'데이터의 기준 날짜가 변경되었습니다.{shifted_num}일')
    # tmp_df['rgsr_dt'] = tmp_df['rgsr_dt'].apply(lambda x) x.strtime('%Y%m%d')

    df_moved = tmp_df[['rgsr_dt', 'hrci_cach_expo']]


    logger.info('데이터분석에 사용될 데이터프레임이 생성 되었습니다.')
    return df_moved

# 두 데이터 프레임을 일자를 기준으로 Join 하고 행 null 값은 삭제하는 함수

# def hrci_merged_data():



if __name__=='__main__':
    df_interpolated = hrci_redifined_data()