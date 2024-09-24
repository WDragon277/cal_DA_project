import pandas as pd
import numpy as np
from common.utils.utils import searchAPI, switch_idx_data

from common.utils.setting import EsSetting

esinfo = EsSetting()

def bdi_raw_data():
    tmp = searchAPI(esinfo.sea_read_index)
    df = switch_idx_data(tmp)
    df_bdi = df[['rgsr_dt','bdi_cach_expo']]
    df_bdi = df_bdi.replace({np.nan: np.nan})
    return df_bdi


def usdx_raw_data():
    tmp = searchAPI(esinfo.usd_read_index)
    df_usdx = tmp[['sbmt_dt','lprc']].copy()
    df_usdx['sbmt_dt'] = pd.to_datetime(df_usdx['sbmt_dt'])
    df_usdx['sbmt_dt'] = df_usdx['sbmt_dt'].dt.strftime('%Y%m%d')
    df_usdx.columns = ['rgsr_dt','lprc']
    return df_usdx


def raw_maters_price():
    tmp = searchAPI(esinfo.pds_read_index)
    copper_price = tmp[tmp['data_cd'] == '11'][['ref_dt','prlst_amt']]
    dubai_crud = tmp[tmp['data_cd'] == '01'][['ref_dt','prlst_amt']]

    copper_price['ref_dt'] = pd.to_datetime(copper_price['ref_dt'])
    dubai_crud['ref_dt'] = pd.to_datetime(dubai_crud['ref_dt'])
    copper_price['ref_dt'] = copper_price['ref_dt'].dt.strftime('%Y%m%d')
    dubai_crud['ref_dt'] = dubai_crud['ref_dt'].dt.strftime('%Y%m%d')

    copper_price.columns = ['rgsr_dt', 'copper_price']
    dubai_crud.columns = ['rgsr_dt', 'dubai_price']

    raw_mater_price = pd.merge(copper_price,dubai_crud, how = 'outer',on = 'rgsr_dt')
    return raw_mater_price.sort_values(by = 'rgsr_dt')


# 일자별 기준으로 데이터 모두 머지해서 활용
# def bdi_total_data():
def raw_data_merge(bdi_raw_data, usdx_raw_data, raw_maters_price):

    tmp = pd.merge(usdx_raw_data, raw_maters_price, how = 'outer', on = 'rgsr_dt')
    tmp2 = pd.merge(tmp,bdi_raw_data, how = 'outer', on = 'rgsr_dt')
    tmp2 = tmp2.sort_values('rgsr_dt')
    merged_raw_data = tmp2.reset_index(drop=True)

    return merged_raw_data