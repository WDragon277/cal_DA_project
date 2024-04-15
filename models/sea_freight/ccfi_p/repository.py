import pandas as pd
import numpy as np
from common.utils.utils import searchAPI, switch_idx_data, logger, make_dates

from common.utils.setting import EsSetting

esinfo = EsSetting()

def ccfi_raw_data():

    tmp = searchAPI(esinfo.sea_read_index)
    # Classify the index (for ccfi)
    df = switch_idx_data(tmp)
    df_ccfi = df[['rgsr_dt', 'ccfi_cach_expo']]
    df_ccfi = df_ccfi.replace({np.nan: np.nan})

    df_dates = make_dates(min(df_ccfi['rgsr_dt']))

    df_ccfi_merged = pd.merge(df_ccfi, df_dates, on ='rgsr_dt', how = 'outer').sort_values('rgsr_dt')

    return df_ccfi_merged
