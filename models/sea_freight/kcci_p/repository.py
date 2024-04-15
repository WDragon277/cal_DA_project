import pandas as pd
import numpy as np
from common.utils.utils import searchAPI, switch_idx_data, logger, make_dates

from common.utils.setting import EsSetting

esinfo = EsSetting()

def kcci_raw_data():

    tmp = searchAPI(esinfo.sea_read_index)
    # Classify the index (for kcci)
    df = switch_idx_data(tmp)
    df_kcci = df[['rgsr_dt', 'kcci_cach_expo']]
    df_kcci = df_kcci.replace({np.nan: np.nan})

    df_dates = make_dates(min(df_kcci['rgsr_dt']))

    df_kcci_merged = pd.merge(df_kcci, df_dates, on='rgsr_dt', how='outer').sort_values('rgsr_dt')

    return df_kcci_merged
