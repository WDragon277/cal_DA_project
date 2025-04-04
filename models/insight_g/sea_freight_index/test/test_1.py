import pandas as pd
import numpy as np
from common.utils.utils import searchAPI, switch_idx_data

from common.utils.setting import EsSetting

esinfo = EsSetting()

def bdi_raw_data():
    tmp = searchAPI(esinfo.sea_read_index)
    df = switch_idx_data(tmp)
    df_bdi = df[['rgsr_dt','bdi_cach_expo']]
    df_bdi2 = df_bdi.replace({np.nan: np.nan})
    return df_bdi

tmp = bdi_raw_data()