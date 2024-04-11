import pandas as pd
import numpy as np
from common.utils.utils import searchAPI, switch_idx_data, logger, make_dates


def scfi_raw_data():

    tmp = searchAPI("dgl_idx_expo_lst")
    # Classify the index (for scfi)
    df = switch_idx_data(tmp)
    df_scfi = df[['rgsr_dt', 'scfi_cach_expo']]
    df_scfi = df_scfi.replace({np.nan: np.nan})

    df_dates = make_dates(min(df_scfi['rgsr_dt']))

    df_scfi_merged = pd.merge(df_scfi, df_dates, on='rgsr_dt', how='outer').sort_values('rgsr_dt')

    return df_scfi_merged
