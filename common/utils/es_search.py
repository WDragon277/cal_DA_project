import pandas as pd

from common.utils.utils import searchAPI
from common.utils.setting import EsSetting


esinfo = EsSetting()

raw_data = searchAPI('cal_idx_koreapds_lst')

tmp = pd.DataFrame(raw_data)
tmp.to_csv('C:\\workspace\\cal_idx_koreapds_lst.csv')