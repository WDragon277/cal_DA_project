from common.utils.utils import searchAPI
from common.utils.setting import EsSetting
es = EsSetting()
idx = es.sea_save_freight
tmp = searchAPI(idx)
tmp = tmp.sort_values(['data_cd','dptr_cnty','arvl_cnty','year_mon'])