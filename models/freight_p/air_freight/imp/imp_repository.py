from elasticsearch import Elasticsearch
import logging
import pandas as pd
import numpy as np
import seaborn as sns
from matplotlib import pyplot as plt

from common.utils.utils import searchAPI
from common.utils.setting import EsSetting

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

esinfo = EsSetting()
raw_data = searchAPI(esinfo.air_read_index)
imp_data = raw_data[(raw_data.data_cd == 'kcla_imp_air')]

# 코드 데이터
allColumns = list(raw_data.columns.values)
cach_columns = ['year_mon','cach_45k_amt','cach_100k_amt','cach_300k_amt','cach_500k_amt','cach_1000k_amt']
only_cach_columns = ['cach_45k_amt','cach_100k_amt','cach_300k_amt','cach_500k_amt','cach_1000k_amt']

# 미주 공항의 항공운임 데이터 생성
la_imp_data = imp_data[(imp_data['dptr_cnty'].isin(['Los Angeles']))].sort_values('year_mon')
la_imp_data.index = la_imp_data['year_mon']
newyork_imp_data = imp_data[(imp_data['dptr_cnty'].isin(['New York']))].sort_values('year_mon')
newyork_imp_data.index = newyork_imp_data['year_mon']
chicago_imp_data = imp_data[(imp_data['dptr_cnty'].isin(['Chicago']))].sort_values('year_mon')
chicago_imp_data.index = chicago_imp_data['year_mon']
sfo_imp_data = imp_data[(imp_data['dptr_cnty'].isin(['San Francisco']))].sort_values('year_mon')
sfo_imp_data.index = sfo_imp_data['year_mon']
atl_imp_data = imp_data[(imp_data['dptr_cnty'].isin(['Atlanta']))].sort_values('year_mon')
atl_imp_data.index = atl_imp_data['year_mon']

# 중국 공항의 항공운임 데이터 생성
pudong_imp_data = imp_data[(imp_data['dptr_cnty'].isin(['Pudong']))].sort_values('year_mon')
pudong_imp_data.index = pudong_imp_data['year_mon']
tianjin_imp_data = imp_data[(imp_data['dptr_cnty'].isin(['Tianjin']))].sort_values('year_mon')
tianjin_imp_data.index = tianjin_imp_data['year_mon']
qingdao_imp_data = imp_data[(imp_data['dptr_cnty'].isin(['Qingdao']))].sort_values('year_mon')
qingdao_imp_data.index = qingdao_imp_data['year_mon']
hangzhou_imp_data = imp_data[(imp_data['dptr_cnty'].isin(['Hangzhou']))].sort_values('year_mon')
hangzhou_imp_data.index = hangzhou_imp_data['year_mon']
beijing_imp_data = imp_data[(imp_data['dptr_cnty'].isin(['Beijing']))].sort_values('year_mon')
beijing_imp_data.index = beijing_imp_data['year_mon']
guangzhou_imp_data = imp_data[(imp_data['dptr_cnty'].isin(['Guangzhou']))].sort_values('year_mon')
guangzhou_imp_data.index = guangzhou_imp_data['year_mon']


# 아시아 공항의 항공운임 데이터 생성
kuala_imp_data = imp_data[(imp_data['dptr_cnty'].isin(['Kuala Lumpur']))].sort_values('year_mon')
kuala_imp_data.index = kuala_imp_data['year_mon']
singapor_imp_data = imp_data[(imp_data['dptr_cnty'].isin(['Singapore']))].sort_values('year_mon')
singapor_imp_data.index = singapor_imp_data['year_mon']
hongkong_imp_data = imp_data[(imp_data['dptr_cnty'].isin(['Hong Kong']))].sort_values('year_mon')
hongkong_imp_data.index = hongkong_imp_data['year_mon']
hochimin_imp_data = imp_data[(imp_data['dptr_cnty'].isin(['Ho Chi Minh']))].sort_values('year_mon')
hochimin_imp_data.index = hochimin_imp_data['year_mon']
penang_imp_data = imp_data[(imp_data['dptr_cnty'].isin(['Penang']))].sort_values('year_mon')
penang_imp_data.index = penang_imp_data['year_mon']
kansai_imp_data = imp_data[(imp_data['dptr_cnty'].isin(['Kansai']))].sort_values('year_mon')
kansai_imp_data.index = kansai_imp_data['year_mon']
narita_imp_data = imp_data[(imp_data['dptr_cnty'].isin(['Narita']))].sort_values('year_mon')
narita_imp_data.index = narita_imp_data['year_mon']
nagoya_imp_data = imp_data[(imp_data['dptr_cnty'].isin(['Nagoya']))].sort_values('year_mon')
nagoya_imp_data.index = nagoya_imp_data['year_mon']

# 구주 공항의 항공운임 데이터 생성
frankfrut_imp_data = imp_data[(imp_data['dptr_cnty'].isin(['Frankfrut']))].sort_values('year_mon')
frankfrut_imp_data.index = frankfrut_imp_data['year_mon']
