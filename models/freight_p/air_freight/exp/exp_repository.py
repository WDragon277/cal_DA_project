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
exp_data = raw_data[(raw_data.data_cd == 'kcla_exp_air')]

# 코드 데이터
allColumns = list(raw_data.columns.values)
cach_columns = ['year_mon','cach_45k_amt','cach_100k_amt','cach_300k_amt','cach_500k_amt','cach_1000k_amt']
only_cach_columns = ['cach_45k_amt','cach_100k_amt','cach_300k_amt','cach_500k_amt','cach_1000k_amt']

# 학습용 데이터 생성을 위한 도시별 항공 운임 데이터 생성 *광저우는 데이터 부족
# 베이징 공항 코드 오입력된 것 통일 수정
beijing = ['BEIJING', 'Beijing (PEK)', 'BEIJING (PEK)']
# beijing_exp_data = exp_data[(exp_data['arvl_cnty'].isin(beijing))].sort_values('year_mon')

# 미주 공항의 항공운임 데이터 생성
la_exp_data = exp_data[(exp_data['arvl_cnty'].isin(['Los Angeles (LAX)']))].sort_values('year_mon')
la_exp_data.index = la_exp_data['year_mon']
newyork_exp_data = exp_data[(exp_data['arvl_cnty'].isin(['New York (JFK)']))].sort_values('year_mon')
newyork_exp_data.index = newyork_exp_data['year_mon']
chicago_exp_data = exp_data[(exp_data['arvl_cnty'].isin(['Chicago (ORD)']))].sort_values('year_mon')
chicago_exp_data.index = chicago_exp_data['year_mon']
sfo_exp_data = exp_data[(exp_data['arvl_cnty'].isin(['San Francisco (SFO)']))].sort_values('year_mon')
sfo_exp_data.index = sfo_exp_data['year_mon']
atl_exp_data = exp_data[(exp_data['arvl_cnty'].isin(['Atlanta (ATL)']))].sort_values('year_mon')
atl_exp_data.index = atl_exp_data['year_mon']

# 중국 공항의 항공운임 데이터 생성
pudong_exp_data = exp_data[(exp_data['arvl_cnty'].isin(['Pudong (PVG)']))].sort_values('year_mon')
pudong_exp_data.index = pudong_exp_data['year_mon']
tianjin_exp_data = exp_data[(exp_data['arvl_cnty'].isin(['Tianjin (TSN)']))].sort_values('year_mon')
tianjin_exp_data.index = tianjin_exp_data['year_mon']
qingdao_exp_data = exp_data[(exp_data['arvl_cnty'].isin(['Qingdao (TAO)']))].sort_values('year_mon')
qingdao_exp_data.index = qingdao_exp_data['year_mon']
hangzhou_exp_data = exp_data[(exp_data['arvl_cnty'].isin(['Hangzhou (HGH)']))].sort_values('year_mon')
hangzhou_exp_data.index = hangzhou_exp_data['year_mon']
beijing_exp_data = exp_data[(exp_data['arvl_cnty'].isin(beijing))].sort_values('year_mon')
beijing_exp_data.index = beijing_exp_data['year_mon']
guangzhou_exp_data = exp_data[(exp_data['arvl_cnty'].isin(['Guangzhou (CAN)']))].sort_values('year_mon')
guangzhou_exp_data.index = guangzhou_exp_data['year_mon']

# 아시아 공항의 항공운임 데이터 생성
kuala_exp_data = exp_data[(exp_data['arvl_cnty'].isin(['Kuala Lumpur (KUL)']))].sort_values('year_mon')
kuala_exp_data.index = kuala_exp_data['year_mon']
singapor_exp_data = exp_data[(exp_data['arvl_cnty'].isin(['Singapore (SIN)']))].sort_values('year_mon')
singapor_exp_data.index = singapor_exp_data['year_mon']
hongkong_exp_data = exp_data[(exp_data['arvl_cnty'].isin(['Hong Kong (HKG)']))].sort_values('year_mon')
hongkong_exp_data.index = hongkong_exp_data['year_mon']
hochimin_exp_data = exp_data[(exp_data['arvl_cnty'].isin(['Ho Chi Minh (SGN)']))].sort_values('year_mon')
hochimin_exp_data.index = hochimin_exp_data['year_mon']
penang_exp_data = exp_data[(exp_data['arvl_cnty'].isin(['Penang (PEN)']))].sort_values('year_mon')
penang_exp_data.index = penang_exp_data['year_mon']
kansai_exp_data = exp_data[(exp_data['arvl_cnty'].isin(['Kansai (KIX)']))].sort_values('year_mon')
kansai_exp_data.index = kansai_exp_data['year_mon']
narita_exp_data = exp_data[(exp_data['arvl_cnty'].isin(['Narita (NRT)']))].sort_values('year_mon')
narita_exp_data.index = narita_exp_data['year_mon']
nagoya_exp_data = exp_data[(exp_data['arvl_cnty'].isin(['Nagoya (NGO)']))].sort_values('year_mon')
nagoya_exp_data.index = nagoya_exp_data['year_mon']

# 구주 공항의 항공운임 데이터 생성
frankfrut_exp_data = exp_data[(exp_data['arvl_cnty'].isin(['Frankfrut (FRA)']))].sort_values('year_mon')
frankfrut_exp_data.index = frankfrut_exp_data['year_mon']
