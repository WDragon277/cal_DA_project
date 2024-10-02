import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

import pandas as pd
import seaborn as sns
from elasticsearch import Elasticsearch
from matplotlib import pyplot as plt
import numpy as np
from tqdm import tqdm
from scipy import stats
from sklearn.neural_network import MLPRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error


from common.utils.utils import searchAPI, switch_idx_data
from common.utils.setting import EsSetting


esinfo = EsSetting()

# Index name and document type
index_name = esinfo.sea_save_freight

sea_freight_data = searchAPI(index_name)

by_sea_route_tables = {}
key_list = []



# key가 되는 컬럼(data_cd, dptr_cnty, arvl_cnty)으로 groupby 시행
for key, group in sea_freight_data.groupby(['data_cd', 'dptr_cnty', 'arvl_cnty']):

  # key별 데이터(group) year_mon로 오름차순 정렬 및 딕셔너리에 입력
  by_sea_route_tables[key] = group.sort_values(by='year_mon')
  # Lisy에 key 저장
  key_list.append(key)


if __name__ == '__main__':

  pass