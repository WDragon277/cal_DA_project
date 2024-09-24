from elasticsearch import Elasticsearch, helpers
import logging
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import r2_score
from statsmodels.tsa.ar_model import AutoReg
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller

from models.air_freight.exp.asia.test import asia_data, asia_name_mae, asia_mae_dic
from common.utils.setting import EsSetting
from common.utils.utils import searchAPI

esinfo = EsSetting()
test_air = searchAPI(esinfo.air_save_index)
test_sea = searchAPI(esinfo.sea_save_index)

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def asia_mae():
    # MAE 연산
    for num in range(1,4):
        for i, j in zip(asia_data, asia_name_mae):

            i = i.iloc[:, 4:9]
            train, test = train_test_split(i, test_size=0.2, shuffle=False)

            for k, l in zip(range(i.shape[1]), j):
                model = AutoReg(train.iloc[:, k], lags=num)
                model_fit = model.fit()

                yhat = model_fit.predict(start=len(train), end=len(train)+len(test)-1)

                mae = mean_absolute_error(test.iloc[:, k], yhat)

                plt.plot(i.index[len(train):len(train)+len(test)], test.iloc[:, k])
                plt.plot(i.index[len(train):len(train)+len(test)], yhat)
                # plt.show()

                asia_mae_dic.update({l:[mae]})



        df_mae = pd.DataFrame(asia_mae_dic)

        idx = df_mae.index.tolist() # df의 인덱스를 리스트로 변환한 형태
        idx[0] = "MAE" # 특정 인덱스에 특정값 대입
        df_mae.index = idx # 변경된 인덱스 삽입

        df_mae_tp = df_mae.transpose()

        # View barplot (가로)
        plt.figure()
        # plt.rcParams["figure.figsize"] = (20, 8)
        df_mae_tp.plot(kind='barh', y='MAE', rot=0)
        plt.axvline(x=1, color='r', linestyle='--')
        plt.title(f'asia model MAE (rag={num})')
        plt.tight_layout()
        # plt.show()
        plt.savefig(f"C:\\Users\\0614_\\OneDrive - (주)케이씨넷\\바탕 화면\\cal_air_test_result\\asia\\test_{num}")


# 임의의 시계열 데이터 생성
np.random.seed(0)
n_samples = 100
X = np.random.normal(size=n_samples)

# 정상성 테스트 수행
result = adfuller(X)

print('ADF Statistic: %f' % result[0])
print('p-value: %f' % result[1])
print('Critical Values:')
for key, value in result[4].items():
    print('\t%s: %.3f' % (key, value))
