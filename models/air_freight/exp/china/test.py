from models.air_freight.exp.exp_repository import pudong_exp_data, tianjin_exp_data, qingdao_exp_data, \
    hangzhou_exp_data, beijing_exp_data, guangzhou_exp_data
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from statsmodels.tsa.ar_model import AutoReg


import matplotlib.pyplot as plt

# 항공운임 예측 모델 테스트


# 기초통계 정보 (표준편차가 클 수록 자가회귀 분석에 어려움)
pudong_exp_data.describe()
tianjin_exp_data.describe()
qingdao_exp_data.describe()
hangzhou_exp_data.describe()
beijing_exp_data.describe()
guangzhou_exp_data.describe()

china_data = [pudong_exp_data, tianjin_exp_data, qingdao_exp_data,
             hangzhou_exp_data, beijing_exp_data, guangzhou_exp_data]

china_name_mae = [['pudong_exp_45k_mae', 'pudong_exp_100k_mae','pudong_exp_300k_mae',
                  'pudong_exp_500k_mae','pudong_exp_1000k_mae'],
                 ['tianjin_exp_data_45k_mae', 'tianjin_exp_data_100k_mae','tianjin_exp_data_300k_mae',
                  'tianjin_exp_data_500k_mae','tianjin_exp_data_1000k_mae'],
                 ['qingdao_exp_exp_45k_mae', 'qingdao_exp_exp_100k_mae','qingdao_exp_exp_300k_mae',
                  'qingdao_exp_exp_500k_mae','qingdao_exp_exp_1000k_mae'],
                 ['hangzhou_exp_45k_mae', 'hangzhou_exp_100k_mae','hangzhou_exp_300k_mae',
                  'hangzhou_exp_500k_mae','hangzhou_exp_1000k_mae'],
                 ['beijing_exp_45k_mae','beijing_exp_100k_mae','beijing_exp_300k_mae',
                  'beijing_exp_500k_mae','beijing_exp_1000k_mae'],
                 ['guangzhou_exp_45k_mae','guangzhou_exp_100k_mae','guangzhou_exp_300k_mae',
                  'guangzhou_exp_500k_mae','guangzhou_exp_1000k_mae'],
                 ['narita_exp_45k_mae','narita_exp_100k_mae','narita_exp_300k_mae',
                  'narita_exp_500k_mae','narita_exp_1000k_mae'],
                 ['nagoya_exp_45k_mae','nagoya_exp_100k_mae','nagoya_exp_300k_mae',
                  'nagoya_exp_500k_mae','nagoya_exp_1000k_mae']
                 ]

china_mae_dic = {}


def china_mae():
    # MAE 연산
    for num in range(1,4):
        for i, j in zip(china_data, china_name_mae):

            i = i.iloc[:, 4:9]
            train, test = train_test_split(i, test_size=0.2, shuffle=False)

            for k, l in zip(range(i.shape[1]), j):
                model = AutoReg(train.iloc[:, k], lags=num)
                model_fit = model.fit()

                yhat = model_fit.predict(start=len(train), end=len(train)+len(test)-1)

                mae = mean_absolute_error(test.iloc[:, k], yhat)

                china_mae_dic.update({l:[mae]})

        df_mae = pd.DataFrame(china_mae_dic)

        idx = df_mae.index.tolist() # df의 인덱스를 리스트로 변환한 형태
        idx[0] = "MAE" # 특정 인덱스에 특정값 대입
        df_mae.index = idx # 변경된 인덱스 삽입

        df_mae_tp = df_mae.transpose()

        # View barplot (가로)
        # plt.figure(figsize=(15, 8))
        # plt.rcParams["figure.figsize"] = (20, 8)
        df_mae_tp.plot(kind='barh', y='MAE', rot=0)
        plt.axvline(x=1, color='r', linestyle='--')
        plt.title(f'asia model MAE (rag={num})')
        plt.tight_layout()
        # plt.show()
        plt.savefig(f"C:\\Users\\0614_\\OneDrive - (주)케이씨넷\\바탕 화면\\cal_air_test_result\\china\\test_{num}")


if __name__ == '__main__':
    china_mae()