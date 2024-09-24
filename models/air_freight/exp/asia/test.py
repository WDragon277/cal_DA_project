from models.air_freight.exp.exp_repository import kuala_exp_data, singapor_exp_data, hongkong_exp_data, \
    hochimin_exp_data, penang_exp_data, kansai_exp_data, narita_exp_data, nagoya_exp_data
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import r2_score
from statsmodels.tsa.ar_model import AutoReg
import matplotlib.pyplot as plt

# 항공운임 예측 모델 테스트


# 기초통계 정보 (표준편차가 클 수록 자가회귀 분석에 어려움)
hochimin_exp_data.describe()
hongkong_exp_data.describe()
penang_exp_data.describe()
singapor_exp_data.describe()
kuala_exp_data.describe()
kansai_exp_data.describe()
narita_exp_data.describe()
nagoya_exp_data.describe()


asia_data = [kuala_exp_data, singapor_exp_data, hongkong_exp_data, hochimin_exp_data, penang_exp_data,
             kansai_exp_data, narita_exp_data, nagoya_exp_data]

asia_name_mae = [['kuala_exp_45k_mae', 'kuala_exp_100k_mae','kuala_exp_300k_mae',
                  'kuala_exp_500k_mae','kuala_exp_1000k_mae'],
                 ['singapor_exp_45k_mae', 'singapor_exp_100k_mae','singapor_exp_300k_mae',
                  'singapor_exp_500k_mae','singapor_exp_1000k_mae'],
                 ['hongkong_exp_45k_mae', 'hongkong_exp_100k_mae','hongkong_exp_300k_mae',
                  'hongkong_exp_500k_mae','hongkong_exp_1000k_mae'],
                 ['hochimin_exp_45k_mae', 'hochimin_exp_100k_mae','hochimin_exp_300k_mae',
                  'hochimin_exp_500k_mae','hochimin_exp_1000k_mae'],
                 ['penang_exp_45k_mae','penang_exp_100k_mae','penang_exp_300k_mae'
                     ,'penang_exp_500k_mae','penang_exp_1000k_mae'],
                 ['kansai_exp_45k_mae','kansai_exp_100k_mae','kansai_exp_300k_mae'
                     ,'kansai_exp_500k_mae','kansai_exp_1000k_mae'],
                 ['narita_exp_45k_mae','narita_exp_100k_mae','narita_exp_300k_mae'
                     ,'narita_exp_500k_mae','narita_exp_1000k_mae'],
                 ['nagoya_exp_45k_mae','nagoya_exp_100k_mae','nagoya_exp_300k_mae'
                     ,'nagoya_exp_500k_mae','nagoya_exp_1000k_mae']
                 ]


asia_mae_dic = {}


def asia_mae():

    phi_num = 0
    # MAE 연산
    for num in range(1, 2):
        for i, j in zip(asia_data, asia_name_mae):

            i = i.iloc[:, 4:9]
            train, test = train_test_split(i, test_size=0.2, shuffle=False)

            for k, l in zip(range(i.shape[1]), j):
                model = AutoReg(train.iloc[:, k], lags=num)
                model_fit = model.fit()

                yhat = model_fit.predict(start=len(train), end=len(train)+len(test)-1)

                mae = mean_absolute_error(test.iloc[:, k], yhat)

                # plt.plot(i.index[len(train):len(train)+len(test)], test.iloc[:, k])
                # plt.plot(i.index[len(train):len(train)+len(test)], yhat)
                # # plt.show()

                asia_mae_dic.update({l:[mae]})

                phi = model_fit.params[1]

                print('φ값: %f' % phi)

                # 정상성 판단
                if abs(phi) < 1:
                    print('모델은 정상성을 가집니다.')
                    phi_num = phi_num + 1
                else:
                    print(f'{l}모델은 정상성을 가지지 않습니다.')

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

        return phi_num

if __name__ == '__main__':
    phi_num = asia_mae()
    print(phi_num)
