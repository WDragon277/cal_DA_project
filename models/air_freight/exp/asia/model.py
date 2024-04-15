from models.air_freight.exp.exp_repository import kuala_exp_data, singapor_exp_data, hongkong_exp_data, \
    hochimin_exp_data, penang_exp_data
import pandas as pd
from statsmodels.tsa.ar_model import AutoReg

# 각 공항별 운송료 데이터를 예측 모델에 적용하여 예측값 출력


def pred_freight_model(data):

    '''
    항공 수출 운송운임에 대한 예측모델
    각 항로별 데이터에 특화된 예측모델 설계를 진행할 예정
    '''

    data.index = pd.to_datetime(data.index, format='%Y%m').to_period('M')

    result = data.copy()
    only_cach_columns = ['cach_45k_amt', 'cach_100k_amt', 'cach_300k_amt', 'cach_500k_amt', 'cach_1000k_amt']

    # 각 열마다 예측값을 새롭게 입력하는 다중 for문
    for col in only_cach_columns:

        lag = 3
        model = AutoReg(data[col], lags=lag)
        model_fit = model.fit()

        # predict for three month
        predictions = model_fit.predict(start=len(data), end=len(data) + 2)

        # Make date_index of result
        last_date = str(data.index[-1])
        last_date_index = pd.to_datetime(last_date, format='%Y-%m')

        # Insert prediction to result
        for row in predictions:
            last_date_index = last_date_index + pd.DateOffset(months=1)
            new_row_index = last_date_index.strftime('%Y%m')
            result.loc[new_row_index, col] = row


    return result


if __name__== '__main__':

    # test sample
    data = kuala_exp_data
    result = pred_freight_model(data)
    result[['data_cd','dptr_cnty','arvl_cnty']] = result[['data_cd', 'dptr_cnty', 'arvl_cnty']].ffill()
    print(result)