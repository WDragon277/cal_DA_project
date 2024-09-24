import pandas as pd
from statsmodels.tsa.ar_model import AutoReg
from models.sea_freight_index.ccfi_p.repository import ccfi_raw_data

# 모델에 사용할 데이터 정제
def define_ccfi_data():

    df_data = ccfi_raw_data()
    # 날짜 데이터를 인덱스로 설정
    df_data = df_data.set_index('rgsr_dt')
    # ccfi의 타입을 숫자형으로 변경
    df_data['ccfi_cach_expo'] = pd.to_numeric(df_data['ccfi_cach_expo'])

    interpolated_data = df_data.interpolate()
    defined_data = interpolated_data.dropna()
    defined_data.index = pd.DatetimeIndex(defined_data.index).to_period('D')

    return defined_data

# ccfi 모델 훈련 및 예상 결과 도출
def pred_ccfi_model():
    df_data = ccfi_raw_data()
    df_data = df_data.set_index('rgsr_dt')
    df_data['ccfi_cach_expo'] = pd.to_numeric(df_data['ccfi_cach_expo'])

    # 이전 3일간의 데이터를 자가학습하여 결과를 만듭니다.
    defined_data = define_ccfi_data()
    lag = 3
    model = AutoReg(defined_data['ccfi_cach_expo'],lags=lag)
    model_fit = model.fit()

    # predict for two weeks
    predictions = model_fit.predict(start=len(defined_data), end=len(defined_data)+14)
    # printout two values(every friday)
    predictions_weekday = predictions[predictions.index.weekday == 4]

    # Attach the predicted data
    result_df = pd.DataFrame(df_data['ccfi_cach_expo'])
    last_date = str(defined_data.index[-1])
    last_date_index = pd.to_datetime(last_date)

    for row in predictions_weekday:
        last_date_index = last_date_index + pd.DateOffset(days=7)
        new_row = last_date_index.strftime('%Y%m%d')
        result_df.loc[new_row] = row
    result_df = result_df.dropna()

    return result_df



if __name__ == "__main__":

    # defined_data = define_ccfi_data()
    # pred_ccfi_model(defined_data)

    print(pred_ccfi_model())