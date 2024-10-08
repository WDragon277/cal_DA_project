import pandas as pd
from sklearn.metrics import mean_squared_error
from statsmodels.tsa.ar_model import AutoReg
from models.sea_freight_index.kcci_p.repository import kcci_raw_data

# kcci 모델 훈련 및 예상 결과 도출
def pred_kcci_model():

    df_data = kcci_raw_data()
    df_data = df_data.set_index('rgsr_dt')
    df_data['kcci_cach_expo'] = pd.to_numeric(df_data['kcci_cach_expo'])

    interpolated_data = df_data.interpolate()
    defined_data = interpolated_data.dropna()
    defined_data.index = pd.DatetimeIndex(defined_data.index).to_period('D')

    lag = 3
    model = AutoReg(defined_data['kcci_cach_expo'],lags=lag)
    model_fit = model.fit()

    # predict for two weeks
    predictions = model_fit.predict(start=len(defined_data), end=len(defined_data)+14)
    # printout two values(every friday)
    predictions_weekday = predictions[predictions.index.weekday == 4]

    # Attach the predicted data
    result_df = pd.DataFrame(df_data['kcci_cach_expo'])
    last_date = str(defined_data.index[-1])
    last_date_index = pd.to_datetime(last_date)

    for row in predictions_weekday:
        last_date_index = last_date_index + pd.DateOffset(days=7)
        new_row = last_date_index.strftime('%Y%m%d')
        result_df.loc[new_row] = row
    result_df = result_df.dropna()

    return result_df

if __name__ == "__main__":

    # df_data = kcci_raw_data()
    # df_data = df_data.set_index('rgsr_dt')
    # df_data['kcci_cach_expo'] = pd.to_numeric(df_data['kcci_cach_expo'])
    #
    # interpolated_data = df_data.interpolate()
    # defined_data = interpolated_data.dropna()
    # defined_data.index = pd.DatetimeIndex(defined_data.index).to_period('D')
    #
    # train = defined_data['kcci_cach_expo']
    # pred = pred_kcci_model()
    #
    # mse = mean_squared_error(train[-1:], pred)

    print(pred_kcci_model())