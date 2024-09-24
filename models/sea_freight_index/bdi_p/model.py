import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
import numpy as np
from models.sea_freight_index.bdi_p.repository import bdi_raw_data, usdx_raw_data, raw_maters_price, raw_data_merge


def data_define():

    bdi_data = bdi_raw_data()
    usdx_data = usdx_raw_data()
    maters_price = raw_maters_price()

    merged_data = raw_data_merge(bdi_data, usdx_data, maters_price)
    merged_data = merged_data.set_index('rgsr_dt')

    interpolated_data = merged_data.interpolate()
    defined_data = interpolated_data.dropna()

    return defined_data


def pred_bdi_model():

    # setting the data
    defined_data = data_define()
    pred_days = 14
    last_date = defined_data.index[-1]

    # split the data
    X = np.array(defined_data[['copper_price','dubai_price','lprc']])
    y = np.array(defined_data['bdi_cach_expo'])

    # Moving data for predict days
    X_train = X[:-pred_days] #[0~-14]
    y_train = y[pred_days:] #[14~end]
    X_pred = X[-pred_days:] #-14~-0

    # Predict 14days
    model = GradientBoostingRegressor(n_estimators=200, learning_rate=0.05, max_depth=5)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_pred)

    # Attach the predicted data
    result_df = pd.DataFrame(defined_data['bdi_cach_expo'])

    last_date_index = pd.to_datetime(last_date)
    for row in y_pred:
        last_date_index = last_date_index + pd.DateOffset(days=1)
        new_row = last_date_index.strftime('%Y%m%d')
        result_df.loc[new_row] = row

    return result_df

if __name__=='__main__':

    print(pred_bdi_model())

