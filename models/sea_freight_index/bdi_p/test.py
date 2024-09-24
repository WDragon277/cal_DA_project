from models.bdi_p.model import data_define
from sklearn.preprocessing import MinMaxScaler
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_squared_error

merged_data = data_define()
scaler = MinMaxScaler()

# tmp = merged_data.iloc[406:575]
# tmp2 = tmp.interpolate()
# tmp3 = tmp2.dropna()
# tmp3 = merged_data

# tmp3[['copper_price','dubai_price','lprc','bdi_cach_expo']] = \
#     scaler.fit_transform(tmp3[['copper_price','dubai_price','lprc','bdi_cach_expo']])
#
# lenth = len(tmp3)
# ratio = 0.8
# train_str = int(lenth*ratio)
#
# X = np.array(tmp3[['copper_price','dubai_price','lprc']])
# y = np.array(tmp3['bdi_cach_expo'])
def validation(merged_data):
    # merged_data[['copper_price','dubai_price','lprc','bdi_cach_expo']] = \
    #     scaler.fit_transform(merged_data[['copper_price','dubai_price','lprc','bdi_cach_expo']])

    date = merged_data.index
    # lenth = len(merged_data)
    # ratio = 0.8
    # train_str = int(lenth*ratio)

    x = np.array(merged_data[['copper_price','dubai_price','lprc']])
    y = np.array(merged_data['bdi_cach_expo'])

    #  최신 데이터 활용
    x_resent = x[int(len(x) * 0.6):]
    y_resent = y[int(len(y) * 0.6):]

    lenth = len(x_resent)
    ratio = 0.8
    train_str = int(lenth*ratio)

    x_train = x_resent[:train_str-28]
    y_train = y_resent[28:train_str]  # 실제 데이터 보다 14일 이후의 값 훈련

    x_test = x_resent[train_str-28:train_str-14]
    y_test = y_resent[train_str-14:train_str]  # 실제 데이터 보다 14일 이후의 값 훈련


    # x_train = x[:train_str-28]
    # y_train = y[28:train_str]  # 실제 데이터 보다 14일 이후의 값 훈련
    #
    # x_test = x[train_str-28:train_str-14]
    # y_test = y[train_str-14:train_str]  # 실제 데이터 보다 14일 이후의 값 훈련

    # x_train = x[:train_str]
    # y_train = y[14:train_str+14]  # 실제 데이터 보다 14일 이후의 값 훈련
    #
    # x_test = x[train_str:train_str+14]
    # y_test = y[14+train_str:train_str+28]  # 실제 데이터 보다 14일 이후의 값 훈련

    model = GradientBoostingRegressor(n_estimators=200, learning_rate=0.05, max_depth=5)
    model.fit(x_train, y_train)
    y_pred = model.predict(x_test)

    accuracy = mean_squared_error(y_test,y_pred)

    print("RMSE:", accuracy)

    from sklearn.metrics import r2_score
    r2 = r2_score(y_test, y_pred)

    print('r2 score: ', r2)


    import matplotlib.pyplot as plt

    plt.figure(figsize=(10, 6))
    # plt.scatter(date[-14:], y_test, color='blue', label='Actual')
    plt.plot(date[-14:], y_test, color='red', linewidth=2, label='Actual')
    plt.plot(date[-14:], y_pred, color='blue', linewidth=2, label='Predicted')
    plt.title(f'Linear Regression\nR-squared: {r2:.3f}')
    plt.xlabel('X')
    plt.ylabel('y')
    plt.legend()
    plt.show()

if __name__=="__main__":

    merged_data = data_define()
    scaler = MinMaxScaler()

    validation(merged_data)