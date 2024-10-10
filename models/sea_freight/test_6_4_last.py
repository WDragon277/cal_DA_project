from sklearn.preprocessing import MinMaxScaler
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_absolute_error
from models.sea_freight.repository import sea_freight_data

# 데이터 로드
data = sea_freight_data
data = data.sort_values(by=['dptr_cnty', 'arvl_cnty', 'year_mon'])
# 출발지와 도착지별로 그룹화
routes = data.groupby(['dptr_cnty', 'arvl_cnty'])

class SARIMAmodel:
    def __init__(self):
        self.results = {} # 결과 저장을 위한 딕셔너리

        # ARIMA 파라미터
        self.p, self.d, self.q = 6, 1, 6
        # 계절성 파라미터 (s=12는 12개월 계절성 가정)
        self.P, self.D, self.Q, self.s = 1, 1, 1, 12

        self.forcast = 6
        self.scaler = MinMaxScaler()

        # 하나의 항로의 운임데이터 SARIMA 모델 학습
    def model_fitting(self, route_data):

        # 정규화
        route_data['cach_amt'] = self.scaler.fit_transform(route_data[['cach_amt']])
        route_data = route_data.set_index('year_mon')
        x_train = route_data['cach_amt']

        # SARIMA 모델 학습
        model = SARIMAX(x_train, order=(self.p, self.d, self.q),
                        seasonal_order=(self.P, self.D, self.Q, self.s),
                        enforce_stationarity=False,
                        enforce_invertibility=False)
        model_fit = model.fit(disp=False)

        return model_fit

    def prediction(self, model_fit):

        # 예측
        predictions = model_fit.forecast(steps=self.forcast)

        # 예측값을 원래 스케일로 복원
        predictions_inv = self.scaler.inverse_transform(predictions.values.reshape(-1, 1))

        return predictions_inv

