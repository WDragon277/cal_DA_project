from models.insight_g.air_freight.repository import input_order
from models.insight_g.air_freight.repository import imp_info_list, imp_df_list
from models.insight_g.air_freight.repository import exp_info_list, exp_df_list
from models.insight_g.model import ChatService
import logging

logging.basicConfig(filename='INFO.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

imp_json_list = []
# 항로별 데이터 100kg 화물 데이터만 활용
# 45kg, 30kg0, 500kg, 1000kg 화물 값은 제거
for idx, val in enumerate(imp_df_list):
    import time

    start = time.time()

    imp_df_list[idx] = val.drop(columns=['arvl_cnty','dptr_cnty','data_cd','year_mon', 'cach_45k_amt', 'cach_300k_amt', 'cach_500k_amt',
                        'cach_1000k_amt'])
    # imp_df_list[idx] = imp_df_list[idx].reset_index()
    imp_json_list.append(imp_df_list[idx].to_json())

import time

start = time.time()
imp_data = imp_df_list

model = ChatService()
result = []
result.append(model.get_content(input_order = input_order + '한글로 말해줘',
                                input_info = imp_info_list[1],
                                input_json = imp_json_list[1]))

print(f'텍스트 생성에 걸린 시간{time.time() - start}s')

# from models.freight_p.air_freight.imp.exp_repository import
logging.basicConfig(filename='INFO.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 수출 데이터
exp_json_list = []
exp_data = exp_df_list
# 항로별 데이터 100kg 화물 데이터만 활용
# 45kg, 30kg0, 500kg, 1000kg 화물 값은 제거
for idx, val in enumerate(exp_data):
    import time

    start = time.time()

    exp_data[idx] = val.drop(columns=['arvl_cnty','dptr_cnty','data_cd','year_mon', 'cach_45k_amt', 'cach_300k_amt', 'cach_500k_amt',
                        'cach_1000k_amt'])
    exp_json_list.append(exp_data[idx].to_json())

import time

start = time.time()

model = ChatService()
result_exp = []
for i in range(20):
    start = time.time()
    result_exp.append(model.get_content(input_order = input_order + '한글로 말해줘',
                                    input_info = exp_info_list[i],
                                    input_json = exp_json_list[i]))
    print(f'텍스트 생성에 걸린 시간{time.time() - start}s')