from models.insight_g.air_freight.repository import input_order
from models.insight_g.air_freight.repository import imp_info_list, imp_df_list
from models.insight_g.air_freight.repository import air_frt_no, air_frt_cty, air_imp_expl
from models.insight_g.model import ChatService
import logging
import time
import datetime

logging.basicConfig(filename='INFO.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 수출 데이터
imp_json_list = []
imp_data = imp_df_list
# 항로별 데이터 100kg 화물 데이터만 활용
# 45kg, 30kg0, 500kg, 1000kg 화물 값은 제거
for idx, val in enumerate(imp_data):
    import time

    start = time.time()

    imp_data[idx] = val.drop(columns=['arvl_cnty', 'dptr_cnty', 'data_cd', 'year_mon', 'cach_45k_amt', 'cach_300k_amt',
                                      'cach_500k_amt', 'cach_1000k_amt'])
    imp_json_list.append(imp_data[idx].to_json())


# 수출데이터기반 보고서 생성

start = time.time()
model = ChatService()
results_list = []

for i in range(20):
    start = time.time()
    results_list.append(model.get_content(input_order=input_order,
                                          input_info=imp_info_list[i],
                                          input_json=imp_json_list[i]))
    print(f'텍스트 생성에 걸린 시간{time.time() - start}s')

# 날짜 데이터생성
current_time = datetime.datetime.now()
current_day = current_time.strftime('%Y%m%d')

# 데이터 업로드
list_indx_no = air_frt_no
expo_cd = air_frt_cty

data_atlanta = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'AIR_IMP',
    'expo_cd': expo_cd[0],
    'expo_cd_nm': air_imp_expl[0],
    'ai_anay_desc': results_list[0],
    'cach_tp_no': list_indx_no[0],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_atlanta)

data_beijing = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'AIR_IMP',
    'expo_cd': expo_cd[1],
    'expo_cd_nm': air_imp_expl[1],
    'ai_anay_desc': results_list[1],
    'cach_tp_no': list_indx_no[1],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_beijing)

data_chicago = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'AIR_IMP',
    'expo_cd': expo_cd[2],
    'expo_cd_nm': air_imp_expl[2],
    'ai_anay_desc': results_list[2],
    'cach_tp_no': list_indx_no[2],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_chicago)


data_frankfurt = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'AIR_IMP',
    'expo_cd': expo_cd[3],
    'expo_cd_nm': air_imp_expl[3],
    'ai_anay_desc': results_list[3],
    'cach_tp_no': list_indx_no[3],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_frankfurt)

data_guangzhou = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'AIR_IMP',
    'expo_cd': expo_cd[4],
    'expo_cd_nm': air_imp_expl[4],
    'ai_anay_desc': results_list[4],
    'cach_tp_no': list_indx_no[4],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_guangzhou)

data_hangzhou = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'AIR_IMP',
    'expo_cd': expo_cd[5],
    'expo_cd_nm': air_imp_expl[5],
    'ai_anay_desc': results_list[5],
    'cach_tp_no': list_indx_no[5],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_hangzhou)

data_hochimin = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'AIR_IMP',
    'expo_cd': expo_cd[6],
    'expo_cd_nm': air_imp_expl[6],
    'ai_anay_desc': results_list[6],
    'cach_tp_no': list_indx_no[6],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_hochimin)

data_hongkong = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'AIR_IMP',
    'expo_cd': expo_cd[7],
    'expo_cd_nm': air_imp_expl[7],
    'ai_anay_desc': results_list[7],
    'cach_tp_no': list_indx_no[7],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_hongkong)

data_kansai = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'AIR_IMP',
    'expo_cd': expo_cd[8],
    'expo_cd_nm': air_imp_expl[8],
    'ai_anay_desc': results_list[8],
    'cach_tp_no': list_indx_no[8],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_kansai)

data_kuala = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'AIR_IMP',
    'expo_cd': expo_cd[9],
    'expo_cd_nm': air_imp_expl[9],
    'ai_anay_desc': results_list[9],
    'cach_tp_no': list_indx_no[9],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_kuala)

data_la = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'AIR_IMP',
    'expo_cd': expo_cd[10],
    'expo_cd_nm': air_imp_expl[10],
    'ai_anay_desc': results_list[10],
    'cach_tp_no': list_indx_no[10],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_la)

data_nagoya = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'AIR_IMP',
    'expo_cd': expo_cd[11],
    'expo_cd_nm': air_imp_expl[11],
    'ai_anay_desc': results_list[11],
    'cach_tp_no': list_indx_no[11],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_nagoya)

data_narita = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'AIR_IMP',
    'expo_cd': expo_cd[12],
    'expo_cd_nm': air_imp_expl[12],
    'ai_anay_desc': results_list[12],
    'cach_tp_no': list_indx_no[12],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_narita)

data_ny = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'AIR_IMP',
    'expo_cd': expo_cd[13],
    'expo_cd_nm': air_imp_expl[13],
    'ai_anay_desc': results_list[13],
    'cach_tp_no': list_indx_no[13],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_ny)

data_penang = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'AIR_IMP',
    'expo_cd': expo_cd[14],
    'expo_cd_nm': air_imp_expl[14],
    'ai_anay_desc': results_list[14],
    'cach_tp_no': list_indx_no[14],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_penang)

data_pudong = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'AIR_IMP',
    'expo_cd': expo_cd[15],
    'expo_cd_nm': air_imp_expl[15],
    'ai_anay_desc': results_list[15],
    'cach_tp_no': list_indx_no[15],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_pudong)

data_qingdao = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'AIR_IMP',
    'expo_cd': expo_cd[16],
    'expo_cd_nm': air_imp_expl[16],
    'ai_anay_desc': results_list[16],
    'cach_tp_no': list_indx_no[16],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_qingdao)

data_sanfrancisco = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'AIR_IMP',
    'expo_cd': expo_cd[17],
    'expo_cd_nm': air_imp_expl[17],
    'ai_anay_desc': results_list[17],
    'cach_tp_no': list_indx_no[17],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_sanfrancisco)

data_singapore = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'AIR_IMP',
    'expo_cd': expo_cd[18],
    'expo_cd_nm': air_imp_expl[18],
    'ai_anay_desc': results_list[18],
    'cach_tp_no': list_indx_no[18],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_singapore)

data_tianjin = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'AIR_IMP',
    'expo_cd': expo_cd[19],
    'expo_cd_nm': air_imp_expl[19],
    'ai_anay_desc': results_list[19],
    'cach_tp_no': list_indx_no[19],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_tianjin)
