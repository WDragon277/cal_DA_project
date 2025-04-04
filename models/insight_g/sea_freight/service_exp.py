from models.insight_g.model import ChatService
from common.utils.setting import PostgreSQL
from models.insight_g.sea_freight.repository import sea_frt_no, sea_frt_cty, sea_exp_expl, sea_imp_expl
from models.insight_g.sea_freight.repository import exp_info_list, input_order, sea_freight_tables, \
    key_list
import logging
import datetime

list_indx_no = sea_frt_no
expo_cd = sea_frt_cty
expo_cd_nm_exp = sea_exp_expl
expo_cd_nm_imp = sea_imp_expl

model = ChatService()
rdb_info = PostgreSQL()


logging.basicConfig(filename='INFO.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

exp_json_list = []
results_list = []
total_info_list = exp_info_list

for i in range(17):
    tmp = model.df_to_json(sea_freight_tables[key_list[i]])()
    # tmp = model.clean_json(tmp)

    result_sea = model.get_content(input_order=input_order,
                                   input_info=total_info_list[i],
                                   input_json=tmp)

    results_list.append(result_sea)


current_time = datetime.datetime.now()
current_day = current_time.strftime('%Y%m%d')


data_dubai = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'SEA_EXP',
    'expo_cd': expo_cd[0],
    'expo_cd_nm': sea_exp_expl[0],
    'ai_anay_desc': results_list[0],
    'cach_tp_no': list_indx_no[0],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_dubai)


data_hamburg = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'SEA_EXP',
    'expo_cd': expo_cd[1],
    'expo_cd_nm': sea_exp_expl[1],
    'ai_anay_desc': results_list[1],
    'cach_tp_no': list_indx_no[1],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_hamburg)


data_hochiminh = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'SEA_EXP',
    'expo_cd': expo_cd[2],
    'expo_cd_nm': sea_exp_expl[2],
    'ai_anay_desc': results_list[2],
    'cach_tp_no': list_indx_no[2],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_hochiminh)

data_la = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'SEA_EXP',
    'expo_cd': expo_cd[3],
    'expo_cd_nm': sea_exp_expl[3],
    'ai_anay_desc': results_list[3],
    'cach_tp_no': list_indx_no[3],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_la)

data_manzanillo = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'SEA_EXP',
    'expo_cd': expo_cd[4],
    'expo_cd_nm': sea_exp_expl[4],
    'ai_anay_desc': results_list[4],
    'cach_tp_no': list_indx_no[4],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_manzanillo)

data_montreal = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'SEA_EXP',
    'expo_cd': expo_cd[5],
    'expo_cd_nm': sea_exp_expl[5],
    'ai_anay_desc': results_list[5],
    'cach_tp_no': list_indx_no[5],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_montreal)

data_mumbai = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'SEA_EXP',
    'expo_cd': expo_cd[6],
    'expo_cd_nm': sea_exp_expl[6],
    'ai_anay_desc': results_list[6],
    'cach_tp_no': list_indx_no[6],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_mumbai)

data_ny = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'SEA_EXP',
    'expo_cd': expo_cd[7],
    'expo_cd_nm': sea_exp_expl[7],
    'ai_anay_desc': results_list[7],
    'cach_tp_no': list_indx_no[7],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_ny)

data_rotterdam = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'SEA_EXP',
    'expo_cd': expo_cd[8],
    'expo_cd_nm': sea_exp_expl[8],
    'ai_anay_desc': results_list[8],
    'cach_tp_no': list_indx_no[8],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_rotterdam)

data_saintpetersburg = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'SEA_EXP',
    'expo_cd': expo_cd[9],
    'expo_cd_nm': sea_exp_expl[9],
    'ai_anay_desc': results_list[9],
    'cach_tp_no': list_indx_no[9],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_saintpetersburg)

data_shanghai = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'SEA_EXP',
    'expo_cd': expo_cd[10],
    'expo_cd_nm': sea_exp_expl[10],
    'ai_anay_desc': results_list[10],
    'cach_tp_no': list_indx_no[10],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_shanghai)

data_singapore = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'SEA_EXP',
    'expo_cd': expo_cd[11],
    'expo_cd_nm': sea_exp_expl[11],
    'ai_anay_desc': results_list[11],
    'cach_tp_no': list_indx_no[11],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_singapore)

data_tokyo = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'SEA_EXP',
    'expo_cd': expo_cd[12],
    'expo_cd_nm': sea_exp_expl[12],
    'ai_anay_desc': results_list[12],
    'cach_tp_no': list_indx_no[12],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_tokyo)

data_vancouver = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'SEA_EXP',
    'expo_cd': expo_cd[13],
    'expo_cd_nm': sea_exp_expl[13],
    'ai_anay_desc': results_list[13],
    'cach_tp_no': list_indx_no[13],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_vancouver)

data_vostochny = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'SEA_EXP',
    'expo_cd': expo_cd[14],
    'expo_cd_nm': sea_exp_expl[14],
    'ai_anay_desc': results_list[14],
    'cach_tp_no': list_indx_no[14],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_vostochny)

data_xingang = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'SEA_EXP',
    'expo_cd': expo_cd[15],
    'expo_cd_nm': sea_exp_expl[15],
    'ai_anay_desc': results_list[15],
    'cach_tp_no': list_indx_no[15],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_xingang)

data_yocohama = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'SEA_EXP',
    'expo_cd': expo_cd[16],
    'expo_cd_nm': sea_exp_expl[16],
    'ai_anay_desc': results_list[16],
    'cach_tp_no': list_indx_no[16],
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

model.save_data_postdb(data_yocohama)

