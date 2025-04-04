from models.insight_g.model import ChatService
from common.utils.setting import PostgreSQL

upload_service = ChatService()

rdb_info = PostgreSQL()

import datetime
current_time = datetime.datetime.now()
current_day = current_time.strftime('%Y%m%d')

data_la = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_EXP',
    'expo_cd': 'Los Angeles',
    'expo_cd_nm': 'Export to Los Angeles',
    'ai_anay_desc' : result_exp[0],
    'cach_tp_no' : '011',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_la)



data_newyork = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_EXP',
    'expo_cd': 'New York',
    'expo_cd_nm': 'Export to New York',
    'ai_anay_desc' : result_exp[1],
    'cach_tp_no' : '014',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_newyork)


data_chicago = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_EXP',
    'expo_cd': 'Chicago',
    'expo_cd_nm': 'Export to Chicago',
    'ai_anay_desc' : result_exp[2],
    'cach_tp_no' : '003',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_chicago)

data_sfo = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_EXP',
    'expo_cd': 'San Francisco',
    'expo_cd_nm': 'Export to San Francisco',
    'ai_anay_desc': result_exp[3],
    'cach_tp_no': '018',
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

upload_service.save_data_postdb(data_sfo)

data_atl = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_EXP',
    'expo_cd': 'Atlanta',
    'expo_cd_nm': 'Export to Atlanta',
    'ai_anay_desc': result_exp[4],
    'cach_tp_no' : '001',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_atl)

data_pudong = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_EXP',
    'expo_cd': 'Pudong',
    'expo_cd_nm': 'Export to Pudong',
    'ai_anay_desc' : result_exp[5],
    'cach_tp_no' : '016',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_pudong)

data_tianjin = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_EXP',
    'expo_cd': 'Tianjin',
    'expo_cd_nm': 'Export to Tianjin',
    'ai_anay_desc' : result_exp[6],
    'cach_tp_no' : '020',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_tianjin)

data_qingdao = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_EXP',
    'expo_cd': 'Qingdao',
    'expo_cd_nm': 'Export to Qingdao',
    'ai_anay_desc' : result_exp[7],
    'cach_tp_no' : '017',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_qingdao)

data_hangzhou = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_EXP',
    'expo_cd': 'Hangzhou',
    'expo_cd_nm': 'Export to Hangzhou',
    'ai_anay_desc' : result_exp[8],
    'cach_tp_no' : '006',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_hangzhou)

data_beijing = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_EXP',
    'expo_cd': 'Beijing',
    'expo_cd_nm': 'Export to Beijing',
    'ai_anay_desc': result_exp[9],
    'cach_tp_no' : '002',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_beijing)

data_guangzhou = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_EXP',
    'expo_cd': 'Guangzhou',
    'expo_cd_nm': 'Export to Guangzhou',
    'ai_anay_desc': result_exp[10],
    'cach_tp_no' : '005',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_guangzhou)

data_kuala = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_EXP',
    'expo_cd': 'Kuala',
    'expo_cd_nm': 'Export to Kuala Lumpur',
    'ai_anay_desc': result_exp[11],
    'cach_tp_no' : '010',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_kuala)

data_singapor = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_EXP',
    'expo_cd': 'Singapor',
    'expo_cd_nm': 'Export to Singapor',
    'ai_anay_desc':result_exp[12],
    'cach_tp_no' : '019',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_singapor)

data_hongkong = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_EXP',
    'expo_cd': 'Hongkong',
    'expo_cd_nm': 'Export to Hongkong',
    'ai_anay_desc' : result_exp[13],
    'cach_tp_no' : '008',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_hongkong)

data_hochimin = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_EXP',
    'expo_cd': 'Hochimin',
    'expo_cd_nm': 'Export to Hochimin',
    'ai_anay_desc' : result_exp[14],
    'cach_tp_no' : '007',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_hochimin)

data_penang = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_EXP',
    'expo_cd': 'Penang',
    'expo_cd_nm': 'Export to Penang',
    'ai_anay_desc' : result_exp[15],
    'cach_tp_no' : '015',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_penang)

data_kansai = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_EXP',
    'expo_cd': 'Kansai',
    'expo_cd_nm': 'Export to Kansai',
    'ai_anay_desc' : result_exp[16],
    'cach_tp_no' : '009',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_kansai)

data_narita = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_EXP',
    'expo_cd': 'Narita',
    'expo_cd_nm': 'Export to Narita',
    'ai_anay_desc' : result_exp[17],
    'cach_tp_no' : '013',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_narita)

data_nagoya = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_EXP',
    'expo_cd': 'Nagoya',
    'expo_cd_nm': 'Export to Nagoya',
    'ai_anay_desc' : result_exp[18],
    'cach_tp_no' : '012',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_nagoya)

data_frankfrut = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_EXP',
    'expo_cd': 'Frankfrut',
    'expo_cd_nm': 'Export to Frankfrut',
    'ai_anay_desc' : result_exp[19],
    'cach_tp_no' : '004',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_frankfrut)