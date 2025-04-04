from models.insight_g.model import ChatService
from common.utils.setting import PostgreSQL


# 인덱스별 데이터
from models.freight_p.sea_freight_index.bdi_p.model import data_define
from models.freight_p.sea_freight_index.scfi_p.repository import scfi_raw_data
from models.freight_p.sea_freight_index.ccfi_p.repository import ccfi_raw_data
from models.freight_p.sea_freight_index.hrci_p.repository import hrci_redifined_data
from models.freight_p.sea_freight_index.kcci_p.repository import kcci_raw_data

# 인덱스 별 설명문
from models.insight_g.sea_freight_index.repository import input_bdi_info
from models.insight_g.sea_freight_index.repository import input_scfi_info
from models.insight_g.sea_freight_index.repository import input_ccfi_info
from models.insight_g.sea_freight_index.repository import input_hrci_info
from models.insight_g.sea_freight_index.repository import input_kcci_info

# 상세 명령문
from models.insight_g.sea_freight_index.repository import input_order

# Index 정보
from models.insight_g.sea_freight_index.repository import cach_tp_cd, sea_idx_expl, sea_idx_cd, sea_idx_no

list_indx_no = sea_idx_no
expo_cd = sea_idx_cd
expo_cd_nm = sea_idx_expl


def service():
    import logging

    logging.basicConfig(filename='INFO.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    content_list = []

    gemma2 = ChatService()

    dic_json = {
        'input_bdi_json': gemma2.clean_json(data_define()['bdi_cach_expo']),
        'input_scfi_json': gemma2.clean_json(scfi_raw_data()),
        'input_ccfi_json': gemma2.clean_json(ccfi_raw_data()),
        'input_hrci_json': gemma2.clean_json(hrci_redifined_data()[['rgsr_dt','hrci_cach_expo']]),
        'input_kcci_json': gemma2.clean_json(kcci_raw_data())
    }

    dic_info = {
        'input_bdi_info': input_bdi_info,
        'input_scfi_info': input_scfi_info,
        'input_ccfi_info': input_ccfi_info,
        'input_hrci_info': input_hrci_info,
        'input_kcci_info': input_kcci_info
    }

    result_list = []

    result_list.append(gemma2.get_content(input_order=input_order,
                               input_info=input_kcci_info,
                               input_json=dic_json['input_kcci_json']))

    result_list.append(gemma2.get_content(input_order=input_order,
                               input_info=input_scfi_info,
                               input_json=dic_json['input_scfi_json']))

    result_list.append(gemma2.get_content(input_order=input_order,
                               input_info=input_ccfi_info,
                               input_json=dic_json['input_ccfi_json']))

    result_list.append(gemma2.get_content(input_order=input_order,
                               input_info=input_hrci_info,
                               input_json=dic_json['input_hrci_json']))

    result_list.append(gemma2.get_content(input_order=input_order,
                               input_info=input_bdi_info,
                               input_json=dic_json['input_bdi_json']))

    ##
    rdb_info = PostgreSQL()

    import datetime
    current_time = datetime.datetime.now()
    current_day = current_time.strftime('%Y%m%d')
    ##

    data_kcci = {
        'crtn_dt' : current_day,
        'cach_tp_cd' : cach_tp_cd[0],
        'expo_cd': expo_cd[0],
        'expo_cd_nm': expo_cd_nm[0],
        'ai_anay_desc' : result_list[0],
        'cach_tp_no' : list_indx_no[0],
        'frst_rgsr_id' : 'admin',
        'frst_rgsr_dttm' : datetime.datetime.now()
    }

    gemma2.save_data_postdb(data_kcci)

    ##

    data_scfi = {
        'crtn_dt' : current_day,
        'cach_tp_cd' : cach_tp_cd[0],
        'expo_cd': expo_cd[1],
        'expo_cd_nm': expo_cd_nm[1],
        'ai_anay_desc' : result_list[1],
        'cach_tp_no' : list_indx_no[1],
        'frst_rgsr_id' : 'admin',
        'frst_rgsr_dttm' : datetime.datetime.now()
    }

    gemma2.save_data_postdb(data_scfi)

    ##

    data_ccfi = {
        'crtn_dt' : current_day,
        'cach_tp_cd' : cach_tp_cd[0],
        'expo_cd': expo_cd[2],
        'expo_cd_nm': expo_cd_nm[2],
        'ai_anay_desc' : result_list[2],
        'cach_tp_no' : list_indx_no[2],
        'frst_rgsr_id' : 'admin',
        'frst_rgsr_dttm' : datetime.datetime.now()
    }

    gemma2.save_data_postdb(data_ccfi)

    ##

    data_hrci = {
        'crtn_dt' : current_day,
        'cach_tp_cd' : cach_tp_cd[0],
        'expo_cd': expo_cd[3],
        'expo_cd_nm': expo_cd_nm[3],
        'ai_anay_desc' : result_list[3],
        'cach_tp_no' : list_indx_no[3],
        'frst_rgsr_id' : 'admin',
        'frst_rgsr_dttm' : datetime.datetime.now()
    }

    gemma2.save_data_postdb(data_hrci)

    ##

    data_bdi = {
        'crtn_dt' : current_day,
        'cach_tp_cd' : cach_tp_cd[0],
        'expo_cd': expo_cd[4],
        'expo_cd_nm': expo_cd_nm[4],
        'ai_anay_desc' : result_list[4],
        'cach_tp_no' : list_indx_no[4],
        'frst_rgsr_id' : 'admin',
        'frst_rgsr_dttm' : datetime.datetime.now()
    }

    gemma2.save_data_postdb(data_bdi)

if __name__ == '__main__':
    service()