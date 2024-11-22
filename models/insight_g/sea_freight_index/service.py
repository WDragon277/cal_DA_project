from models.insight_g.model import ChatService

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

import logging

logging.basicConfig(filename='INFO.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

content_list = []

gemma2 = ChatService()
gemma2_bdi = ChatService()
gemma2_scfi = ChatService()
gemma2_ccfi = ChatService()
gemma2_hrci = ChatService()
gemma2_kcci = ChatService()
model_list = [gemma2_bdi, gemma2_scfi, gemma2_ccfi, gemma2_hrci, gemma2_kcci]

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


for item_json, item_info, item_model in zip(dic_json, dic_info, model_list):

    logging.info(f'{item_json} start')

    try:
        item_json = item_model.clean_json(item_json)
    except AttributeError:
        logging.info(f'{item_json}: AttributeError')
        pass

    content_list.append(item_model.get_content(input_order=input_order,
                                               input_indx_info=item_info,
                                               input_indx_json=item_json)
                            )




model = ChatService()

result = model.get_content(input_order=input_order,
                                           input_indx_info=input_scfi_info,
                                           input_indx_json=dic_json['input_scfi_json'])

result = model.get_content(input_order=input_order,
                                           input_indx_info=input_ccfi_info,
                                           input_indx_json=dic_json['input_ccfi_json'])

result = model.get_content(input_order=input_order,
                                           input_indx_info=input_hrci_info,
                                           input_indx_json=dic_json['input_hrci_json'])

result = model.get_content(input_order=input_order,
                                           input_indx_info=input_kcci_info,
                                           input_indx_json=dic_json['input_kcci_json'])