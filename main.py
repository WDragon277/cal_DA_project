# 각 모델들을 실행하는 서비스 순차적으로 수행 및 로그 남기는 스크립트 제작
from models.air_freight.exp.asia.service import save_asia_pred_exp
from models.air_freight.exp.china.service import save_china_pred_exp
from models.air_freight.exp.euro.service import save_euro_pred_exp
from models.air_freight.exp.usa.service import save_usa_pred_exp

from models.air_freight.imp.asia.service import save_asia_pred_imp
from models.air_freight.imp.china.service import save_china_pred_imp
from models.air_freight.imp.euro.service import save_euro_pred_imp
from models.air_freight.imp.usa.service import save_usa_pred_imp

from elasticsearch import Elasticsearch, helpers
import logging

from models.sea_freight.bdi_p.service import predict_bdi
from models.sea_freight.ccfi_p.service import predict_ccfi
from models.sea_freight.kcci_p.service import predict_kcci
from models.sea_freight.hrci_p.service import predict_hrci
from models.sea_freight.scfi_p.service import predict_scfi

from models.sea_freight.bdi_p.service import insert_bdi
from models.sea_freight.ccfi_p.service import insert_ccfi
from models.sea_freight.kcci_p.service import insert_kcci
from models.sea_freight.hrci_p.service import insert_hrci
from models.sea_freight.scfi_p.service import insert_scfi

from common.utils.utils import delete_ops_index

from common.utils.setting import EsSetting

esinfo = EsSetting()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 항공운임 데이터 삽입
if __name__ == '__main__':

    delete_ops_index(esinfo.air_save_index)
    logger.info("데이터 수정/삽입을 위해 기존의 항공운임 인덱스가 제거되었습니다.")

    save_asia_pred_exp()
    save_china_pred_exp()
    save_euro_pred_exp()
    save_usa_pred_exp()

    save_asia_pred_imp()
    save_china_pred_imp()
    save_euro_pred_imp()
    save_usa_pred_imp()

    # 해상운임지수 예측 및 내용 저장

    es = Elasticsearch(esinfo.IP, basic_auth=(esinfo.ID, esinfo.PW))

    # Index name and document type
    index_name = esinfo.sea_save_index
    doc_type = '_doc'
    index_exists = es.indices.exists(index=index_name)

    predict_bdi_data = predict_bdi()
    predict_ccfi_data = predict_ccfi()
    predict_kcci_data = predict_kcci()
    predict_hrci_data = predict_hrci()
    predict_scfi_data = predict_scfi()

    # Delete the index
    delete_ops_index(esinfo.sea_save_index)
    logger.info("데이터 수정/삽입을 위해 기존의 해상인덱스 인덱스가 제거되었습니다.")

    # Insert generated data
    try:
        insert_bdi(predict_bdi_data)
        logger.info("bdi 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("bdi 데이터 삽입에 오류가 발생하였습니다.")

    try:
        insert_ccfi(predict_ccfi_data)
        logger.info("ccfi 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("ccfi 데이터 삽입에 오류가 발생하였습니다.")

    try:
        insert_kcci(predict_kcci_data)
        logger.info("kcci 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("kcci 데이터 삽입에 오류가 발생하였습니다.")

    try:
        insert_hrci(predict_hrci_data)
        logger.info("hrdi 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("hrdi 데이터 삽입에 오류가 발생하였습니다.")

    try:
        insert_scfi(predict_scfi_data)
        logger.info("scfi 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("scfi 데이터 삽입에 오류가 발생하였습니다.")
