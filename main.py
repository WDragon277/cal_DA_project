from elasticsearch import Elasticsearch, helpers
import logging
# # ===== 항공운임의 각 모델들을 실행하는 함수 불러오기 =====
# # 수출 항공운임 예측 데이터 생성
# from models.air_freight.exp.asia.service import save_asia_pred_exp
# from models.air_freight.exp.china.service import save_china_pred_exp
# from models.air_freight.exp.euro.service import save_euro_pred_exp
# from models.air_freight.exp.usa.service import save_usa_pred_exp
# # 수입 항공운임 예측 데이터 생성
# from models.air_freight.imp.asia.service import save_asia_pred_imp
# from models.air_freight.imp.china.service import save_china_pred_imp
# from models.air_freight.imp.euro.service import save_euro_pred_imp
# from models.air_freight.imp.usa.service import save_usa_pred_imp
#
# # ===== 해상운임 인덱스 각 모델을 실행하는 함수 불러오기 =====
# # 해상운임 인덱스 예측 값 생성 함수
# from models.sea_freight_index.bdi_p.service import predict_bdi
# from models.sea_freight_index.ccfi_p.service import predict_ccfi
# from models.sea_freight_index.kcci_p.service import predict_kcci
# from models.sea_freight_index.hrci_p.service import predict_hrci
# from models.sea_freight_index.scfi_p.service import predict_scfi
#
# # 해상운임 인덱스 예측 값 삽입 함수
# from models.sea_freight_index.bdi_p.service import insert_bdi
# from models.sea_freight_index.ccfi_p.service import insert_ccfi
# from models.sea_freight_index.kcci_p.service import insert_kcci
# from models.sea_freight_index.hrci_p.service import insert_hrci
# from models.sea_freight_index.scfi_p.service import insert_scfi

# ===== 해상운임 예측 값 =====


# ===== 기타 =====

# ES 인덱스 내용 삭제 함수
from common.utils.utils import delete_index

# ES 세팅값 클래스
from common.utils.setting import EsSetting

esinfo = EsSetting()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 항공운임 데이터 삽입
if __name__ == '__main__':

    # delete_index(esinfo.air_save_index)
    # logger.info("데이터 수정/삽입을 위해 기존의 항공운임 인덱스가 제거되었습니다.")
    #
    # save_asia_pred_exp()
    # save_china_pred_exp()
    # save_euro_pred_exp()
    # save_usa_pred_exp()
    #
    # save_asia_pred_imp()
    # save_china_pred_imp()
    # save_euro_pred_imp()
    # save_usa_pred_imp()
    #
    # # 해상운임지수 예측 및 내용 저장
    #
    # es = Elasticsearch(esinfo.IP, basic_auth=(esinfo.ID, esinfo.PW))
    #
    # # Index name and document type
    # index_name = esinfo.sea_save_index
    # doc_type = '_doc'
    # index_exists = es.indices.exists(index=index_name)
    #
    # predict_bdi_data = predict_bdi()
    # predict_ccfi_data = predict_ccfi()
    # predict_kcci_data = predict_kcci()
    # predict_hrci_data = predict_hrci()
    # predict_scfi_data = predict_scfi()
    #
    # # Delete the index
    # delete_index(esinfo.sea_save_index)
    # logger.info("데이터 수정/삽입을 위해 기존의 해상인덱스 인덱스가 제거되었습니다.")
    #
    # # Insert generated data
    # try:
    #     insert_bdi(predict_bdi_data)
    #     logger.info("bdi 데이터가 입력 되었습니다.")
    # except Exception as e:
    #     logger.error("bdi 데이터 삽입에 오류가 발생하였습니다.")
    #
    # try:
    #     insert_ccfi(predict_ccfi_data)
    #     logger.info("ccfi 데이터가 입력 되었습니다.")
    # except Exception as e:
    #     logger.error("ccfi 데이터 삽입에 오류가 발생하였습니다.")
    #
    # try:
    #     insert_kcci(predict_kcci_data)
    #     logger.info("kcci 데이터가 입력 되었습니다.")
    # except Exception as e:
    #     logger.error("kcci 데이터 삽입에 오류가 발생하였습니다.")
    #
    # try:
    #     insert_hrci(predict_hrci_data)
    #     logger.info("hrdi 데이터가 입력 되었습니다.")
    # except Exception as e:
    #     logger.error("hrdi 데이터 삽입에 오류가 발생하였습니다.")
    #
    # try:
    #     insert_scfi(predict_scfi_data)
    #     logger.info("scfi 데이터가 입력 되었습니다.")
    # except Exception as e:
    #     logger.error("scfi 데이터 삽입에 오류가 발생하였습니다.")



    # 해상운임 예측 내용 저장
    from models.sea_freight.service import insert_sea_freight, dict_save_data

    try:
        insert_sea_freight(esinfo.sea_save_freight, dict_save_data)
        logger.info("해상운임 예측 값 전체가 입력되었습니다.")
    except Exception as e:
        logger.info("해상운임 예측값 삽입에 오류가 발생하였습니다.")