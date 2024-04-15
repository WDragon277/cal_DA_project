# 모델 불러와서 엘라스틱서치에 삽입하는 함수 제작
from elasticsearch import Elasticsearch, helpers
import logging

from models.air_freight.exp.china.model import pred_freight_model
from models.air_freight.exp.exp_repository import pudong_exp_data, tianjin_exp_data, qingdao_exp_data, \
    hangzhou_exp_data, beijing_exp_data, guangzhou_exp_data
from common.utils.setting import EsSetting
from common.utils.utils import searchAPI

esinfo = EsSetting()

# Ativate logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Connect to Elasticsearch
es = Elasticsearch('http://121.138.113.10:39202', basic_auth=('elastic', '1q2w3e4r5t')) #ops

# Index name for saving data and document type
index_name = esinfo.air_save_index
doc_type = '_doc'
index_exists = es.indices.exists(index=index_name)


def insert_air_pred(df_data):

    if not es.indices.exists(index=index_name):
        # Create the index
        es.indices.create(index=index_name)
        logger.info("새로운 인덱스가 생성되었습니다.")

    # Insert data into Elasticsearch
    helpers.bulk(es, df_data)

def save_china_pred_exp():

    # 모델 사용 해서 예측값 생성
    pudong_exp_data_cp = pudong_exp_data.copy()
    pred_pudong_exp = pred_freight_model(pudong_exp_data_cp)
    pred_pudong_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']] = pred_pudong_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']].ffill()
    # 생성된 인덱스(날짜)를 날짜 칼럼에 넣고 엘라스틱서치에 삽입할 수 있도록 데이터 타입을 periodindex에서 str로 변경
    pred_pudong_exp['year_mon'] = pred_pudong_exp.index.strftime('%Y%m')

    # 엘라스틱서치 인덱스에 입력하기 위한 데이터 변경작업
    pred_pudong_exp_js = []
    for index, row in pred_pudong_exp.iterrows():
        pred_pudong_exp_js.append({"_index": index_name,
                                  "_source": {
                                      "data_cd": row["data_cd"],
                                      "dptr_cnty": row["dptr_cnty"],
                                      "arvl_cnty": row["arvl_cnty"],
                                      "year_mon": row['year_mon'],
                                      "cach_45k_amt": row['cach_45k_amt'],
                                      "cach_100k_amt": row['cach_100k_amt'],
                                      "cach_300k_amt": row['cach_300k_amt'],
                                      "cach_500k_amt": row['cach_500k_amt'],
                                      "cach_1000k_amt": row['cach_1000k_amt']
                                  }})

    tianjin_exp_data_cp= tianjin_exp_data.copy()
    pred_tianjin_exp_ = pred_freight_model(tianjin_exp_data_cp)
    pred_tianjin_exp_[['data_cd', 'dptr_cnty', 'arvl_cnty']] = pred_tianjin_exp_[['data_cd', 'dptr_cnty', 'arvl_cnty']].ffill()
    pred_tianjin_exp_['year_mon'] = pred_tianjin_exp_.index.strftime('%Y%m')
    pred_tianjin_exp_js = []
    for index, row in pred_tianjin_exp_.iterrows():
        pred_tianjin_exp_js.append({"_index": index_name,
                                  "_source": {
                                      "data_cd": row["data_cd"],
                                      "dptr_cnty": row["dptr_cnty"],
                                      "arvl_cnty": row["arvl_cnty"],
                                      "year_mon": row['year_mon'],
                                      "cach_45k_amt": row['cach_45k_amt'],
                                      "cach_100k_amt": row['cach_100k_amt'],
                                      "cach_300k_amt": row['cach_300k_amt'],
                                      "cach_500k_amt": row['cach_500k_amt'],
                                      "cach_1000k_amt": row['cach_1000k_amt']
                                  }})

    qingdao_exp_data_cp = qingdao_exp_data.copy()
    pred_qingdao_exp = pred_freight_model(qingdao_exp_data_cp)
    pred_qingdao_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']] = pred_qingdao_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']].ffill()
    pred_qingdao_exp['year_mon'] = pred_qingdao_exp.index.strftime('%Y%m')
    pred_qingdao_exp_js = []
    for index, row in pred_qingdao_exp.iterrows():
        pred_qingdao_exp_js.append({"_index": index_name,
                                  "_source": {
                                      "data_cd": row["data_cd"],
                                      "dptr_cnty": row["dptr_cnty"],
                                      "arvl_cnty": row["arvl_cnty"],
                                      "year_mon": row['year_mon'],
                                      "cach_45k_amt": row['cach_45k_amt'],
                                      "cach_100k_amt": row['cach_100k_amt'],
                                      "cach_300k_amt": row['cach_300k_amt'],
                                      "cach_500k_amt": row['cach_500k_amt'],
                                      "cach_1000k_amt": row['cach_1000k_amt']
                                  }})

    hangzhou_exp_data_cp = hangzhou_exp_data.copy()
    pred_hangzhou_exp = pred_freight_model(hangzhou_exp_data_cp)
    pred_hangzhou_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']] = pred_hangzhou_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']].ffill()
    pred_hangzhou_exp['year_mon'] = pred_hangzhou_exp.index.strftime('%Y%m')
    pred_hangzhou_exp_js = []
    for index, row in pred_hangzhou_exp.iterrows():
        pred_hangzhou_exp_js.append({"_index": index_name,
                                  "_source": {
                                      "data_cd": row["data_cd"],
                                      "dptr_cnty": row["dptr_cnty"],
                                      "arvl_cnty": row["arvl_cnty"],
                                      "year_mon": row['year_mon'],
                                      "cach_45k_amt": row['cach_45k_amt'],
                                      "cach_100k_amt": row['cach_100k_amt'],
                                      "cach_300k_amt": row['cach_300k_amt'],
                                      "cach_500k_amt": row['cach_500k_amt'],
                                      "cach_1000k_amt": row['cach_1000k_amt']
                                  }})

    beijing_exp_data_cp = beijing_exp_data.copy()
    pred_beijing_exp = pred_freight_model(beijing_exp_data_cp)
    pred_beijing_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']] = pred_beijing_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']].ffill()
    pred_beijing_exp['year_mon'] = pred_beijing_exp.index.strftime('%Y%m')
    pred_beijing_exp_js = []
    for index, row in pred_beijing_exp.iterrows():
        pred_beijing_exp_js.append({"_index": index_name,
                                  "_source": {
                                      "data_cd": row["data_cd"],
                                      "dptr_cnty": row["dptr_cnty"],
                                      "arvl_cnty": row["arvl_cnty"],
                                      "year_mon": row['year_mon'],
                                      "cach_45k_amt": row['cach_45k_amt'],
                                      "cach_100k_amt": row['cach_100k_amt'],
                                      "cach_300k_amt": row['cach_300k_amt'],
                                      "cach_500k_amt": row['cach_500k_amt'],
                                      "cach_1000k_amt": row['cach_1000k_amt']
                                  }})

    guangzhou_exp_data_cp = guangzhou_exp_data.copy()
    pred_guangzhou_exp = pred_freight_model(guangzhou_exp_data_cp)
    pred_guangzhou_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']] = pred_guangzhou_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']].ffill()
    pred_guangzhou_exp['year_mon'] = pred_guangzhou_exp.index.strftime('%Y%m')
    pred_guangzhou_exp_js = []
    for index, row in pred_guangzhou_exp.iterrows():
        pred_guangzhou_exp_js.append({"_index": index_name,
                                  "_source": {
                                      "data_cd": row["data_cd"],
                                      "dptr_cnty": row["dptr_cnty"],
                                      "arvl_cnty": row["arvl_cnty"],
                                      "year_mon": row['year_mon'],
                                      "cach_45k_amt": row['cach_45k_amt'],
                                      "cach_100k_amt": row['cach_100k_amt'],
                                      "cach_300k_amt": row['cach_300k_amt'],
                                      "cach_500k_amt": row['cach_500k_amt'],
                                      "cach_1000k_amt": row['cach_1000k_amt']
                                  }})

    # Insert generated data
    try:
        insert_air_pred(pred_pudong_exp_js)
        logger.info("푸동 수출운임예측 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("푸동 수출운임예측 데이터 삽입에 오류가 발생하였습니다.")

    try:
        insert_air_pred(pred_tianjin_exp_js)
        logger.info("톈진 수출운임예측 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("톈진 수출운임예측 데이터 삽입에 오류가 발생하였습니다.")

    try:
        insert_air_pred(pred_qingdao_exp_js)
        logger.info("칭다오 수출운임예측 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("칭다오 수출운임예측 데이터 삽입에 오류가 발생하였습니다.")

    try:
        insert_air_pred(pred_hangzhou_exp_js)
        logger.info("항저우 수출운임예측 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("항저우 수출운임예측 데이터 삽입에 오류가 발생하였습니다.")

    try:
        insert_air_pred(pred_beijing_exp_js)
        logger.info("베이징 수출운임예측 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("베이징 수출운임예측 데이터 삽입에 오류가 발생하였습니다.")

    try:
        insert_air_pred(pred_guangzhou_exp_js)
        logger.info("광저우 수출운임예측 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("광저우 수출운임예측 데이터 삽입에 오류가 발생하였습니다.")
