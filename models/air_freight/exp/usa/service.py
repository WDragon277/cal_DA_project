# 모델 불러와서 엘라스틱서치에 삽입하는 함수 제작
from elasticsearch import Elasticsearch, helpers
import logging

from models.air_freight.exp.china.model import pred_freight_model
from models.air_freight.exp.exp_repository import la_exp_data, newyork_exp_data, chicago_exp_data, \
    sfo_exp_data, atl_exp_data
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


def save_usa_pred_exp():
    # 모델 사용 해서 예측값 생성
    la_exp_data_cp = la_exp_data.copy()
    pred_la_exp = pred_freight_model(la_exp_data_cp)
    pred_la_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']] = pred_la_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']].ffill()
    # 생성된 인덱스(날짜)를 날짜 칼럼에 넣고 엘라스틱서치에 삽입할 수 있도록 데이터 타입을 periodindex에서 str로 변경
    pred_la_exp['year_mon'] = pred_la_exp.index.strftime('%Y%m')

    # 엘라스틱서치 인덱스에 입력하기 위한 데이터 변경작업
    pred_la_exp_js = []
    for index, row in pred_la_exp.iterrows():
        pred_la_exp_js.append({"_index": index_name,
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

    newyork_exp_data_cp= newyork_exp_data.copy()
    pred_newyork_exp = pred_freight_model(newyork_exp_data_cp)
    pred_newyork_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']] = pred_newyork_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']].ffill()
    pred_newyork_exp['year_mon'] = pred_newyork_exp.index.strftime('%Y%m')
    pred_newyork_exp_js = []
    for index, row in pred_newyork_exp.iterrows():
        pred_newyork_exp_js.append({"_index": index_name,
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

    chicago_exp_data_cp = chicago_exp_data.copy()
    pred_chicago_exp = pred_freight_model(chicago_exp_data_cp)
    pred_chicago_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']] = pred_chicago_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']].ffill()
    pred_chicago_exp['year_mon'] = pred_chicago_exp.index.strftime('%Y%m')
    pred_chicago_exp_js = []
    for index, row in pred_chicago_exp.iterrows():
        pred_chicago_exp_js.append({"_index": index_name,
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

    sfo_exp_data_cp = sfo_exp_data.copy()
    pred_sfo_exp = pred_freight_model(sfo_exp_data_cp)
    pred_sfo_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']] = pred_sfo_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']].ffill()
    pred_sfo_exp['year_mon'] = pred_sfo_exp.index.strftime('%Y%m')
    pred_sfo_exp_js = []
    for index, row in pred_sfo_exp.iterrows():
        pred_sfo_exp_js.append({"_index": index_name,
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

    atl_exp_data_cp = atl_exp_data.copy()
    pred_atl_exp = pred_freight_model(atl_exp_data_cp)
    pred_atl_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']] = pred_atl_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']].ffill()
    pred_atl_exp['year_mon'] = pred_atl_exp.index.strftime('%Y%m')
    pred_atl_exp_js = []
    for index, row in pred_atl_exp.iterrows():
        pred_atl_exp_js.append({"_index": index_name,
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
        insert_air_pred(pred_la_exp_js)
        logger.info("LA 수출운임예측 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("LA 수출운임예측 데이터 삽입에 오류가 발생하였습니다.")

    try:
        insert_air_pred(pred_newyork_exp_js)
        logger.info("뉴욕 수출운임예측 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("뉴욕 수출운임예측 데이터 삽입에 오류가 발생하였습니다.")

    try:
        insert_air_pred(pred_chicago_exp_js)
        logger.info("시카고 수출운임예측 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("시카고 수출운임예측 데이터 삽입에 오류가 발생하였습니다.")

    try:
        insert_air_pred(pred_sfo_exp_js)
        logger.info("샌프란시스코 수출운임예측 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("샌프란시스코 수출운임예측 데이터 삽입에 오류가 발생하였습니다.")

    try:
        insert_air_pred(pred_atl_exp_js)
        logger.info("애틀랜타 수출운임예측 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("애틀랜타 수출운임예측 데이터 삽입에 오류가 발생하였습니다.")
