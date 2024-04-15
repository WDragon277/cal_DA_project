# 모델 불러와서 엘라스틱서치에 삽입하는 함수 제작
from elasticsearch import Elasticsearch, helpers
import logging

from models.air_freight.exp.asia.model import pred_freight_model
from models.air_freight.exp.exp_repository import kuala_exp_data, singapor_exp_data, hongkong_exp_data, \
    hochimin_exp_data, penang_exp_data, kansai_exp_data, narita_exp_data, nagoya_exp_data
from common.utils.setting import EsSetting

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


# 예측값 생성 및 데이터 삽입을 위한 정제

# 모델 사용 해서 예측값 생성

def save_asia_pred_exp():
    kuala_exp_data_cp = kuala_exp_data.copy()
    pred_kuala_exp = pred_freight_model(kuala_exp_data_cp)
    pred_kuala_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']] = pred_kuala_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']].ffill()
    # 생성된 인덱스(날짜)를 날짜 칼럼에 넣고 엘라스틱서치에 삽입할 수 있도록 데이터 타입을 periodindex에서 str로 변경
    pred_kuala_exp['year_mon'] = pred_kuala_exp.index.strftime('%Y%m')

    # 엘라스틱서치 인덱스에 입력하기 위한 데이터 변경작업
    pred_kuala_exp_js = []
    for index, row in pred_kuala_exp.iterrows():
        pred_kuala_exp_js.append({"_index": index_name,
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

    singapor_exp_data_cp= singapor_exp_data.copy()
    pred_singapor_exp = pred_freight_model(singapor_exp_data_cp)
    pred_singapor_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']] = pred_singapor_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']].ffill()
    pred_singapor_exp['year_mon'] = pred_singapor_exp.index.strftime('%Y%m')
    pred_singapor_exp_js = []
    for index, row in pred_singapor_exp.iterrows():
        pred_singapor_exp_js.append({"_index": index_name,
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

    hongkong_exp_data_cp = hongkong_exp_data.copy()
    pred_hongkong_exp = pred_freight_model(hongkong_exp_data_cp)
    pred_hongkong_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']] = pred_hongkong_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']].ffill()
    pred_hongkong_exp['year_mon'] = pred_hongkong_exp.index.strftime('%Y%m')
    pred_hongkong_exp_js = []
    for index, row in pred_hongkong_exp.iterrows():
        pred_hongkong_exp_js.append({"_index": index_name,
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

    hochimin_exp_data_cp = hochimin_exp_data.copy()
    pred_hochimin_exp = pred_freight_model(hochimin_exp_data_cp)
    pred_hochimin_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']] = pred_hochimin_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']].ffill()
    pred_hochimin_exp['year_mon'] = pred_hochimin_exp.index.strftime('%Y%m')
    pred_hochimin_exp_js = []
    for index, row in pred_hochimin_exp.iterrows():
        pred_hochimin_exp_js.append({"_index": index_name,
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

    penang_exp_data_cp = penang_exp_data.copy()
    pred_penang_exp = pred_freight_model(penang_exp_data_cp)
    pred_penang_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']] = pred_penang_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']].ffill()
    pred_penang_exp['year_mon'] = pred_penang_exp.index.strftime('%Y%m')
    pred_penang_exp_js = []
    for index, row in pred_penang_exp.iterrows():
        pred_penang_exp_js.append({"_index": index_name,
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

    kansai_exp_data_cp= kansai_exp_data.copy()
    pred_kansai_exp = pred_freight_model(kansai_exp_data_cp)
    pred_kansai_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']] = pred_kansai_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']].ffill()
    pred_kansai_exp['year_mon'] = pred_kansai_exp.index.strftime('%Y%m')
    pred_kansai_exp_js = []
    for index, row in pred_kansai_exp.iterrows():
        pred_kansai_exp_js.append({"_index": index_name,
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

    narita_exp_data_cp= narita_exp_data.copy()
    pred_narita_exp = pred_freight_model(narita_exp_data_cp)
    pred_narita_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']] = pred_narita_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']].ffill()
    pred_narita_exp['year_mon'] = pred_narita_exp.index.strftime('%Y%m')
    pred_narita_exp_js = []
    for index, row in pred_narita_exp.iterrows():
        pred_narita_exp_js.append({"_index": index_name,
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

    nagoya_exp_data_cp= nagoya_exp_data.copy()
    pred_nagoya_exp = pred_freight_model(nagoya_exp_data_cp)
    pred_nagoya_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']] = pred_nagoya_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']].ffill()
    pred_nagoya_exp['year_mon'] = pred_nagoya_exp.index.strftime('%Y%m')
    pred_nagoya_exp_js = []
    for index, row in pred_nagoya_exp.iterrows():
        pred_nagoya_exp_js.append({"_index": index_name,
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
        insert_air_pred(pred_kuala_exp_js)
        logger.info("쿠알라룸푸르 수출운임예측 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("쿠알라룸푸르 수출운임예측 데이터 삽입에 오류가 발생하였습니다.")

    try:
        insert_air_pred(pred_singapor_exp_js)
        logger.info("싱가포르 수출운임예측 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("싱가포르 수출운임예측 데이터 삽입에 오류가 발생하였습니다.")

    try:
        insert_air_pred(pred_hongkong_exp_js)
        logger.info("홍콩 수출운임예측 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("홍콩 수출운임예측 데이터 삽입에 오류가 발생하였습니다.")

    try:
        insert_air_pred(pred_hochimin_exp_js)
        logger.info("호치민 수출운임예측 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("호치민 수출운임예측 데이터 삽입에 오류가 발생하였습니다.")

    try:
        insert_air_pred(pred_penang_exp_js)
        logger.info("페낭 수출운임예측 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("페낭 수출운임예측 데이터 삽입에 오류가 발생하였습니다.")

    try:
        insert_air_pred(pred_kansai_exp_js)
        logger.info("간사이 수출운임예측 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("간사이 수출운임예측 데이터 삽입에 오류가 발생하였습니다.")

    try:
        insert_air_pred(pred_narita_exp_js)
        logger.info("나리타 수출운임예측 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("나리타 수출운임예측 데이터 삽입에 오류가 발생하였습니다.")

    try:
        insert_air_pred(pred_nagoya_exp_js)
        logger.info("나고야 수출운임예측 데이터가 입력 되었습니다.")
    except Exception as e:
        logger.error("나고야 수출운임예측 데이터 삽입에 오류가 발생하였습니다.")


