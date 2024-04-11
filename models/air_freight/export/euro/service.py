# 모델 불러와서 엘라스틱서치에 삽입하는 함수 제작
from elasticsearch import Elasticsearch, helpers
import logging

from models.air_freight.export.china.model import pred_freight_model
from models.air_freight.export.exp_repository import frankfrut_exp_data
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


# 예측값 생성 및 데이터 삽입을 위한 정제

# 모델 사용 해서 예측값 생성
frankfrut_exp_data_cp = frankfrut_exp_data.copy()
pred_frankfrut_exp = pred_freight_model(frankfrut_exp_data_cp)
pred_frankfrut_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']] = pred_frankfrut_exp[['data_cd', 'dptr_cnty', 'arvl_cnty']].ffill()
# 생성된 인덱스(날짜)를 날짜 칼럼에 넣고 엘라스틱서치에 삽입할 수 있도록 데이터 타입을 periodindex에서 str로 변경
pred_frankfrut_exp['year_mon'] = pred_frankfrut_exp.index.strftime('%Y%m')

# 엘라스틱서치 인덱스에 입력하기 위한 데이터 변경작업
pred_frankfrut_exp_js = []
for index, row in pred_frankfrut_exp.iterrows():
    pred_frankfrut_exp_js.append({"_index": index_name,
                              "_source": {
                                  "data_cd": 'kcla_exp_air',
                                  "dptr_cnty": 'Incheon',
                                  "arvl_cnty": 'Kuala Lumpur (KUL)',
                                  "year_mon": row['year_mon'],
                                  "cach_45k_amt": row['cach_45k_amt'],
                                  "cach_100k_amt": row['cach_100k_amt'],
                                  "cach_300k_amt": row['cach_300k_amt'],
                                  "cach_500k_amt": row['cach_500k_amt'],
                                  "cach_1000k_amt": row['cach_1000k_amt']
                              }})


if index_exists:
    es.indices.delete(index=index_name)
    logger.info("데이터 수정/삽입을 위해 기존의 인덱스가 제거되었습니다.")

# Insert generated data
try:
    insert_air_pred(pred_frankfrut_exp_js)
    logger.info("pred_frankfrut_exp_js 데이터가 입력 되었습니다.")
except Exception as e:
    logger.error("pred_frankfrut_exp_js 데이터 삽입에 오류가 발생하였습니다.")
