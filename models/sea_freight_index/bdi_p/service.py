import pandas as pd
from elasticsearch import Elasticsearch, helpers
from models.sea_freight_index.bdi_p.model import pred_bdi_model
from common.utils.setting import EsSetting


import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Connect to Elasticsearch
esinfo = EsSetting()
es = Elasticsearch(esinfo.IP, basic_auth=(esinfo.ID, esinfo.PW))

# Index name and document type
index_name = esinfo.sea_save_index
doc_type = '_doc'

# make predicted data
def predict_bdi():

    data = pred_bdi_model()
    data = data.dropna()
    df_data_source = pd.DataFrame({
                            "data_cd": "bdi",
                            "rgsr_dt": data.index,
                            "cach_expo": data['bdi_cach_expo']
                                }
                            )

    # make data to adaptable doc_type for es
    dict_data = []
    for index, row in df_data_source.iterrows():
        dict_data.append({"_index": index_name,
                        "_source": {
                            "data_cd": "bdi",
                            "rgsr_dt": row['rgsr_dt'],
                            "cach_expo": row['cach_expo']
                                }})
    return dict_data


def insert_bdi(dict_data):

    if not es.indices.exists(index=index_name):
        # Create the index
        es.indices.create(index=index_name)
        logger.info("새로운 인덱스가 생성되었습니다.")

    # Insert data into Elasticsearch
    helpers.bulk(es, dict_data)

