import json
import pandas as pd
from elasticsearch import Elasticsearch, helpers
from models.sea_freight_index.hrci_p.model import pred_hrci_model
from common.utils.setting import EsSetting

import logging
logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Connect to Elasticsearch
esinfo = EsSetting()
es = Elasticsearch(esinfo.IP, basic_auth=(esinfo.ID, esinfo.PW))

# Index name and document type
index_name = esinfo.sea_save_index
doc_type = '_doc'


# make predicted data
def predict_hrci():
    data = pred_hrci_model()
    data = data.dropna()
    df_data_source = pd.DataFrame({
                            "data_cd": "hrci",
                            "rgsr_dt": data.index,
                            "cach_expo": data['hrci_cach_expo']
                                }
                            )
    # make data to adaptable doc_type for es
    df_data = []
    for index,row in df_data_source.iterrows():
        df_data.append({"_index":index_name,
                        "_source": {
                            "data_cd": "hrci",
                            "rgsr_dt": row['rgsr_dt'],
                            "cach_expo": row['cach_expo']
                                }})
    return df_data

def insert_hrci(df_data):
    if not es.indices.exists(index=index_name):
        # Create the index
        es.indices.create(index=index_name)
        logger.info("인덱스가 생성되었습니다.")
    # Insert data into Elasticsearch
    helpers.bulk(es,df_data)


# Delete the index
# response = es.indices.delete(index=index_name)

# response = es.index(index=index_name, doc_type=doc_type, body=query)

#Check if the insertion was successful
# if response['result'] == 'created':
#     print('Data inserted successfully.')
# else:
#     print('Failed to insert data.')