import pandas as pd
from elasticsearch import Elasticsearch, helpers
from common.utils.setting import EsSetting


class model_common_settings():

    def __init__(self):
        self.lag = 7