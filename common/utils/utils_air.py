import pandas as pd
from elasticsearch import Elasticsearch, helpers
from common.utils.setting import EsSetting


class ModelCommonSettings:

    def __init__(self):
        self.lag = 6
        self.period = 6