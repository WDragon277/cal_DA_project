from elasticsearch import Elasticsearch, helpers
import logging

from common.utils.setting import EsSetting
from common.utils.utils import searchAPI

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

esinfo = EsSetting()
test_air = searchAPI(esinfo.air_save_index)
test_sea = searchAPI(esinfo.sea_save_index)
