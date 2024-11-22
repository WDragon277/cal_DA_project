import sys
sys.stdout.reconfigure(encoding='utf-8')  # 터미널 출력 인코딩을 UTF-8로 설정
import os
os.environ["PYTHONIOENCODING"] = "utf-8"  # 출력 및 입력 기본 인코딩 설정


class EsSetting:

    def __init__(self):
        # self.IP = 'http://121.138.113.10:35602' # for kibana Ops
        # self.IP = 'http://121.138.113.10:35603' # for kibana Dev
        # self.IP = 'http://121.138.113.10:39203' # for Dev
        self.IP = 'http://121.138.113.10:39202' # for Ops
        self.ID = 'elastic'
        self.PW = '1q2w3e4r5t'
        self.sea_read_index = 'cal_idx_expo_lst'
        # self.sea_save_index = 'dgl_idx_expo_pred_lst' # tmp index
        self.sea_save_index = 'cal_idx_expo_pred_lst'
        self.sea_read_freight = 'cal_idx_kcla_sea_cach_lst'
        self.sea_save_freight = 'cal_idx_kcla_sea_cach_pred_lst'
        self.air_read_index = 'cal_idx_kcla_air_cach_lst'
        self.air_save_index = 'cal_idx_air_pred_lst'
        self.pds_read_index = 'cal_idx_koreapds_lst'
        self.usd_read_index = 'cal_idx_usdx_data'
        self.tmp = 'dgl_idx_expo_lst'


class PostgreSQL:

    def __init__(self):
        self.IP = '121.138.113.10'
        self.port = '35432'
        self.ID = 'agens'
        self.PW = 'agens'
        self.dbname_cheonan = 'cheonandb'
        self.tbname_cheonan = 'tb_sea_cach_anay_dtl'

0

    # def sea_freight_index(self):
    #     self.read_index = 'cal_idx_expo_lst'
    #     self.save_index = 'cal_idx_expo_pred_lst'
    #
    #
    # def air_freight(self):
    #     self.read_index = 'cal_idx_kcla_air_cach_lst'
    #     self.save_index = 'cgl_idx_kcla_air_cach_pred_lst'
    #     #self.save_index = 'dgl_idx_kcla_air_cach_pred_lst'