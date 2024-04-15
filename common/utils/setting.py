class EsSetting:

    def __init__(self):
        # self.IP = 'http://121.138.113.10:35602' # for kibana Ops
        # self.IP = 'http://121.138.113.10:35603' # for kibana Dev
        # self.IP = 'http://121.138.113.10:39203' # for Dev
        self.IP = 'http://121.138.113.10:39202' # for Ops
        self.ID = 'elastic'
        self.PW = '1q2w3e4r5t'
        self.sea_read_index = 'cal_idx_expo_lst'
        self.sea_save_index = 'cal_idx_sea_pred_lst'
        self.air_read_index = 'cal_idx_kcla_air_cach_lst'
        self.air_save_index = 'cal_idx_air_pred_lst'
        self.pds_read_index = 'cal_idx_koreapds_lst'
        self.usd_read_index = 'cal_idx_usdx_data'
        self.tmp = 'dgl_idx_expo_lst'


    # def sea_freight(self):
    #     self.read_index = 'cal_idx_expo_lst'
    #     self.save_index = 'cal_idx_expo_pred_lst'
    #
    #
    # def air_freight(self):
    #     self.read_index = 'cal_idx_kcla_air_cach_lst'
    #     self.save_index = 'cgl_idx_kcla_air_cach_pred_lst'
    #     #self.save_index = 'dgl_idx_kcla_air_cach_pred_lst'