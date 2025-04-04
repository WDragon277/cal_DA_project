from models.insight_g.model import ChatService
from common.utils.setting import PostgreSQL

upload_service = ChatService()

rdb_info = PostgreSQL()

import datetime
# content = content_list[0]
current_time = datetime.datetime.now()
current_day = str(current_time.year)+str(current_time.month)+str(current_time.day)
list_indx_no = ['001', '002', '003', '004', '005']
expo_cd = ["KCCI", "SCFI", "CCFI", "HRCI", "BDI"]
expo_cd_nm =['KOBC Container Composite Index', 'Shanghai Containerized Freight Index',
             'China Containerized Freight Index', 'Howe Robinson Container Index',
             'Baltic Dry Index']
# sea_idx_nm
# sea_idx_cd
# sea_idx_no

cach_tp_cd = ['CNTR', 'FLGH_IMP', 'FLGH_EXP', 'SEA_IMP', 'SEA_EXP']

##

data_kcci = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : cach_tp_cd[0],
    'expo_cd': expo_cd[0],
    'expo_cd_nm': expo_cd_nm[0],
    'ai_anay_desc' : '''최근 KCCI는 지속적인 상승 추세를 보이고 있습니다. 
특히 2024년 초부터 급격한 상승을 시작하여 8월까지 높은 수준을 유지하며, 
전반적으로 국제 해운 시장의 활성화가 이러한 추세 변화에 영향을 미치고 있음을 알 수 있습니다. 
하지만 최근 몇 주간 KCCI는 감소하는 추세를 보이고 있으며, 앞으로 수출입 업체들은 해상 운송 비용의 
변동 상황을 주시해야 합니다. ''',
    'cach_tp_no' : list_indx_no[0],
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_kcci)

##

data_scfi = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : cach_tp_cd[0],
    'expo_cd': expo_cd[1],
    'expo_cd_nm': expo_cd_nm[1],
    'ai_anay_desc' : '''최근 SCFI 지표는 하락세를 보이며 
2024년 11월 현재 약 2250대 수준으로 안정되어 있습니다.  
지난 해 초반에는 5000점 이상을 기록했던 SCFI가 주요 경제 불확실성과 글로벌 물류 부동에 의해 지속적으로 하락세를 
보였다는 분석이 가능합니다. 이러한 추세는 수출입 업체들에게 운임 절감의 기회를 제공할 수 있습니다.  
향후 SCFI가 어떻게 변화할지 주시하면서, 효율적인 운송 전략을 세우는 것이 중요합니다. ''',
    'cach_tp_no' : list_indx_no[1],
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_scfi)

##

data_ccfi = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : cach_tp_cd[0],
    'expo_cd': expo_cd[2],
    'expo_cd_nm': expo_cd_nm[2],
    'ai_anay_desc' : '''CCFI는 최근 몇 개월 동안 하락세를 보이고 있습니다. 
2022년 초부터 약 3,500대에서 시작된 CCFI는 점차 하락 추세로 변했습니다. 
특히 2023년 후반부터는 더욱 큰 하락폭을 보이며 현재는 
1,400대에 머물고 있습니다. 이러한 CCFI 하락은 글로벌 물류 시장의 여전히 불안정한 상황과 
수출입 물량 감소 등 다양한 요인의 영향으로 분석됩니다.  수출입 업체들은 이러한 CCFI 추세를 
고려하여 운송 비용 예측 및 계획 수립에 신경 써야 합니다.''',
    'cach_tp_no' : list_indx_no[2],
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_ccfi)

##

data_hrci = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : cach_tp_cd[0],
    'expo_cd': expo_cd[3],
    'expo_cd_nm': expo_cd_nm[3],
    'ai_anay_desc' : '''수출입 업체를 위한 분석 결과, 최근 HRCI 데이터는 하락 추세에 있습니다. 
특히 2022년 초반부터 HRCI가 상승하던 모습에서 뚜렷한 감소세를 보이며 2024년 말 기준으로 낮은 
수치를 나타내고 있습니다. 이러한 변화는 글로벌 화물 수요 감소와 해운 운임 하락 등의 요인이 반영될 
가능성이 높습니다.  수출입 업체들은 이러한 HRCI 추세를 고려하여 물류 계획을 조정하고, 
원재료 및 제품 가격 변동에 대비하는 노력이 필요할 것으로 예상됩니다. ''',
    'cach_tp_no' : list_indx_no[3],
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_hrci)

##

data_bdi = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : cach_tp_cd[0],
    'expo_cd': expo_cd[4],
    'expo_cd_nm': expo_cd_nm[4],
    'ai_anay_desc' : '''최근 데이터에 따르면 BDI 지수는 1804점으로 나타났습니다. 
이는 지난달 대비 3% 증가를 보이며 상승 추세입니다. 
물가지수 또한 2% 상승했고, 생산지수는 1% 하락했습니다. 소득지수는 0.5% 증가했으며, 
경제활동지수는 1.2% 상승하는 등 전반적으로 경제 성장 지표들이 우상향 추세입니다.
최근 데이터를 분석해보면 BDI 지수의 상승은 
물가 및 소득지수의 인상적인 증가로 이끌어진 것으로 분석됩니다. 
전반적으로 경제활동이 활발하며 지속적인 성장 가능성이 높아 보입니다.''',
    'cach_tp_no' : list_indx_no[4],
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_bdi)
