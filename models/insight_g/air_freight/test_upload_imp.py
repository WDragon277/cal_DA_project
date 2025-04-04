from models.insight_g.model import ChatService
from common.utils.setting import PostgreSQL

upload_service = ChatService()

rdb_info = PostgreSQL()

import datetime
current_time = datetime.datetime.now()
current_day = current_time.strftime('%Y%m%d')

data_la = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_IMP',
    'expo_cd': 'Los Angeles',
    'expo_cd_nm': 'Import from Los Angeles',
    'ai_anay_desc' : '''LA에서 인천공항으로 운송되는 화물의 평균 운임은 최근 몇 년 동안 큰 변동을 보이고 있습니다. 2021년 초까지는 대체로 일정한 수준을 유지했지만, 그 이후로는 하락세를 보여주고 있습니다. 특히 2023년 들어서는 상당히 저렴해졌습니다.  이러한 추세는 글로벌 화물 운송 시장의 변화와 관련이 있을 가능성이 높으며, 수출입 업체들은 이를 고려하여 비용 계획을 수립해야 할 것으로 보입니다.''',
    'cach_tp_no' : '011',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_la)



data_newyork = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_IMP',
    'expo_cd': 'New York',
    'expo_cd_nm': 'Import from New York',
    'ai_anay_desc' : '''뉴욕에서 인천공항으로 날아오는 화물 운임은 2015년부터 지속적으로 변동을 보였습니다.  초반에는 2달러대를 유지하다가 2021년 이후로 큰 폭으로 하락하며 현재는 1달러대 초반의 수준입니다. 특히 최근에는 꾸준한 하락세를 보이고 있으며, 예상치는 앞으로도 낮아질 것으로 전망됩니다.  이는 글로벌 공급망 변화와 항공 운임 경쟁 심화 등 여러 요인이 복합적으로 작용하는 결과로 분석됩니다. 수출입 업체들은 이러한 추세를 고려하여 운송비 예산 계획을 세우는 것이 중요합니다.''',
    'cach_tp_no' : '014',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_newyork)


data_chicago = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_IMP',
    'expo_cd': 'Chicago',
    'expo_cd_nm': 'Import from Chicago',
    'ai_anay_desc' : '''시카고에서 인천공항으로 화물을 운송하는 평균 운임은 2015년에는 약 1.55달러였으며, 2021년까지는 일정 수준을 유지하면서 1.86~1.93달러 사이를 형성하고 있었습니다. 그러나 2021년 이후부터는 감소세로 들어가며 현재는 약 1.04 달러로 나타났습니다.  이는 글로벌 운송 시장 상황 변화와 연료 가격 변동 등 다양한 요인이 복합적으로 작용한 결과로 분석됩니다. 수출입 업체들은 이러한 추세를 지속적으로 관찰하고, 운임 예상 및 계획에 반영하여 효율적인 비용 관리 전략을 수립하는 것이 중요합니다.''',
    'cach_tp_no' : '003',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_chicago)

data_sfo = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_IMP',
    'expo_cd': 'San Francisco',
    'expo_cd_nm': 'Import from San Francisco',
    'ai_anay_desc' : '''샌프란시스코에서 인천공항으로 날아오는 화물 운임은 2015년에는  2달러 내외를 유지하다가 2017년 이후부터 일정 수준을 유지하며 변동폭이 작습니다. 특히 2021년과 2023년에 들어서는 평균 운임이 하락하는 추세를 보이며, 최근 예측 데이터에서는 2024년에도 일정 수준으로 유지될 것으로 전망됩니다.  이는 글로벌 화물 물류 시장의 변동성과 항공료 산정 방식에 영향을 받는 결과입니다.''',
    'cach_tp_no' : '018',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_sfo)

data_atl = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_IMP',
    'expo_cd': 'Atlanta',
    'expo_cd_nm': 'Import from Atlanta',
    'ai_anay_desc' : '''아틀라타에서 인천공항으로 날아오는 화물의 평균 운임은 꾸준히 변동하고 있습니다.  2015년부터 2020년까지는 대부분 1.94 USD로 안정적으로 유지되었지만, 2021년에는 하락하여 1.25 USD를 기록했고 이후 다시 상승세로 돌아섰습니다.  최근 데이터에 따르면 2023년 말 현재는 약 1.45 USD로 나타났으며, 2024년 초에는 다시 하락 추세를 보일 것으로 예상됩니다.  



수출입 업체들은 앞으로 발생할 수 있는 운임 변동성을 고려하여 사전에 물류 계획을 세우는 것이 중요합니다. 특히 최근 데이터를 기반으로 2024년 초에는 운임이 하락할 가능성이 높다는 점을 유의해야 합니다. ''',
    'cach_tp_no' : '001',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_atl)

data_pudong = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_IMP',
    'expo_cd': 'Pudong',
    'expo_cd_nm': 'Import from Pudong',
    'ai_anay_desc' : '''수출입 업체들은 앞으로 발생할 수 있는 운임 변동성을 고려하여 사전에 물류 계획을 세우는 것이 중요합니다. 특히 최근 데이터를 기반으로 2024년 초에는 운임이 하락할 가능성이 높다는 점을 유의해야 합니다.''',
    'cach_tp_no' : '016',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_pudong)

data_tianjin = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_IMP',
    'expo_cd': 'Tianjin',
    'expo_cd_nm': 'Import from Tianjin',
    'ai_anay_desc' : '''천진에서 인천공항으로 날아오는 화물의 평균 운임은 2015년 4월부터 꾸준히 변동을 보이며 최근에는 비교적 안정적인 수준을 유지하고 있습니다. 특히 2016년부터 2020년까지는 1.94 USD 근처에서 유지되었으며, 2021년부터는 다시 일정한 수치를 나타내고 있습니다. 최근 예측 데이터에 따르면, 2024년까지는  평균 운임이 약 2.4 USD로 유지될 것으로 예상됩니다.  결론적으로 천진에서 인천공항으로 화물을 운송하는 경우 최근 운임은 상대적으로 안정적인 수준이며, 앞으로도 비슷한 수준을 유지할 가능성이 높습니다.''',
    'cach_tp_no' : '020',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_tianjin)

data_qingdao = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_IMP',
    'expo_cd': 'Qingdao',
    'expo_cd_nm': 'Import from Qingdao',
    'ai_anay_desc' : '''칭다오에서 인천공항으로 날아오는 화물의 평균 운임은 2015년 초기에는 약 1.84 USD로 시작하여, 2016년 이후에는 대체로 1.23 USD 정도로 안정적인 수준을 유지했습니다. 하지만 최근 몇 년 동안 다시 상승세를 보이고 있으며, 2023년 말부터는 약 1.54 USD에서 시작하여 2024년 초에는 약 1.55 USD의 수준입니다. 특히 2022년 중순 이후부터는  지속적인 상승 추세를 보이고 있으며, 이러한 변화는 세계 경제 불안과 해상 운송 비용 상승 등 다양한 요인이 복합적으로 작용한 결과로 분석됩니다.''',
    'cach_tp_no' : '017',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_qingdao)

data_hangzhou = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_IMP',
    'expo_cd': 'Hangzhou',
    'expo_cd_nm': 'Import from Hangzhou',
    'ai_anay_desc' : '''항저우에서 인천공항으로 날아오는 화물의 평균 운임은 꾸준히 변동을 보이고 있습니다. 2015년부터 2021년까지는 대체로 1.6~1.7달러/kg  대 사이를 유지했고, 2022년부터는 급격하게 상승하여 현재는 2.4~2.6달러/kg대로 형성되어 있습니다. 특히 최근 데이터에서는 운임이 하락세를 보이고 있는 추세입니다. 이러한 변동은 글로벌 경제 상황, 유류 가격 변동, 그리고 시즌적인 요인 등 다양한 요소들이 복합적으로 작용하여 발생하는 결과라고 볼 수 있습니다.''',
    'cach_tp_no' : '006',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_hangzhou)

data_beijing = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_IMP',
    'expo_cd': 'Beijing',
    'expo_cd_nm': 'Import from Beijing',
    'ai_anay_desc' : '''베이징에서 인천공항으로 날아오는 화물의 평균 운임은 2020년 6월까지 꾸준히 2.53달러로 유지되다가 하락세를 보였습니다. 2020년 7월부터는 다시 2.53달러대에서 변동하며 올라오고 있으며, 2023년 10월 현재 약 2.43달러로 나타나고 있습니다.  예상치는 2024년에도 하락세를 이어갈 가능성이 높습니다.''',
    'cach_tp_no' : '002',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_beijing)

data_guangzhou = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_IMP',
    'expo_cd': 'Guangzhou',
    'expo_cd_nm': 'Import from Guangzhou',
    'ai_anay_desc' : '''광저우에서 인천공항으로 날아오는 화물의 평균 운임은 시간에 따라 변동을 보입니다. 2015년부터 2019년까지는 대부분 1.54 USD/kg로 안정적인 수준을 유지했습니다. 그러나 2019년 7월부터는 하락세를 보이며 1.12 USD/kg 이하로 떨어졌습니다. 2020년부터는 다시 상승 추세에 들어가 2021년에는 최대 3.795 USD/kg까지 치솟았습니다. 그 후 2022년부터는 점차 하락하며 현재는 2.36 USD/kg 수준입니다. 이러한 변동은 글로벌 시장 상황, 유류 가격, 여객 부문의 성과 등 다양한 요인에 영향을 받습니다. 향후 광저우에서 인천공항으로 운송되는 화물의 평균 운임은 지속적인 관찰이 필요하며,  시장 변화에 따라 상승하거나 하락할 수 있습니다.''',
    'cach_tp_no' : '005',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_guangzhou)

data_kuala = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_IMP',
    'expo_cd': 'Kuala',
    'expo_cd_nm': 'Import from Kuala Lumpur',
    'ai_anay_desc' : '''쿠알라룸푸르에서 인천공항으로 화물을 운송하는 평균 운임은 꾸준히 변동하고 있습니다.  특히 2019년 초부터는 하락 추세를 보였으며, 2020년에는 코로나19 팬데믹 이후 상승세로 돌아섰습니다. 2021년부터 2023년까지는 일정 수준을 유지하며 상승했지만 최근 데이터(2024년)에서는 다시 소폭 하락 추세를 보이고 있습니다.  이는 글로벌 경제 상황과 에너지 가격 변동, 그리고 운송 수요의 변화에 영향을 받는다는 것을 의미합니다.''',
    'cach_tp_no' : '010',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_kuala)

data_singapor = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_IMP',
    'expo_cd': 'Singapor',
    'expo_cd_nm': 'Import from Singapor',
    'ai_anay_desc' : '''싱가포르에서 인천공항으로 날아오는 화물 운임은 최근 몇 년간 큰 변동을 보이고 있습니다. 2015년까지는 평균 운임이 2달러대를 유지하다가, 2019년부터 점차 감소 추세로 들어섰습니다. 특히 코로나 팬데믹 이후에는 운임이 크게 하락했으며, 2020년과 2021년은 대부분 평균 1.5달러대로 유지되었습니다. 그러나 최근 몇 달간 다시 상승세에 접어들고 있습니다. 2023년 9월부터 운임이 2달러대를 돌파하며, 2024년 초에도 이 추세가 지속될 것으로 예상됩니다.''',
    'cach_tp_no' : '019',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_singapor)

data_hongkong = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_IMP',
    'expo_cd': 'Hongkong',
    'expo_cd_nm': 'Import from Hongkong',
    'ai_anay_desc' : '''홍콩에서 인천공항으로 날아오는 화물 운임은 지난 몇 년 동안 변동성을 보였습니다. 초기에는 1.2~1.4 달러 사이를 유지했지만, 2020년부터  COVID-19 팬데믹과 글로벌 공급망 불안정으로 인해 크게 상승하기 시작했습니다. 2020년 중반부터는 2달러대를 넘어섰고, 2021년에는 최대 3.2 달러까지 상승했습니다. 하지만 2023년 이후로는 다시 하락 추세에 접어들고 있으며 현재는 약 2.9~3.0 달러대입니다.  최근 데이터를 바탕으로 예상할 때, 앞으로의 운임은 지속적인 하락세를 보일 것으로 분석됩니다.''',
    'cach_tp_no' : '008',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_hongkong)

data_hochimin = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_IMP',
    'expo_cd': 'Hochimin',
    'expo_cd_nm': 'Import from Hochimin',
    'ai_anay_desc' : '''호치민에서 인천공항으로 날아오는 화물의 평균 운임은 2015년 초반에는 약 2.4 USD였으나, 2019년부터 급격하게 하락하여 1.57 USD까지 떨어졌습니다. 이후 2020년부터 다시 상승 추세를 보이며 현재는 약 3.61 USD로 유지되고 있습니다. 특히 2021년부터는 지속적인 상승세를 보이고 있으며, 2023년에는  3.49 ~ 3.61 USD 사이에서 운행되고 있습니다. 이러한 운임 변동은 전반적으로 세계적인 화물 수요 변화 및 연료 가격 변동 등 여러 요인에 영향을 받고 있는 것으로 보입니다. 

**결론:** 호치민에서 인천공항까지의 화물 운송 비용은 최근 상승 추세를 보이며, 수출입 기업들은 이러한 변화를 주시하고 계획에 반영해야 합니다.''',
    'cach_tp_no' : '007',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_hochimin)

data_penang = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_IMP',
    'expo_cd': 'Penang',
    'expo_cd_nm': 'Import from Penang',
    'ai_anay_desc' : '''페낭에서 인천공항으로 날아오는 화물의 평균 운임은 최근 몇 년 동안 변동을 보이고 있습니다. 2015년부터 2019년까지는 대체로 2달러 근처를 유지하였으며, 2020년부터는 코로나19 팬데믹 영향으로 상승세를 보이며, 2023년 현재 약 4.2 달러 수준입니다. 특히 2021년 이후에는 글로벌 공급망의 지속적인 불안정과 에너지 가격 상승으로 운임이 크게 증가한 추세를 보이고 있습니다.  




수출입 업체들은 앞으로도 페낭에서 인천공항까지 화물 운송 시 비용 변동성에 유의해야 합니다. ''',
    'cach_tp_no' : '015',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_penang)

data_kansai = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_IMP',
    'expo_cd': 'Kansai',
    'expo_cd_nm': 'Import from Kansai',
    'ai_anay_desc' : '''칸사이에서 인천공항으로 날아오는 화물의 평균 운임은 최근 몇 년 동안 변동성을 보이고 있습니다. 2019년 하반기부터 2021년까지는 상승세를 보였으며, 특히 코로나 팬데믹 이후 수요 증가로 인해 운임이 높아졌습니다. 그러나 2022년부터는 다시 감소하는 추세에 있으며, 현재는 1.2 USD 정도입니다.  이는 글로벌 공급망의 회복과 에너지 가격 하락 등으로 인한 것으로 분석됩니다. 앞으로 칸사이에서 인천공항으로 운송되는 화물의 평균 운임은 2024년까지 일정 수준을 유지할 것으로 예상되며, 변동성은 감소할 것으로 보입니다.''',
    'cach_tp_no' : '009',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_kansai)

data_narita = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_IMP',
    'expo_cd': 'Narita',
    'expo_cd_nm': 'Import from Narita',
    'ai_anay_desc' : '''나리타 공항에서 인천공항으로 화물을 운송하는 평균 운임 데이터를 분석해 보았습니다. 2015년부터 현재까지, 운임은 변동성이 크게 나타났습니다. 특히 2020년 ~ 2021년에는 COVID-19 팬데믹으로 인한 교통망 지연과 수요 변화로 평균 운임이 상승했습니다. 최근 몇 개월간, 하락세를 보이며 안정적인 추세로 돌아가고 있습니다. 

결론적으로, 나리타에서 인천공항까지의 화물 운임은 시장 변동과 글로벌 사건에 따라 큰 영향을 받습니다. 예측 모델을 통해 미래 운임 변화를 파악하는 것이 중요하며, 수출입 업체들은 이러한 추세 변화를 주시하여 사업 계획에 반영해야 합니다. ''',
    'cach_tp_no' : '013',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_narita)

data_nagoya = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_IMP',
    'expo_cd': 'Nagoya',
    'expo_cd_nm': 'Import from Nagoya',
    'ai_anay_desc' : '''한글 말해줘에서 인천공항으로 날아오는 화물의 평균 운임 데이터를 분석해보았습니다. 2015년부터 2023년까지의 데이터를 살펴봤을 때, 2020년 초반까지 운임은 대체로 1.2 달러 이상으로 유지되었습니다. 그러나 2020년 4월부터는 코로나19 팬데믹 영향으로 운임이 하락세로 돌아가면서 1달러 이하까지 떨어졌습니다.  2021년에는 다시 한 번 상승세를 보였지만, 2022년부터는 지속적으로 하락 추세에 있으며 현재는 0.8~0.9 달러대입니다. 최근 데이터는 예측치로, 향후 운임 변동에 대한 감시가 필요합니다.  


결론적으로, 한글 말해줘에서 인천공항으로 화물을 운송하는 경우 운임이 전반적으로 하락세를 보이고 있으며, 지속적인 감시가 필요합니다. ''',
    'cach_tp_no' : '012',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_nagoya)

data_frankfrut = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'FLGH_IMP',
    'expo_cd': 'Frankfrut',
    'expo_cd_nm': 'Import from Frankfrut',
    'ai_anay_desc' : ''''프랑크푸르트에서 인천공항으로 운송되는 화물의 평균 운임 데이터를 분석해보면, 2019년 하반기부터 꾸준히 하락하는 추세를 보입니다. 특히 2021년부터는 심각한 하락을 보이며, 최근에는 1달 미만으로까지 감소했습니다.  이는 글로벌 경제 상황 변화와 물류 산업의 불안정성에 영향을 받은 것으로 분석됩니다. 
수출입 업체들은 이러한 추세를 고려하여 운송 계획을 수립하고 비용 부담 최소화 노력이 필요합니다.''',
    'cach_tp_no' : '004',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_frankfrut)