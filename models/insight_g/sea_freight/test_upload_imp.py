from models.insight_g.model import ChatService
from common.utils.setting import PostgreSQL

upload_service = ChatService()

rdb_info = PostgreSQL()

import datetime
current_time = datetime.datetime.now()
current_day = current_time.strftime('%Y%m%d')


data_dubai = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_IMP',
    'expo_cd': 'Dubai',
    'expo_cd_nm': 'Import from Dubai',
    'ai_anay_desc' : '''두바이에서 부산항으로 운송되는 화물의 평균 운임은 최근 몇 년 동안 큰 변동을 보이고 있습니다. 특히 2020년부터는 코로나 팬데믹 이후 글로벌 수요 증가와 공급망 문제로 인해 운임이 폭등하는 추세를 보였습니다.
2021년부터 2023년까지는 해운 운임이 감소했지만, 2024년 1월부터 다시 상승하기 시작했습니다. 예측 데이터에 따르면 두바이에서 부산항으로의 화물 운송 평균 운임은  지속적으로 상승할 것으로 보입니다. 이는 세계 경제 회복과 수요 증가로 인한 요인이 작용한다고 분석됩니다. 따라서 수출입 업체들은 향후 운임 변동에 유의해야 하며, 장기적인 관점에서 운송 비용 관리 전략을 수립하는 것이 중요합니다. 
''',
    'cach_tp_no' : '001',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_dubai)



data_hamburg = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_IMP',
    'expo_cd': 'Hamburg',
    'expo_cd_nm': 'Import from Hamburg',
    'ai_anay_desc' : '''함부르크에서 부산항으로 화물을 운송하는 평균 운임은 최근 몇 년 동안 변동성이 큰 경향을 보이고 있습니다. 2020년 후반부터 급격히 상승했으며, 특히 2021년에는 7,000달러 이상까지 치솟았습니다. 그러나 2022년 하반기부터 감소세로 돌아섰고, 현재는 과거의 수준으로 회복되지 못하고 있습니다. 최근 예측 데이터에 따르면 2024년에도 운임이 상대적으로 높은 수준을 유지할 것으로 보입니다.  
''',
    'cach_tp_no' : '002',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_hamburg)


data_hochiminh = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_IMP',
    'expo_cd': 'Ho Chi Minh',
    'expo_cd_nm': 'Import from Ho Chi Minh',
    'ai_anay_desc' : '''호치민에서 부산항으로 운송되는 화물의 평균 운임은 최근 6개월 동안 변동 추세를 보이고 있습니다. 2023년 10월부터는 감소 추세를 보였으며, 2024년 6월까지 예상치는  상승하는 모습을 보입니다. 특히 2024년 6월에는 약 444.5 달러로 가장 높은 운임이 예상됩니다. 이러한 변동은 글로벌 경제 상황, 에너지 가격 변동, 선박 수요 등 다양한 요인에 영향을 받는 것으로 분석됩니다.  수출입 업체들은 향후 운임 변화 추세를 주시하고 운송 계획에 반영해야 합니다.
''',
    'cach_tp_no' : '003',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_hochiminh)

data_la = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_IMP',
    'expo_cd': 'Los Angeles',
    'expo_cd_nm': 'Import from Los Angeles',
    'ai_anay_desc' : '''LA에서 부산항으로 운송되는 화물의 평균 운임은 최근 몇 개월간 크게 변동을 보였습니다. 특히 2021년 하반기부터 급격히 상승하여 2022년에는 일정 수준을 유지하다가 2023년 초부터 다시 감소하는 추세를 보입니다. 최근 6개월 데이터는 예측치이며, 앞으로 운임이 어떤 방향으로 변동할지는 불확실합니다.  그러나 전체적인 트렌드를 살펴볼 때, LA에서 부산항으로 화물을 운송하는 것은 비용이 상대적으로 높은 편입니다. 수출입 업체들은 이러한 추세를 고려하여 물류 계획 및 가격 전략을 수립하는 것이 중요합니다. 
''',
    'cach_tp_no' : '004',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_la)

data_manzanillo = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_IMP',
    'expo_cd': 'Manzanillo',
    'expo_cd_nm': 'Import from Manzanillo',
    'ai_anay_desc' : '''만사니요에서 부산항으로 운송되는 화물의 평균 운임을 분석한 결과, 2016년 이후 지속적인 상승 추세를 보였습니다. 특히 2020년부터는 코로나 팬데믹 영향과 글로벌 수급 불균형으로 인해 운임이 크게 증가했습니다. 최근 몇 개월 동안은 전반적으로 하락세를 보이고 있으며,  2024년 1~6월까지는 예상치 7510.6 달러까지 상승할 수 있습니다. 이러한 추세를 고려하여 수출입 업체들은 운임 변동에 대비하여 물류 계획을 신중하게 세우는 것이 중요합니다. 
''',
    'cach_tp_no' : '005',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_manzanillo)

data_montreal = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_IMP',
    'expo_cd': 'Montreal',
    'expo_cd_nm': 'Import from Montreal',
    'ai_anay_desc' : '''몬트리올에서 부산항으로 운송되는 화물의 평균 운임은 최근 몇 달 동안 상당한 변동을 보였습니다. 2020년 하반기부터 큰 폭으로 상승세를 보이며, 특히 2021년과 2022년에는 전년 대비 두 배가 넘는 수준에 이르렀습니다. 그러나 2023년 중순부터 다시 감소 추세로 돌아서 현재까지는 꾸준하게 하락하고 있습니다. 예측 데이터를 바탕으로 볼 때, 앞으로도 운임은 안정적인 추이를 이루어 갈 것으로 보입니다.  
''',
    'cach_tp_no' : '006',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_montreal)

data_mumbai = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_IMP',
    'expo_cd': 'Mumbai',
    'expo_cd_nm': 'Import from Mumbai',
    'ai_anay_desc' : '''뭄바이에서 부산항으로 운송되는 화물의 평균 운임은 최근 몇 년 동안 큰 변동을 보이고 있습니다. 2016년부터 2019년까지는 대체로 안정적인 추세를 보였으며, 특히 2019년 하반기부터 감소하는 경향을 보였습니다. 그러나 2020년에는 코로나 팬데믹의 영향으로 운임이 크게 상승했고, 이후에도 불확실성이 지속되어 급격한 변동을 거듭하고 있습니다. 최근 데이터를 분석해보면 2023년 하반기부터 운임이 감소하는 추세로 나타나고 있으며, 2024년 예측치 또한 이러한 경향을 따르고 있으므로 안정적인 수준으로 회복될 가능성이 있습니다.  
''',
    'cach_tp_no' : '007',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_mumbai)

data_ny = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_IMP',
    'expo_cd': 'New York',
    'expo_cd_nm': 'Import from New York',
    'ai_anay_desc' : '''뉴욕에서 부산항으로 운송되는 화물의 평균 운임은 최근 몇 년 동안 변동성이 큰 추세를 보이고 있습니다. 특히 2021년부터는 전반적으로 상승세로,  2021년 4월 이후에는 지속적으로 증가하여  2021년 12월에 최고치인 약 17,000달러를 기록했습니다. 하지만 2022년부터는 감소세로 돌아섰으며, 현재는 상대적으로 안정적인 수준을 유지하고 있습니다.  최근 예측 데이터를 보면, 2024년까지는 운임이 다시 상승할 가능성이 높다고 분석됩니다. 신중한 판단 기반으로 화물 운송 계획을 수립하는 것이 중요합니다.
''',
    'cach_tp_no' : '008',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_ny)

data_rotterdam = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_IMP',
    'expo_cd': 'Rotterdam',
    'expo_cd_nm': 'Import from Rotterdam',
    'ai_anay_desc' : '''로테르담에서 부산항으로 운송되는 화물의 평균 운임은 시간에 따라 변동하는 추세를 보입니다. 2023년 초까지는  급격한 감소를 보였지만, 2024년 예측치는 다시 상승세로 나타나고 있습니다. 특히 2024년 5월부터 운임이 급증할 것으로 예상되며, 이는 해외 시장의 변동과 화물 수요 증가에 영향을 받은 결과일 가능성이 높습니다. 최근 6개월간의 데이터를 분석해보면 하락세에서 벗어나 상승 추세로 돌아오고 있는 것을 확인할 수 있습니다.  이는 앞으로 로테르담-부산항 노선 운임에 대한 주의가 필요하며, 수출입 업체들은 미래 운임 변동성을 감안하여 재무 계획을 수립하는 것이 중요합니다.''',
    'cach_tp_no' : '009',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_rotterdam)

data_saintpetersburg = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_IMP',
    'expo_cd': 'Saint Petersburg',
    'expo_cd_nm': 'Import from Saint Petersburg',
    'ai_anay_desc' : '세인트피츠버그에서 부산항으로 운송되는 화물의 평균 운임은 최근 몇 개월간 큰 변동을 보이고 있습니다. 2020년부터는 특히 상승세를 보였으며, 2021년에는 5,000달러대를 돌파했고 2022년에는 다시 감소 추세를 보이며 현재 3,000달러대에서 유동되고 있습니다. 최근 예측 데이터는 2024년 상반기까지 일정 수준으로 유지될 것으로 나타났습니다. 이러한 변동은 글로벌 원자재 가격 및 해운 운임의 영향을 받아 발생하는 것으로 분석됩니다.  앞으로도 화물 운송 시 예상치 못한 변동이 있을 가능성이 있으므로, 신중하게 운송 계획을 수립하고 관련 정보를 지속적으로 확인해야 합니다.',
    'cach_tp_no' : '010',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_saintpetersburg)

data_shanghai = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_IMP',
    'expo_cd': 'Shanghai',
    'expo_cd_nm': 'Import from Shanghai',
    'ai_anay_desc' : '''상하이에서 부산항으로 운송되는 화물의 평균 운임 데이터를 분석한 결과, 최근 6개월 동안 변동성이 매우 크게 나타났습니다. 2023년 초부터는 하락 추세를 보였으며, 특히 2023년 5월에는 29.4달러로 가장 저렴한 운임을 기록했습니다. 그러나 2024년 1~2월에는 다시 상승세로 돌아섰습니다. 이러한 변동성은 글로벌 원자재 가격과 세계적인 수송 시장의 불안정성에 영향을 받는 것으로 분석됩니다.  앞으로도 운임 변동이 지속될 가능성이 높으므로, 수출입 업체들은 항상 최신 정보를 확인하고 운임 예측 모델을 활용하여 신중한 운송 계획을 세우는 것이 중요합니다. 
''',
    'cach_tp_no' : '011',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_shanghai)

data_singapore = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_IMP',
    'expo_cd': 'Singapore',
    'expo_cd_nm': 'Import from Singapore',
    'ai_anay_desc' : '''싱가포르에서 부산항으로 운송되는 화물의 평균 운임은 최근 몇 달간 감소 추세를 보이고 있습니다. 특히 지난 6개월 동안에는 꾸준한 하락세를 보이며, 2023년 12월 기준 258.9달러로 나타났습니다. 이는 전반적인 세계 시장의 경기침체와 물류비용 감소에 따른 영향으로 분석됩니다. 앞으로도 글로벌 경제 상황과 유관 정책 변화에 따라 운임 변동이 예상되므로 주시가 필요합니다. 수출입 업계는 운임 변동을 파악하고 사전 계획 및 대비를 통해 비용 효율성을 높이는 데 노력해야 합니다. ''',
    'cach_tp_no' : '012',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_singapore)

data_tokyo = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_IMP',
    'expo_cd': 'Tokyo',
    'expo_cd_nm': 'Import from Tokyo',
    'ai_anay_desc' : '''도쿄에서 부산항으로 운송되는 화물의 평균 운임은 최근 몇 개월 동안 변동성이 큰 추세를 보이고 있습니다. 특히, 2021년 하반기부터 2022년 상반기에 이르기까지 급격하게 상승했으며, 그 이후에는 감소하는 경향을 보입니다.  최근 6개월 데이터는 예측치로 나타나며, 앞으로의 운임 변동은 다양한 요인에 따라 달라질 수 있습니다. 도쿄에서 부산항으로 화물을 운송할 때는 최신 정보를 확인하여 운임 상황을 파악하는 것이 중요합니다.''',
    'cach_tp_no' : '013',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_tokyo)

data_vancouver = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_IMP',
    'expo_cd': 'Vancouver',
    'expo_cd_nm': 'Import from Vancouver',
    'ai_anay_desc' : '''벤쿠버에서 부산항으로 운송되는 화물의 평균 운임은 지난 몇 년간 큰 변동을 보이고 있습니다.  2020년 이후, 전 세계적으로 물류 산업이 불확실성에 직면하면서 운임이 크게 상승했습니다. 특히 코로나19 팬데믹의 영향으로 수요와 공급 간의 균형이 무너지고, 에너지 가격 상승과 해상 운송 시스템의 지연 등 다양한 요인들이 운임을 증폭시켰습니다.  2023년 현재는 전반적인 경기 하락과 물류 산업 내 회복 가능성으로 인해 운임이 감소하는 추세입니다. 최근 예측 데이터를 분석하면 벤쿠버에서 부산항으로 운송되는 화물의 평균 운임은 앞으로 꾸준히 감소할 것으로 보이며, 수출입 업체들은 이러한 변화를 주시하고 전략을 조정하는 것이 중요합니다.''',
    'cach_tp_no' : '014',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_vancouver)

data_vostochny = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_IMP',
    'expo_cd': 'Vostochny',
    'expo_cd_nm': 'Import from Vostochny',
    'ai_anay_desc' : '''보스턴에서 부산항으로 운송되는 화물의 평균 운임을 보면 2016년부터 급격히 상승하는 추세를 보였습니다. 특히, 2020년 이후에는 코로나 팬데믹과 세계 경제 불안정 등의 요인으로 인해 심각한 운임 상승이 나타났으며, 2021년부터는 전반적으로 매우 높은 수준을 유지하고 있습니다. 최근 6개월 데이터(예측치)를 보면 지난 해에 비해 경차하는 추세가 보이고 있으며 2024년에는 추가적인 하락이 예상됩니다.  결론적으로, 현재 부산항으로 운송되는 화물의 평균 운임은 여전히 높지만, 앞으로는 안정화 및 감소추세로 변화할 것으로 예상됩니다. 
''',
    'cach_tp_no' : '015',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_vostochny)

data_xingang = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_IMP',
    'expo_cd': 'Xingang',
    'expo_cd_nm': 'Import from Xingang',
    'ai_anay_desc' : '''신강에서 부산항으로 운송되는 화물의 평균 운임은 꾸준히 변동을 보이고 있습니다. 최근 몇 달 동안  운임이 감소하는 추세를 보였습니다. 이러한 변화는 글로벌 경제 상황, 원유 가격 변동, 그리고 운송 수요의 영향을 받고 있습니다. 예측 모델에 따르면 앞으로도 운임은 안정적인 추세를 유지할 것으로 보입니다.  이러한 정보를 바탕으로 신강에서 부산항까지 화물 운송 계획을 세우는 데 도움을 얻으실 수 있습니다. ''',
    'cach_tp_no' : '016',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_xingang)

data_yocohama = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_IMP',
    'expo_cd': 'Yokohama',
    'expo_cd_nm': 'Import from Yokohama',
    'ai_anay_desc' : '''요코하마에서 부산으로 운송되는 화물의 평균 운임은 꾸준히 변동을 보이고 있습니다. 2016년부터는 상승세를 보였으며, 특히 2020년 이후에는 큰 폭으로 상승했습니다. 그러나 최근 몇 달 동안은 다시 감소 추세에 있으며 예측 데이터로 볼 때 향후 에도 하락이 지속될 것으로 보입니다. 수출입 업체들은 이러한 운임 변동을 주시하며 운송 계획에 반영하는 것이 중요합니다.''',
    'cach_tp_no' : '017',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_yocohama)