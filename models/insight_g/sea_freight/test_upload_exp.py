from models.insight_g.model import ChatService
from common.utils.setting import PostgreSQL

upload_service = ChatService()

rdb_info = PostgreSQL()

import datetime
current_time = datetime.datetime.now()
current_day = current_time.strftime('%Y%m%d')


data_dubai = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_EXP',
    'expo_cd': 'Dubai',
    'expo_cd_nm': 'Export to Dubai',
    'ai_anay_desc' : '''부산항에서 두바이로 운송되는 화물의 평균 운임은 최근 몇 개월 동안 변동성을 보였습니다. 2019년부터 2020년까지는  운임이 크게 하락했지만, 2020년 중반부터 다시 상승하는 추세를 보이고 있습니다. 특히 2020년 말에는 큰 폭으로 운임이 증가한 모습을 확인할 수 있으며, 이후에도 높은 수준을 유지하고 있습니다. 그러나 최근 몇 달 동안은 감소추세를 보이고 있어 앞으로 운임이 어떻게 변동할지 관찰해야 할 필요가 있습니다. 전반적으로 볼 때, 부산항에서 두바이로 화물을 운송하는 데는 상대적으로 높은 운임을 지불해야 하며, 시장 상황에 따라 운임의 큰 변동이 있을 수 있음을 유의하여야 합니다. ''',
    'cach_tp_no' : '001',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_dubai)



data_hamburg = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_EXP',
    'expo_cd': 'Hamburg',
    'expo_cd_nm': 'Export to Hamburg',
    'ai_anay_desc' : '''부산항에서 함부르크로 운송되는 화물의 평균 운임은 최근 6개월간 감소하는 추세입니다. 특히 2023년 8월 이후부터는 하락이 더욱 두드러지며, 예측 데이터에 따르면 2024년 1분기에는 약 606달러로 예상됩니다. 이러한 추세는 글로벌 화물 운송 시장의 침체와 연관이 있을 수 있으며, 항공료 및 선박 요금 하락 등 여러 요인이 복합적으로 작용하고 있습니다.  수출입업체들은 이번 하락세를 활용하여 물류 비용 절감에 노력하는 것이 유리할 수 있습니다. 
''',
    'cach_tp_no' : '002',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_hamburg)


data_hochiminh = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_EXP',
    'expo_cd': 'Ho Chi Minh',
    'expo_cd_nm': 'Export to Ho Chi Minh',
    'ai_anay_desc' : '''부산항에서 호치민으로 운송되는 화물의 평균 운임은 지난 몇 달 동안 변동성이 커서 예측이 어려운 상황입니다.  2020년 하반기부터 급등하여 2021년, 2022년에는 최대치를 기록했으며, 2023년 중순부터는 감소 추세에 있습니다. 특히, 최근 6개월간은 예측 데이터로 나타나고 있으며, 앞으로의 운임 변동 경향을 정확하게 예측하기 어렵습니다.  최근 몇 달 동안 평균 운임이 감소하고 있는 추세를 보이며, 이는 세계적인 화물 수요 감소와 유럽 지역의 물류 비용 감소 등 여러 요인에 의해 영향을 받고 있다고 분석됩니다.  
**결론적으로,** 부산항에서 호치민으로 운송되는 화물의 평균 운임은 불확실성이 높으며, 앞으로 어느 방향으로 변동할지 명확하게 예측하기 어렵습니다. 
''',
    'cach_tp_no' : '003',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_hochiminh)

data_la = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_EXP',
    'expo_cd': 'Los Angeles',
    'expo_cd_nm': 'Export to Los Angeles',
    'ai_anay_desc' : '''부산항에서 LA로 운송되는 화물의 평균 운임은 최근 몇 달 동안 하락세를 보이고 있습니다. 특히 2023년 중반부터는  추가적인 하락이 예상됩니다. 2024년 초까지는 600달러대로 유지될 것으로 보입니다. 이러한 추세는 글로벌 물류 시장의 여전히 불안정한 상황과 저출력 경제 환경에 영향을 받고 있습니다. 수입/수출 사업자들은 운임 변동에 대한 예상 범위를 파악하고, 최신 정보를 활용하여 적절한 계획을 세우는 것이 중요합니다. ''',
    'cach_tp_no' : '004',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_la)

data_manzanillo = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_EXP',
    'expo_cd': 'Manzanillo',
    'expo_cd_nm': 'Export to Manzanillo',
    'ai_anay_desc' : '''부산항에서 만사니요로 운송되는 화물의 평균 운임은 최근 몇 개월 동안 하락 추세를 보이고 있습니다. 특히 2019년부터는 꾸준히 운임이 감소하며 저점을 기록한 후 다시 상승하는 경향을 보이고 있습니다.  최근 6개월 동안 평균 운임은 약 784 달러로, 지난 몇 년의 데이터와 비교하면 상대적으로 저렴한 수준입니다.  이러한 추세는 세계적인 물류 시장 변화와 부산항에서의 경쟁 심화 등 다양한 요인에 영향을 받고 있습니다.  앞으로도 운임은 변동성을 보일 것으로 예상되므로, 수출입 업체들은 유연하고 적응력 있는 전략을 세우는 것이 중요합니다. 
''',
    'cach_tp_no' : '005',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_manzanillo)

data_montreal = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_EXP',
    'expo_cd': 'Montreal',
    'expo_cd_nm': 'Export to Montreal',
    'ai_anay_desc' : '''부산항에서 몬트리올로 운송되는 화물의 평균 운임은 최근 몇 년 동안 변동성을 보이며 지속적인 하락세를 보입니다. 특히 2019년부터는 운임이 크게 감소하여 2023년 현재 약 670달러 수준에 머물고 있습니다. 이러한 추세는 글로벌 경제 상황과 선박 운송 시장의 변화에 영향을 받아 나타나고 있으며, 앞으로도 유지될 가능성이 높습니다. 수출입 업체들은 이러한 변동성을 감안하여 운송 계획 및 예산을 조정하는 것이 중요하며, 장기적인 관점에서 운임 동향을 지속적으로 주시해야 합니다. ''',
    'cach_tp_no' : '006',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_montreal)

data_mumbai = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_EXP',
    'expo_cd': 'Mumbai',
    'expo_cd_nm': 'Export to Mumbai',
    'ai_anay_desc' : '''부산항에서 뭄바이로 운송되는 화물의 평균 운임은 최근 몇 달 동안 감소하는 추세를 보이고 있습니다. 특히 2022년 하반기부터는 운임이 상당히 낮아졌고, 2023년에는  400달러대에서 시작하며 계속해서 하락하여 2024년 예상치 500 달러 대까지 도달할 것으로 예상됩니다. 이러한 추세는 글로벌 운송 시장의 변화와 경제 상황에 영향을 받고 있습니다.  수출입 업체들은 이러한 변동성을 감안하여 운임 관리 및 계획에 신경 써야 합니다.''',
    'cach_tp_no' : '007',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_mumbai)

data_ny = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_EXP',
    'expo_cd': 'New York',
    'expo_cd_nm': 'Export to New York',
    'ai_anay_desc' : '''부산항에서 뉴욕으로 운송되는 화물의 평균 운임은 최근 몇 년간 변동성을 보였습니다. 2018년까지는 상승세를 보였으나, 이후 급격한 하락 추세로 전환되었습니다. 2023년 현재 운임은 약 685달러에 머물며, 예측치를 통해 앞으로도 일정 수준에서 유지될 것으로 보입니다. 운임 변동성이 주요 원인이며,  글로벌 경제 상황 및 부산항과 뉴욕 항의 운송 규모 등 다양한 요인이 평균 운임에 영향을 미치고 있습니다. 수출입 업체들은 이러한 추세를 고려하여 물류 계획을 수립하는 것이 중요하며, 운임 변동 상황을 지속적으로 모니터링해야 합니다. ''',
    'cach_tp_no' : '008',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_ny)

data_rotterdam = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_EXP',
    'expo_cd': 'Rotterdam',
    'expo_cd_nm': 'Export to Rotterdam',
    'ai_anay_desc' : '''부산항에서 로테르담으로 운송되는 화물의 평균 운임은 최근 몇 달 동안 감소 추세를 보이고 있습니다. 특히 2023년 이후부터는 꾸준히 하락하여 현재 약 654달러 수준에 머무르고 있습니다.  이러한 추세는 세계적인 운송비하락과 글로벌 경제 불확실성으로 인한 것으로 분석되며, 앞으로도 비교적 안정적으로 운임 변동이 이루어질 것으로 예상됩니다. 수출입 업체들은 이를 고려하여 화물 운송 계획을 세우는 것이 좋습니다. 
''',
    'cach_tp_no' : '009',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_rotterdam)

data_saintpetersburg = {
    'crtn_dt': current_day,
    'cach_tp_cd': 'SEA_EXP',
    'expo_cd': 'Saint Petersburg',
    'expo_cd_nm': 'Export to Saint Petersburg',
    'ai_anay_desc': '부산항에서 세인트피츠버그로 운송되는 화물의 평균 운임은 최근 몇 년간 변동성이 큰 추세를 보여줍니다. 2019년부터 2023년까지는 통계적으로 운임이 감소하는 추세였으며, 특히 2023년 말부터는  현재 기준으로 약 554 달러대로 하락했습니다.  하지만 최근 예측 데이터를 보면 2024년에는  600달러대로 다시 상승할 것으로 예상됩니다. 이러한 변동은 세계적인 경제 상황, 유류 가격 변동, 해운 산업의 전반적인 불확실성 등 다양한 요인에 영향을 받고 있습니다. 수출입 업체들은 앞으로 발생할 수 있는 운임 변동에 대비하여 계획을 세우는 것이 중요합니다.',
    'cach_tp_no': '010',
    'frst_rgsr_id': 'admin',
    'frst_rgsr_dttm': datetime.datetime.now()
}

upload_service.save_data_postdb(data_saintpetersburg)

data_shanghai = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_EXP',
    'expo_cd': 'Shanghai',
    'expo_cd_nm': 'Export to Shanghai',
    'ai_anay_desc' : '''부산항에서 상하이로 운송되는 화물의 평균 운임은 최근 몇 달 동안 큰 변동을 보이고 있습니다. 특히 2020년 하반기부터는 수출입 심각한 영향으로 인해 운임이 크게 상승했고, 2021년에는 지속적으로 높은 수준을 유지했습니다. 그러나 2022년 이후로는 감소세를 보이고 있으며 2023년 현재 약 200달러대에서 안정되고 있습니다. 최근 예측 데이터에 따르면, 2024년 상반기에 다시 운임이 상승할 가능성이 높습니다. 수출입 업체들은 앞으로 발생할 수 있는 운임 변동을 고려하여 적절한 사업 계획을 수립해야 합니다.  
''',
    'cach_tp_no' : '011',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_shanghai)

data_singapore = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_EXP',
    'expo_cd': 'Singapore',
    'expo_cd_nm': 'Export to Singapore',
    'ai_anay_desc' : '''부산항에서 싱가포르로 운송되는 화물의 평균 운임은 최근 몇 개월 동안 감소하는 추세를 보이고 있습니다. 특히 2020년 중반부터는 크게 하락했으며,  2023년 이후 다시 상승세를 보이기 시작했습니다. 하지만 여전히 과거 평균 수준보다 낮은 편이며, 예측 데이터에 따르면 향후에도 운임 변동폭은 적고 안정적인 추세를 유지할 것으로 보입니다.''',
    'cach_tp_no' : '012',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_singapore)

data_tokyo = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_EXP',
    'expo_cd': 'Tokyo',
    'expo_cd_nm': 'Export to Tokyo',
    'ai_anay_desc' : '''부산항에서 도쿄로 운송되는 화물의 평균 운임은 최근 몇 달 동안 감소하는 추세를 보이고 있습니다. 2023년 1월부터는 꾸준히 운임이 하락하며, 특히 8월에는 291.9달러까지 감소했습니다.  하지만 9월부터 다시 상승세로 돌아가고 있으며, 현재 예상치는 280.5달러입니다. 이러한 변동은 해운 물류 시장의 불안정성과 글로벌 경제 상황에 영향을 받고 있습니다.  
수출입업체들은 앞으로 발생할 수 있는 운임 변동에 대비하여 신중한 계획 수립이 필요하며, 실시간 운임 정보를 주시하는 것이 중요합니다. ''',
    'cach_tp_no' : '013',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_tokyo)

data_vancouver = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_EXP',
    'expo_cd': 'Vancouver',
    'expo_cd_nm': 'Export to Vancouver',
    'ai_anay_desc' : '''부산항에서 벤쿠버로 운송되는 화물의 평균 운임을 분석해보니, 최근 몇 달 동안 지속적인 하락세를 보이고 있습니다. 특히 2023년 4월부터는 급격한 하락 추세를 보이며, 현재 평균 운임은 약 768.9 USD로 나타났습니다. 이러한 하락은 글로벌 물류 시장의 변화와 세계적인 경기침체 영향으로 분석되며, 향후에도 유지될 가능성이 있습니다.  벤쿠버로 가는 화물 운송 비용을 고려할 때 현재 상황에서 기회를 활용하여 운송비 절감 효과를 얻을 수 있을 것으로 예상됩니다. ''',
    'cach_tp_no' : '014',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_vancouver)

data_vostochny = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_EXP',
    'expo_cd': 'Vostochny',
    'expo_cd_nm': 'Export to Vostochny',
    'ai_anay_desc' : '''부산항에서 보스토치니로 운송되는 화물의 평균 운임 데이터를 분석해보았습니다. 2016년부터 2023년까지는 운임이 다양한 변동성을 보였으며, 특히 2020년부터 급등하는 추세를 보였습니다. 그러나 2023년 이후로는  점차 감소하는 경향을 보이며 현재 약 640달러 수준입니다. 최근 6개월 데이터를 예측해봤을 때, 운임은 계속해서 하락 추세를 유지할 것으로 예상됩니다. 따라서 부산항에서 보스토치니로 화물을 운송하는 업체들은 앞으로의 운임 변동에 주시하고 효율적인 운영 전략을 수립하는 것이 중요합니다.''',
    'cach_tp_no' : '015',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_vostochny)

data_xingang = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_EXP',
    'expo_cd': 'Xingang',
    'expo_cd_nm': 'Export to Xingang',
    'ai_anay_desc' : '''부산항에서 신강으로 운송되는 화물의 평균 운임은 최근 몇 개월 동안 하락세를 보이고 있습니다.  특히, 2023년 상반기에는 급격한 하락 추세를 보였으며, 2024년 6월까지는 약 279.8달러로 예상됩니다. 이러한 추세는 세계적인 물류 비용 인하와 부산항에서의 화물 수요 감소가 원인으로 분석되며,  수출입 업체들은 운임 변동에 유의하여 사업 계획을 수립하는 것이 중요합니다.''',
    'cach_tp_no' : '016',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_xingang)

data_yocohama = {
    'crtn_dt' : current_day,
    'cach_tp_cd' : 'SEA_EXP',
    'expo_cd': 'Yokohama',
    'expo_cd_nm': 'Export to Yokohama',
    'ai_anay_desc' : '''부산항에서 요코하마로 운송되는 화물의 평균 운임은 지난 몇 년 동안 변동성이 있는 추세를 보입니다.  특히 2021년부터는 전반적으로 상승세를 보였으며, 2023년 들어서는 감소하는 추세에 돌아섰습니다. 최근 6개월 데이터를 분석하면 2024년 1월~6월까지 운임은 변동이 적고  평균적인 수준을 유지할 것으로 예상됩니다. 
**결론적으로, 현재 부산항에서 요코하마로 운송되는 화물의 평균 운임은 상대적으로 안정적인 상태이며, 향후에도 큰 변화가 없을 것으로 전망됩니다.**''',
    'cach_tp_no' : '017',
    'frst_rgsr_id' : 'admin',
    'frst_rgsr_dttm' : datetime.datetime.now()
}

upload_service.save_data_postdb(data_yocohama)