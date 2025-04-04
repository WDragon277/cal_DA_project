input_order = '''인사하지마. 보고서 작성금지야.
             '개조식은 금지야. 줄글로 작성해줘, 하나의 문단에 내용을 넣어.
             '글자수는 200자 이상 400자로 제한해줘, 가장 최근 데이터는 예측 데이터야,
             '고객에게 제공하는 문체로 작성해줘,
             '줄글로 작성해줘, 하나의 문단에 내용을 넣어.
             '빈칸 넣지마.
             'insight 제공해줘, 요약 및 분석 해줘, 추가분석은 필요 없어,
             '보고서 작성 금지야.
             '줄글로 작성해줘, 결론을 제공해줘
             '네가 제공하는 정보는 수출입을 하는 업체들을 위한 정보야,'''

exp_dubai_info = '부산항에서 두바이로 수출 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
exp_hamburg_info = '부산항에서 함부르크로 수출 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
exp_hochimin_info = '부산항에서 호치민으로 수출 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
exp_la_info = '부산항에서 LA로 수출 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
exp_manzanillo_info = '부산항에서 만사니요로 수출 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
exp_montreal_info = '부산항에서 몬트리올로 수출 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
exp_mumbai_info = '부산항에서 뭄바이로 수출 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
exp_newyork_info = '부산항에서 뉴욕으로 수출 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
exp_rotterdam_info = '부산항에서 로테르담으로 수출 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
exp_saintpetersburg_info = '부산항에서 세인트피츠버그로 수출 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
exp_shanghai_info = '부산항에서 상하이로 수출 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
exp_singapor_info = '부산항에서 싱가포르로 수출 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
exp_tokyo_info = '부산항에서 도쿄로 수출 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
exp_vancouver_info = '부산항에서 벤쿠버로 수출 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
exp_vostochny_info = '부산항에서 보스토치니로 수출 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
exp_xingang_info = '부산항에서 신강으로 수출 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
exp_yokohama_info = '부산항에서 요코하마로 수출 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '



exp_info_list = [exp_dubai_info,exp_hamburg_info,exp_hochimin_info,exp_la_info,exp_manzanillo_info,
                 exp_montreal_info,exp_mumbai_info,exp_newyork_info,exp_rotterdam_info,exp_saintpetersburg_info,
                 exp_shanghai_info,exp_singapor_info,exp_tokyo_info,exp_vancouver_info,
                 exp_vostochny_info,exp_xingang_info,exp_yokohama_info]

imp_dubai_info = '수출이 아니라 수입이야. 두바이에서 부산항으로 수입 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
imp_hamburg_info = '수출이 아니라 수입이야. 함부르크에서 부산항으로 수입 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
imp_hochimin_info = '수출이 아니라 수입이야. 호치민에서 부산항으로 수입 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
imp_la_info = '수출이 아니라 수입이야. LA에서 부산항으로 수입되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
imp_manzanillo_info = '수출이 아니라 수입이야. 만사니요에서 부산항으로 수입 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
imp_montreal_info = '수출이 아니라 수입이야. 몬트리올에서 부산항으로 수입 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
imp_mumbai_info = '수출이 아니라 수입이야. 뭄바이에서 부산항으로 수입 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
imp_newyork_info = '수출이 아니라 수입이야. 뉴욕에서 부산항으로 수입 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
imp_rotterdam_info = '수출이 아니라 수입이야. 로테르담에서 부산항으로 수입 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
imp_saintpetersburg_info = '수출이 아니라 수입이야. 세인트피츠버그에서 부산항으로 수입 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
imp_shanghai_info = '수출이 아니라 수입이야. 상하이에서 부산항으로 수입 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
imp_singapor_info = '수출이 아니라 수입이야. 싱가포르에서 부산항으로 수입 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
imp_tokyo_info = '수출이 아니라 수입이야. 도쿄에서 부산항으로 수입 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
imp_vancouver_info = '수출이 아니라 수입이야. 벤쿠버에서 부산항으로 수입 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
imp_vostochny_info = '수출이 아니라 수입이야. 보스토치니에서 부산항으로 수입 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
imp_xingang_info = '수출이 아니라 수입이야. 신강에서 부산항으로 수입 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '
imp_yokohama_info = '수출이 아니라 수입이야. 요코하마에서 부산으로 수입 되는 화물의 평균 운임이야. 한글로 말해줘. 최근 6개월 데이터는 예측 데이터야. '



imp_info_list = [imp_dubai_info,imp_hamburg_info,imp_hochimin_info,imp_la_info,imp_manzanillo_info,
                 imp_montreal_info,imp_mumbai_info,imp_newyork_info,imp_rotterdam_info,imp_saintpetersburg_info,
                 imp_shanghai_info,imp_singapor_info,imp_tokyo_info,imp_vancouver_info,
                 imp_vostochny_info,imp_xingang_info,imp_yokohama_info]

from models.freight_p.sea_freight.repository import by_sea_route_tables, key_list

sea_freight_tables = by_sea_route_tables


for key in key_list:

    sea_freight_tables[key] = sea_freight_tables[key].set_index('year_mon')
    sea_freight_tables[key] = sea_freight_tables[key].drop(columns= ['data_cd', 'dptr_cnty', 'arvl_cnty'])


# Data for sea freight
sea_frt_no = ['001', '002', '003', '004', '005', '006',
              '007', '008', '009', '010', '011', '012',
              '013', '014', '015', '016', '017']
sea_frt_cty = ['Dubai', 'Hamburg', 'Ho Chi Minh', 'Los Angeles', 'Manzanillo',
               'Montreal', 'Mumbai', 'New York', 'Rotterdam', 'Saint Petersburg',
               'Shanghai', 'Singapore', 'Tokyo', 'Vancouver', 'Vostochny',
               'Xingang', 'Yokohama']
sea_exp_expl = ['Export to Dubai', 'Export to Hamburg', 'Export to Ho Chi Minh',
                'Export to Los Angeles', 'Export to Manzanillo', 'Export to Montreal',
                'Export to Mumbai', 'Export to New York', 'Export to Rotterdam',
                'Export to Saint Petersburg', 'Export to Shanghai', 'Export to Singapore',
                'Export to Tokyo', 'Export to Vancouver', 'Export to Vostochny',
                'Export to Xingang', 'Export to Yokohama']

sea_imp_expl = ['Import from Dubai','Import from Hamburg', 'Import from Ho Chi Minh',
                'Import from Los Angeles', 'Import from Manzanillo', 'Import from Montreal',
                'Import from Mumbai', 'Import from New York', 'Import from Rotterdam',
                'Import from Saint Petersburg', 'Import from Shanghai', 'Import from Singapore',
                'Import from Vancouver', 'Import from Vostochny', 'Import from Xingang',
                'Import from Tokyo', 'Import from Yokohama']
