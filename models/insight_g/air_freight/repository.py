from models.freight_p.air_freight.imp.imp_repository import la_imp_data \
    ,newyork_imp_data ,chicago_imp_data ,sfo_imp_data \
    ,atl_imp_data, pudong_imp_data, tianjin_imp_data, qingdao_imp_data, \
    hangzhou_imp_data, beijing_imp_data, guangzhou_imp_data, kuala_imp_data, \
    singapor_imp_data, hongkong_imp_data, hochimin_imp_data, penang_imp_data, \
    kansai_imp_data, narita_imp_data, nagoya_imp_data, frankfrut_imp_data

from models.freight_p.air_freight.exp.exp_repository import la_exp_data \
    ,newyork_exp_data,chicago_exp_data,sfo_exp_data \
    ,atl_exp_data, pudong_exp_data, tianjin_exp_data, qingdao_exp_data, \
    hangzhou_exp_data, beijing_exp_data, guangzhou_exp_data, kuala_exp_data, \
    singapor_exp_data, hongkong_exp_data, hochimin_exp_data, penang_exp_data, \
    kansai_exp_data, narita_exp_data, nagoya_exp_data, frankfrut_exp_data

# 수출항로별 데이터 리스트 저장
exp_df_list = [la_exp_data,newyork_exp_data ,chicago_exp_data ,sfo_exp_data
    ,atl_exp_data, pudong_exp_data, tianjin_exp_data, qingdao_exp_data,
    hangzhou_exp_data, beijing_exp_data, guangzhou_exp_data, kuala_exp_data,
    singapor_exp_data, hongkong_exp_data, hochimin_exp_data, penang_exp_data,
    kansai_exp_data, narita_exp_data, nagoya_exp_data, frankfrut_exp_data]

exp_la_info = '인천공항에서 LA으로 날아가는 화물의 평균 운임이야. 한글로 말해줘'
exp_newyork_info = '인천공항에서 뉴욕으로 날아가는 화물의 평균 운임이야. 한글로 말해줘'
exp_chicago_info = '인천공항에서 시카고로 날아가는 화물의 평균 운임이야. 한글로 말해줘'
exp_sfo_info = '인천공항에서 샌프란시스코로 날아가는 화물의 평균 운임이야. 한글로 말해줘'
exp_atl_info = '인천공항에서 애틀랜타로 날아가는 화물의 평균 운임이야. 한글로 말해줘'
exp_pudong_info = '인천공항에서 푸둥으로 날아가는 화물의 평균 운임이야. 한글로 말해줘'
exp_tianjin_info = '인천공항에서 톈진으로 날아가는 화물의 평균 운임이야. 한글로 말해줘'
exp_qingdao_info = '인천공항에서 칭다오로 날아가는 화물의 평균 운임이야. 한글로 말해줘'
exp_hangzhou_info = '인천공항에서 항저우로 날아가는 화물의 평균 운임이야. 한글로 말해줘'
exp_beijing_info = '인천공항에서 베이징으로 날아가는 화물의 평균 운임이야. 한글로 말해줘'
exp_guangzhou_info = '인천공항에서 광저우로 날아가는 화물의 평균 운임이야. 한글로 말해줘'
exp_kuala_info = '인천공항에서 쿠알라 룸푸르로 날아가는 화물의 평균 운임이야. 한글로 말해줘'
exp_singapor_info = '인천공항에서 싱가포르로 날아가는 화물의 평균 운임이야. 한글로 말해줘'
exp_hongkong_info = '인천공항에서 홍콩으로 날아가는 화물의 평균 운임이야. 한글로 말해줘'
exp_hochimin_info = '인천공항에서 호치민으로 날아가는 화물의 평균 운임이야. 한글로 말해줘'
exp_penang_info = '인천공항에서 페낭으로 날아가는 화물의 평균 운임이야. 한글로 말해줘'
exp_kansai_info = '인천공항에서 칸사이로 날아가는 화물의 평균 운임이야. 한글로 말해줘'
exp_narita_info = '인천공항에서 나리타로 날아가는 화물의 평균 운임이야. 한글로 말해줘'
exp_nagoya_info = '인천공항에서 나고야로 날아가는 화물의 평균 운임이야. 한글로 말해줘'
exp_frankfrut_info = '인천공항에서 프랑크프루트로 날아가는 화물의 평균 운임이야. 한글로 말해줘'

exp_info_list = [exp_la_info,exp_newyork_info,exp_chicago_info,exp_sfo_info,exp_atl_info,
                 exp_pudong_info,exp_tianjin_info,exp_qingdao_info,exp_hangzhou_info,exp_beijing_info,
                 exp_guangzhou_info,exp_kuala_info,exp_singapor_info,exp_hongkong_info,exp_hochimin_info,
                 exp_penang_info,exp_kansai_info,exp_narita_info,exp_nagoya_info,exp_frankfrut_info]




# 수입 항로별 데이터 리스트 저장
imp_df_list = [la_imp_data,newyork_imp_data ,chicago_imp_data ,sfo_imp_data
    ,atl_imp_data, pudong_imp_data, tianjin_imp_data, qingdao_imp_data,
    hangzhou_imp_data, beijing_imp_data, guangzhou_imp_data, kuala_imp_data,
    singapor_imp_data, hongkong_imp_data, hochimin_imp_data, penang_imp_data,
    kansai_imp_data, narita_imp_data, nagoya_imp_data, frankfrut_imp_data ]

imp_la_info = 'LA에서 인천공항으로 날아오는 화물의 평균 운임이야. 한글로 말해줘'
imp_newyork_info = '뉴욕에서 인천공항으로 날아오는 화물의 평균 운임이야. 한글로 말해줘'
imp_chicago_info = '시카고에서 인천공항으로 날아오는 화물의 평균 운임이야. 한글로 말해줘'
imp_sfo_info = '샌프란시스코에서 인천공항으로 날아오는 화물의 평균 운임이야. 한글로 말해줘'
imp_atl_info = '애틀랜타에서 인천공항으로 날아오는 화물의 평균 운임이야. 한글로 말해줘'
imp_pudong_info = '푸둥에서 인천공항으로 날아오는 화물의 평균 운임이야. 한글로 말해줘'
imp_tianjin_info = '톈진에서 인천공항으로 날아오는 화물의 평균 운임이야. 한글로 말해줘'
imp_qingdao_info = '칭다오에서 인천공항으로 날아오는 화물의 평균 운임이야. 한글로 말해줘'
imp_hangzhou_info = '항저우에서 인천공항으로 날아오는 화물의 평균 운임이야. 한글로 말해줘'
imp_beijing_info = '베이징에서 인천공항으로 날아오는 화물의 평균 운임이야. 한글로 말해줘'
imp_guangzhou_info = '광저우에서 인천공항으로 날아오는 화물의 평균 운임이야. 한글로 말해줘'
imp_kuala_info = '쿠알라 룸푸르에서 인천공항으로 날아오는 화물의 평균 운임이야. 한글로 말해줘'
imp_singapor_info = '싱가포르에서 인천공항으로 날아오는 화물의 평균 운임이야. 한글로 말해줘'
imp_hongkong_info = '홍콩에서 인천공항으로 날아오는 화물의 평균 운임이야. 한글로 말해줘'
imp_hochimin_info = '호치민에서 인천공항으로 날아오는 화물의 평균 운임이야. 한글로 말해줘'
imp_penang_info = '페낭에서 인천공항으로 날아오는 화물의 평균 운임이야. 한글로 말해줘'
imp_kansai_info = '간사이에서 인천공항으로 날아오는 화물의 평균 운임이야. 한글로 말해줘'
imp_narita_info = '나리타에서 인천공항으로 날아오는 화물의 평균 운임이야. 한글로 말해줘'
imp_nagoya_info = '나고야에서 인천공항으로 날아오는 화물의 평균 운임이야. 한글로 말해줘'
imp_frankfrut_info = '프랑크프루트에서 인천공항으로 날아오는 화물의 평균 운임이야. 한글로 말해줘'

imp_info_list = [imp_la_info,imp_newyork_info,imp_chicago_info,imp_sfo_info,imp_atl_info,
                 imp_pudong_info,imp_tianjin_info,imp_qingdao_info,imp_hangzhou_info,imp_beijing_info,
                 imp_guangzhou_info,imp_kuala_info,imp_singapor_info,imp_hongkong_info,imp_hochimin_info,
                 imp_penang_info,imp_kansai_info,imp_narita_info,imp_nagoya_info,imp_frankfrut_info]

input_order = '''인사하지마. 보고서 작성금지야.
개조식은 금지야. 줄글로 작성해줘, 하나의 문단에 내용을 넣어.
글자수는 200자 이상 400자로 제한해줘, 가장 최근 데이터는 예측 데이터야,
고객에게 제공하는 문체로 작성해줘,
줄글로 작성해줘, 하나의 문단에 내용을 넣어.
빈칸 넣지마.
insight 제공해줘, 요약 및 분석 해줘, 추가분석은 필요 없어,
보고서 작성 금지야.
줄글로 작성해줘, 결론을 제공해줘
네가 제공하는 정보는 수출입을 하는 업체들을 위한 정보야,'''


# Data for air freight
air_frt_no = ['001', '002', '003', '004', '005', '006',
              '007', '008', '009', '010', '011', '012',
              '013', '014', '015', '016', '017', '018', '019', '020']
air_frt_cty = ['Atlanta', 'Beijing','Chicago', 'Frankfurt', 'Guangzhou',
               'Hangzhou', 'Hochimin', 'Hongkong', 'Kansai', 'Kuala',
               'Los Angeles', 'Nagoya', 'Narita', 'New York', 'Penang',
               'Pudong', 'Qingdao', 'San Francisco', 'Singapore', 'Tianjin']
air_exp_expl = ['Export from Atlanta', 'Export from Beijing', 'Export from Chicago',
                'Export from Frankfurt', 'Export from Guangzhou', 'Export from Hangzhou',
                'Export from Hochimin', 'Export from Hongkong', 'Export from Kansai',
                'Export from Kuala Lumpur', 'Export from Los Angeles', 'Export from Nagoya',
                'Export from Narita', 'Export from New York', 'Export from Penang',
                'Export from Pudong', 'Export from Qingdao', 'Export from San Francisco',
                'Export from Singapore', 'Export from Tianjin']
air_imp_expl = ['Import from Atlanta', 'Import from Beijing', 'Import from Chicago',
                'Import from Frankfurt', 'Import from Guangzhou', 'Import from Hangzhou',
                'Import from Hochimin', 'Import from Hongkong', 'Import from Kansai',
                'Import from Kuala Lumpur', 'Import from Los Angeles', 'Import from Nagoya',
                'Import from Narita', 'Import from New York', 'Import from Penang',
                'Import from Pudong', 'Import from Qingdao', 'Import from San Francisco',
                'Import from Singapore', 'Import from Tianjin']