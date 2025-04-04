from models.freight_p.sea_freight_index.ccfi_p.model import define_ccfi_data

import ollama


def ccfi_insight():

    tmp = define_ccfi_data()
    input_json_ccfi = str(tmp)

    input_indx_info = 'ccfi_cach_expo는 CCFI를 나타내는 칼럼명이야,넌 CCFI를 분석하는거야,' \
                 'CCFI는 China Containerized Freight Index의 약자야'

    input_text = '인사하지마. 보고서 작성금지야.'\
                 '개조식은 금지야. 줄글로 작성해줘,하나의 문단에 내용을 넣어.'\
                 '네가 제공하는 정보는 수출입을 하는 업체들을 위한 정보야,' \
                 '글자수는 300자 이상 400자로 제한해줘, 가장 최근 데이터는 예측 데이터야,'\
                 '고객에게 제공하는 정보이니 문체를 정중하게 작성해줘,' \
                 '줄글로 작성해줘, 하나의 문단에 내용을 넣어.' \
                 '인덱스 수치를 대략적으로 언급해, 말할땐 포인트라고 해' \
                 '데이터를 요약 및 분석해줘, insight 제공해줘, 추가분석은 필요 없어,' \
                 '상승과 하락을 예측하지마, 보고서 작성 금지야' \
                 '네가 학습한 기간의 국제 정세와 지수의 추세를 연관 시키도록해' \
                 '네가 2024년 금리를 지수의 추세와 연관 시켜 가능성 언급'\
                 '결론을 제공해줘' \



    print(f'{time.time() - start_time} sec')
    print(response['message']['content'])



tmp = define_ccfi_data()
input_json_ccfi = str(tmp)

input_indx_info = 'ccfi_cach_expo는 CCFI를 나타내는 칼럼명이야,넌 CCFI를 분석하는거야,' \
             'CCFI는 China Containerized Freight Index의 약자야'

input_text = '인사하지마. 보고서 작성금지야.'\
             '개조식은 금지야. 줄글로 작성해줘,하나의 문단에 내용을 넣어.'\
             '네가 제공하는 정보는 수출입을 하는 업체들을 위한 정보야,' \
             '글자수는 300자 이상 400자로 제한해줘, 가장 최근 데이터는 예측 데이터야,'\
             '고객에게 제공하는 정보이니 문체를 정중하게 작성해줘,' \
             '줄글로 작성해줘, 하나의 문단에 내용을 넣어.' \
             'CCFI 수치를 대략적으로 언급해' \
             '데이터를 요약 및 분석해줘, insight 제공해줘, 추가분석은 필요 없어,' \
             '상승과 하락을 예측하지마, 보고서 작성 금지야' \
             '네가 학습한 기간의 국제 정세와 지수의 추세를 연관 시키도록해' \
             '네가 2024년 금리를 지수의 추세와 연관 시켜 가능성 언급'\
             '결론을 제공해줘' \

# ', 말할땐 포인트라고 해' \

prompt = input_text + input_indx_info + input_json_ccfi
import time
start_time = time.time()

response = ollama.chat(model='gemma2', messages=[{
    'role': 'user',
    'content': prompt
},
])

print(f'{time.time() - start_time} sec')
print(response['message']['content'])