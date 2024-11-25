import ollama
from models.freight_p.sea_freight_index.scfi_p.repository import scfi_raw_data
from models.insight_g.sea_freight_index.repository import input_scfi_info

from models.freight_p.sea_freight_index.hrci_p.repository import hrci_redifined_data
from models.insight_g.sea_freight_index.repository import input_hrci_info


from models.insight_g.sea_freight_index.repository import input_text


class ChatService:

    def __init__(self, model="gemma2"):
        self.model = model

    # NaN값 삭제
    # json 타입 데이터 str 전환
    def clean_json(self, input_json):
        input_indx_json = input_json.dropna()
        input_indx_json = str(input_indx_json)
        return input_indx_json

    def get_content(self, input_order, input_indx_info, input_indx_json):
        prompt = input_order + input_indx_info + input_indx_json  # 텍스트 순서에 따라 결과 달라짐
        response = ollama.chat(model=self.model, messages=[{'role': 'user', 'content': prompt}])
        content_generated = response['message']['content']
        return content_generated

    def save_content(self):
        return

gemma2 = ChatService()
input_scfi_json = gemma2.clean_json(scfi_raw_data())
content = gemma2.get_content(input_order=input_text,
                             input_indx_info=input_scfi_info,
                             input_indx_json=input_scfi_json)

gemma2 = ChatService()
input_hrci_json = gemma2.clean_json(hrci_redifined_data())
content_2 = gemma2.get_content(input_order=input_text,
                             input_indx_info=input_hrci_info,
                             input_indx_json=input_hrci_json)

test = 0