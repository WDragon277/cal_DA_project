from models.insight_g.sea_freight.repository import exp_info_list, imp_info_list, input_order, sea_freight_tables, key_list
from models.insight_g.model import ChatService
import logging

model = ChatService()

logging.basicConfig(filename='INFO.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

exp_json_list = []
results_list = []
total_info_list = imp_info_list+exp_info_list

for i in range(34):
    tmp = model.df_to_json(sea_freight_tables[key_list[i]])()
    # tmp = model.clean_json(tmp)

    result_sea = model.get_content(input_order=input_order,
                                   input_info=total_info_list[i],
                                   input_json=tmp)

    results_list.append(result_sea)