import pandas as pd
import seaborn as sns
from elasticsearch import Elasticsearch # elasticsearch==8.8.2 필요
from matplotlib import pyplot as plt

from common.utils.setting import EsSetting

esinfo = EsSetting()

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def searchAPI(index_name):

    '''
    Search data with index name of kibana_server
    and store all the data from the index (table) into 'res'
    :param index_name:
    :return res:
    '''

    esinfo = EsSetting()
    es = Elasticsearch(esinfo.IP, basic_auth=(esinfo.ID, esinfo.PW))
    index = index_name
    body = {
        # 'size': 10000,
        'query':{
            'match_all': {}
        }
    }
    result = es.search(index=index, body=body, size=10000)
    logger.info('데이터베이스에 접속 성공했습니다.')
    res = pd.DataFrame([hit['_source'] for hit in result['hits']['hits']])

    return res


# Delete the index
def delete_index(index_name):

    es = Elasticsearch(esinfo.IP, basic_auth=(esinfo.ID, esinfo.PW))  # ops

    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        logger.info("데이터 수정/삽입을 위해 기존의 인덱스가 제거되었습니다.")



# 인덱스 데이터 행렬 변환
def switch_idx_data(df):

    '''
    Make raw data into a DataFrame by sea freight index
    :param df:
    :return df_total:
    '''

    # 데이터 정렬
    df = df.sort_values('data_cd')

    # 인덱스 재설정
    df = df.reset_index().drop('index', axis=1)

    # 운송지수별 데이터프레임 만들기
    df_bdi = pd.DataFrame(df[df['data_cd'] == 'bdi'])
    df_ccfi = pd.DataFrame(df[df['data_cd'] == 'ccfi'])
    df_scfi = pd.DataFrame(df[df['data_cd'] == 'scfi'])
    df_hrci = pd.DataFrame(df[df['data_cd'] == 'hrci'])
    df_kcci = pd.DataFrame(df[df['data_cd'] == 'kcci'])

    # 숫자형 데이터로 변환(필요시)

    # df_bdi = pd.to_numeric(df_bdi)
    # df_ccfi = pd.to_numeric(df_ccfi)
    # df_scfi = pd.to_numeric(df_scfi)
    # df_hrci = pd.to_numeric(df_hrci)

    # kcci의 nan값을 float 타입으로 전환
    # df_kcci = df_kcci.astype(str)
    # df_kcci = df_kcci.applymap(lambda x: int(x) if x.isdigit() else float(x))

    # 칼럼명 변경
    df_bdi  = df_bdi.rename(columns={'cach_expo': 'bdi_cach_expo'})
    df_ccfi = df_ccfi.rename(columns={'cach_expo': 'ccfi_cach_expo'})
    df_scfi = df_scfi.rename(columns={'cach_expo': 'scfi_cach_expo'})
    df_hrci = df_hrci.rename(columns={'cach_expo': 'hrci_cach_expo'})
    df_kcci = df_kcci.rename(columns={'cach_expo': 'kcci_cach_expo'})

    # 코드 칼럼 삭제
    df_bdi  = df_bdi.drop('data_cd', axis=1)
    df_ccfi = df_ccfi.drop('data_cd', axis=1)
    df_scfi = df_scfi.drop('data_cd', axis=1)
    df_hrci = df_hrci.drop('data_cd', axis=1)
    df_kcci = df_kcci.drop('data_cd', axis=1)

    # 날짜인덱스 만들기
    max_date = df['rgsr_dt'].max()
    min_date = df['rgsr_dt'].min()
    dates = pd.date_range(min_date, max_date)
    df_dates = pd.DataFrame(dates)

    # 날짜 칼럼명 일치를 위한 변경
    df_dates = df_dates.rename(columns={0: 'rgsr_dt'})

    # 날짜 부분의 데이터 타입 변경
    df_dates['rgsr_dt'] = df['rgsr_dt'].astype(str).str.replace('-', '')

    #일자순으로 통합하기
    df_total = pd.merge(df_dates, df_bdi, how='outer', on='rgsr_dt')
    df_total = pd.merge(df_total, df_ccfi, how='outer', on='rgsr_dt')
    df_total = pd.merge(df_total, df_hrci, how='outer', on='rgsr_dt')
    df_total = pd.merge(df_total, df_scfi, how='outer', on='rgsr_dt')
    df_total = pd.merge(df_total, df_kcci, how='outer', on='rgsr_dt')

    #날짜 순으로 정렬 및 인덱스 설정
    df_total = df_total.sort_values(by='rgsr_dt')
    df_total = df_total.reindex()

    #중복 제거
    df_total = df_total.drop_duplicates(keep='first', inplace=False, ignore_index=True)

    logger.info('행열 변환에 성공하였습니다.')

    return df_total


def interpolation(df):
    result = df.interpolate()
    return result


# CCFI,SCFI와의 상관도 연산(HRCI를 정확하게 예측하기 위한 함수로써 높은 상관도 연산값을 확인하는데 이용됨)
def df_corr_hrci(df):

    '''
    To determine how many days the HRCI is leading,
    we obtain the correlation between CCFI, SCFI, and the date-shifted HRCI.
    The higher the correlation with the moved HRCI,
    the more accurate the date that the HRCI is leading.
    :param df:
    :return result:
    '''

    indx_corr = []
    # 데이터 변경 방지를 위한 임시 변수 생성
    df_tmp = pd.DataFrame(df)
    rang = range(30)

    for i in rang:
        j = -i
        # hrci 칼럼 이동시키기
        df_tmp['hrci_cach_expo_shift'] = df_tmp['hrci_cach_expo'].shift(j)
        # 이동된 칼럼과 다른 칼럼들의 상관도의 평균을 구한 뒤 리스트에 저장
        indx_corr.append([j, df_tmp[['hrci_cach_expo_shift', 'scfi_cach_expo', 'ccfi_cach_expo']].corr() \
                                 ['hrci_cach_expo_shift'][1:3].mean()])
        sorted_indx_corr = sorted(indx_corr, key=lambda x: x[1], reverse=True)
        result = sorted_indx_corr[0]
    logger.info(f"적절한 예측 기간과 상관도 : {result}")
    return result


# 인덱스로 활용할 수 있는 일자별 칼럼 생성
def make_dates(start_date):

    end_date = pd.Timestamp.now().strftime("%Y-%m-%d")
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")

    date_column = date_range.strftime("%Y%m%d")
    df_dates = pd.DataFrame({"rgsr_dt": date_column})

    return df_dates


# 입력한 인덱스에 해당하는 그래프 그리기
def draw_graph(df_total,index):
    #하나의 그래프(아티스트)에 모두 그리기
    plt.figure(figsize=(10,10))
    if  index == 'bulk':
        plt.plot(df_total['rgsr_dt']
                 ,df_total['bdi_cach_expo'], 'ro', linestyle='solid')
        plt.title('벌크 해상운임지수 그래프')
        plt.legend(['BDI'
                    ])
    if  index == 'containner':
        plt.plot(df_total['rgsr_dt']
                 , df_total['ccfi_cach_expo'], color='forestgreen', marker='^', markersize=6)
        plt.plot(df_total['rgsr_dt']
                 , df_total['hrci_cach_expo'], 'bo')
        plt.plot(df_total['rgsr_dt']
                 , df_total['scfi_cach_expo'], 'mo')
        plt.title('컨테이너 해상운임지수 3종 그래프')
        plt.legend(['CCFI'
                   , 'HRCI'
                   , 'SCFI'])
    if  index == 'containner2':
        plt.plot(df_total['rgsr_dt']
                 , df_total['ccfi_cach_expo'], color='forestgreen', marker='^', markersize=6)
        plt.plot(df_total['rgsr_dt']
                 , df_total['scfi_cach_expo'], 'mo')
        plt.plot(df_total['rgsr_dt']
                 , df_total['hrci_cach_expo_shifted'], 'yo')
        plt.title('컨테이너 해상운임지수 3종 그래프_(이동된)')
        plt.legend(['CCFI'
                   , 'SCFI'
                   , 'HRCI_Shifted30'])
    if index == 'total':
        plt.plot(df_total['rgsr_dt']
                 , df_total['bdi_cach_expo'], 'ro', linestyle='solid')
        plt.plot(df_total['rgsr_dt']
                 , df_total['ccfi_cach_expo'], color='forestgreen', marker='^', markersize=6, linestyle='solid')
        plt.plot(df_total['rgsr_dt']
                 , df_total['hrci_cach_expo'], 'bo', linestyle='solid')
        plt.plot(df_total['rgsr_dt']
                 , df_total['scfi_cach_expo'], 'mo', linestyle='solid')
        plt.plot(df_total['rgsr_dt']
                 , df_total['hrci_cach_expo_shifted'], 'yo')
        plt.title('해상운임지수 4종 그래프')
        plt.legend(['BDI'
                       , 'CCFI'
                       , 'HRCI'
                       , 'SCFI'
                       , 'HRCI_Shifted30'])
    plt.xlabel('Time')
    plt.xticks([0,110,220,330,440,536])
    plt.ylabel('Cach_expo')
    plt.show()


def draw_heatmap(df):
    heatmap = sns.heatmap(df.corr(), annot=True, cmap='coolwarm')
    heatmap.set_xticklabels(heatmap.get_xticklabels(), rotation=45, ha='right')
    plt.show()

# 데이터 불러오기

from elasticsearch import Elasticsearch as es
# from matplotlib import font_manager, rc
# font_path = "C:/Windows/Fonts/NGULIM.TTF"
# font = font_manager.FontProperties(fname=font_path).get_name()
# rc('font', family=font)


# 엘라스틱 서치에서 데이터 호출 "dgl_idx_expo_lst"
def raw_data(index):

    df = searchAPI(index)
    # 코드 데이터 칼럼화
    df_total = switch_idx_data(df)
    # 보간법 적용
    df_interpolated = interpolation(df_total)

    return df_interpolated


# 엘라스틱서치 doc 타입 설정 (데이터 입력시 활용)
def doc_type_setting(index):
    if index == 'dgl_idx_expo_pred':
        document = {
            "mappings": {
                "dynamic": False,
                "properties": {
                    "data_cd": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "rgsr_dt": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "cach_expo": {
                        "type": "integer"
                    }
                }
            }
        }
    return document

# 엘라스틱서치에 csv 데이터 입력


# if __name__=='__main__':
#     a = defined_data()
#     print(a)