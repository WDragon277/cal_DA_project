import ollama
import time
import psycopg2

from common.utils.setting import PostgreSQL

rdb_info = PostgreSQL()

import logging
logging.basicConfig(filename='error.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class ChatService:

    def __init__(self, model="gemma2"):
        self.model = model

    # NaN값 삭제
    # json 타입 데이터 str 전환
    def clean_json(self, input_json):
        input_indx_json = input_json.dropna()
        input_indx_json = str(input_indx_json)
        return input_indx_json

    def get_content(self, input_order, input_info, input_json):
        start = time.time()
        prompt = input_order + input_info + input_json  # 주의) 프롬프트의 텍스트 순서에 따라 결과 달라짐
        response = ollama.chat(model=self.model, messages=[{'role': 'user', 'content': prompt}])
        content_generated = response['message']['content']
        estimated_time = time.time() - start
        # print(estimated_time)
        logging.info(f'Time to generation: {round(estimated_time, 2)}s ')

        return content_generated

    def conn_cheonan_db(self):

        # setting에 utf-8을 인코딩에 활용할 수 있도록 명시함(긴 한글문장 DB 전송에 필수)
        # 데이터베이스 연결
        conn = psycopg2.connect(
            dbname=rdb_info.dbname_cheonan,
            user=rdb_info.ID,
            password=rdb_info.PW,
            host=rdb_info.IP,
            port=rdb_info.port,
            options="-c client_encoding=UTF8"
        )
        return conn

    def save_data_postdb(self, data):

        try:
            conn = self.conn_cheonan_db()
            cursor = conn.cursor()

            # SQL 삽입 쿼리
            table_name = rdb_info.tbname_cheonan
            columns = ', '.join(data.keys())
            placeholders = ', '.join(['%s'] * len(data))
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            data = data.values()
            data = tuple(data)

            # 데이터 삽입
            cursor.execute(sql, data)
            conn.commit()  # 변경 사항 저장

        except Exception as e:

            print(f"An error occurred: {e}")


    def select_all(self):
        # SELECT 쿼리 실행
        try:
            conn = self.conn_cheonan_db()
            cursor = conn.cursor()
            sql = f"SELECT * FROM {rdb_info.tbname_cheonan}"
            cursor.execute(sql)  # 쿼리 실행

            # 결과 가져오기
            rows = cursor.fetchall()  # 모든 행 가져오기
            for row in rows:
                print(row)

        except Exception as e:
            print(f"An error occurred: {e}")

        finally:
            cursor.close()
            conn.close()
