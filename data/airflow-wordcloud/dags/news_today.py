from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import ssl
import json
from datetime import datetime
import datetime as dt

# 분석
from PyKomoran import *
from collections import Counter

# DB
from pymongo import MongoClient

import traceback


default_args = {
        'owner': 'nayeon',
        'depends_on_past': False,  # 이전 DAG의 Task가 성공, 실패 여부에 따라 현재 DAG 실행 여부가 결정. False는 과거의 실행 결과 상관없이 매일 실행한다
        'start_date': datetime(2023, 10, 1),
        'retires': 1,  # 실패시 재시도 횟수
        'retry_delay': timedelta(minutes=5)  # 만약 실패하면 5분 뒤 재실행
}

current_date = dt.date.today()
year = current_date.year
month = current_date.month
day = current_date.day

komoran = Komoran("EXP")



# 1. 실시간 크롤링
def news_2023():

        ssl._create_default_https_context = ssl._create_unverified_context  
      
        with open('./files/company_lastnum.json', 'r') as json_file:
                company_lastnum = json.load(json_file)

        df = pd.DataFrame(columns=['title', 'date', 'category', 'article'])

        for company in company_lastnum:

                lastIdx = company_lastnum[company]

                while True:
                        stop = False
                        article_num = f"{lastIdx:010d}"

                        try:
                                html = urlopen(f'https://n.news.naver.com/article/{company}/{article_num}')
                                bsObject = BeautifulSoup(html, "html.parser")
                                print(f'{company}언론사 {article_num}번')

                                # 카테고리
                                news_categories = bsObject.select(".media_end_categorize_item")
                                categories_lst = []
                                for news_category in news_categories:
                                        categories_lst.append(news_category.text)
                                category = categories_lst if categories_lst else ['']  

                                # 날짜
                                news_date = bsObject.select_one(".media_end_head_info_datestamp_time").text
                                date = " ".join(news_date.strip().split()) if news_date else ''

                                # 제목
                                news_title = bsObject.select_one("#title_area").text
                                title = " ".join(news_title.strip().split()) if news_title else ''

                                # 내용
                                news_content = bsObject.select_one("#newsct_article").text
                                content = " ".join(news_content.strip().split()) if news_content else ''
                                                                        
                                # DataFrame에 추가
                                df.loc[len(df)] = [title, date, category, content]

                                lastIdx += 1
                        
                        except:
                                    for i in range(lastIdx + 1, lastIdx + 12):

                                        article_num = f"{i:010d}"


                                        try:
                                                html = urlopen(f'https://n.news.naver.com/article/{company}/{article_num}')
                                                bsObject = BeautifulSoup(html, "html.parser")

                                                # 날짜
                                                news_date = bsObject.select_one(".media_end_head_info_datestamp_time").text
                                                date = " ".join(news_date.strip().split()) if news_date else ''

                                                lastIdx += 1
                                                break
                                                                                        
                                        except:
                                                if i == lastIdx + 11:
                                                        stop = True
                                                continue
                        
                        if stop:
                                company_lastnum[company] = lastIdx + 1
                                break

        with open('./files/company_lastnum.json', 'w') as json_file:
                json.dump(company_lastnum, json_file, indent=4) 

    

        table = pa.Table.from_pandas(df)

        
        parquet_file_path = f'./data/news_{year}_{month}_{day}.parquet'

        pq.write_table(table, parquet_file_path)
        
        print(f'마지막 번호 업데이트 -> {company_lastnum}')



# # 2. 분석 실행
def make_wordcloud():
        print('워클 함수 실행')
        
        parquet_file_path = f'./data/news_{year}_{month}_{day}.parquet'
        
        table = pq.read_table(parquet_file_path)
        df = table.to_pandas()

        # 카테고리 리스트 -> string
        df['category'] = df['category'].apply(lambda x: x[0] if len(x) > 0 else None)


        # 기사 내용에서 명사 추출 
        df['main'] = df['article'].apply(lambda x: komoran.nouns(x))
        
        # df를 artdic_dic 형식으로 바꾸기
        artdic_dic = {'경제': [], '사회': [], '생활': [], 'IT': [], '세계': []}

        for index, row in df.iterrows():
                item = {
                        'main': row['main'],
                        'title': row['title'],
                        'date': row['date']
                }
                category = row['category']
                
                # artdic_dic에 있는 카테고리만 처리
                if category in artdic_dic:
                        artdic_dic[category].append(item)

        art_df_dic = {section: pd.DataFrame(artdic_lst) for section, artdic_lst in artdic_dic.items()}


        # 불용어 제거
        stop_words = set()
        with open('./files/stop_words.txt', 'r', encoding='utf-8') as rf:
                for sw in rf.readlines():
                        stop_words.add(sw.rstrip())

        
        # 기사 제목에서 명사 추출
        poss_lst_dic = {}
        for section, art_df in art_df_dic.items():
                poss_lst = []
                for i in art_df.index:
                        ## 1.
                        title = art_df.loc[i, "title"]
                        nouns_lst = komoran.nouns(title)
                        ## 3.
                        for noun in nouns_lst:
                                if noun not in stop_words:
                                        poss_lst.append(noun)
                poss_lst_dic[section] = poss_lst

        
        # 단어 빈도수 계산
        words_dic = {}
        for section, poss_lst in poss_lst_dic.items():
                words = [element for element in poss_lst if len(element) > 1]
                words_dic[section] = words

        # 섹션별 최다 빈도수 상위 50개 단어 추출
        def item_to_dic(key, value):
                return {"text": key, "count": value}

        frequency_rank_dic = {}
        for section, words in words_dic.items():
                frequency_rank = Counter(words)
                items = [item_to_dic(k, v) for k, v in frequency_rank.most_common(50)]
                print(len(items))
                frequency_rank_dic[section] = items


        # 몽고디비에 워드 클라우드 보내자

        cate_dic = {'경제': '경제', '사회': '사회', '생활': '생활/문화', '세계': '세계', 'IT': 'IT/과학'}

        
        USER_NAME = 'elefante'
        PWD = 'E1efante1!'
        HOST = 'j9e101.p.ssafy.io'
        PORT = 27017

        client = MongoClient(f'mongodb://{USER_NAME}:{PWD}@{HOST}', PORT)

        mydb = client['market']
        mycol = mydb['wordcloud']

        for cate_code in cate_dic.keys():
                map_to_database = {
                        "scoreBoard": frequency_rank_dic[cate_code]
                }
                mycol.update_one({"category": cate_dic[cate_code]},
                                {"$set": {**map_to_database}},
                                upsert=True)

        for r in mycol.find():
                print(r)

with DAG(
    dag_id="news_2023",  
    description="make wordcloud & store in HDFS",  
    start_date=days_ago(2),  # DAG 정의 기준 2일 전부터 시작합니다.
    schedule_interval=timedelta(days=1),  # 1일을 주기로 실행합니다.
    tags=["crawl_dags"],
    default_args=default_args
) as dag:
     
        t1 = PythonOperator(
                task_id="news_2023",
                python_callable=news_2023,
        )

        t2 = PythonOperator(
                task_id="make_wordcloud",
                python_callable=make_wordcloud,
        )

        t1 >> t2
