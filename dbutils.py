# -*- coding: utf-8 -*-
from sqlalchemy import create_engine
import psycopg2.extras
from dotenv import load_dotenv
import os
from os.path import join, dirname

os.environ['HOST'] = 'sportmeme-analytics.ccshyl1cmnum.ap-northeast-1.rds.amazonaws.com'
os.environ['PORT'] = '5432'
os.environ['USER'] = 'sportmeme'
os.environ['PASS'] = 'H4fpyjde'
os.environ['DBNAME'] = 'sportmeme_analytics'

from sys import exit

USER = os.environ.get("USER")
PASS = os.environ.get("PASS")
HOST = os.environ.get("HOST")
PORT = os.environ.get("PORT")
DBNAME = os.environ.get("DBNAME")

if not all([USER, PASS, HOST, PORT, DBNAME]):
    ENV_FILE = "./.env"
    is_file = os.path.isfile(ENV_FILE)
    
    print(is_file)

    if is_file:
        load_dotenv(verbose=True)
        dotenv_path = join(dirname(__file__), ENV_FILE)
        load_dotenv(dotenv_path)
    else:
        exit()

    USER = os.environ.get("USER")
    PASS = os.environ.get("PASS")
    HOST = os.environ.get("HOST")
    PORT = os.environ.get("PORT")
    DBNAME = os.environ.get("DBNAME")

'''
ENGIN = create_engine(
    f'postgresql://{USER}:{PASS}@{HOST}:{PORT}/{DBNAME}?client_encoding=utf8', encoding="utf-8")
'''

ENGIN = create_engine(
    f'postgresql://{USER}:{PASS}@{HOST}:{PORT}/{DBNAME}',
    client_encoding='utf8')


# PostgresSQLに接続
def postgres_conn():
    conn = psycopg2.connect(dbname=DBNAME,
                            user=USER,
                            password=PASS,
                            host=HOST,
                            port=PORT)
    conn.set_client_encoding('utf-8')
    return conn


# PandasからDBへ
def pandas_to_db(df, table_name, exists='append'):
    df.to_sql(table_name, con=ENGIN, if_exists=exists, index=False)


# Selectクエリ
'''
def execute_query_select(sql_file, bind=None):
    with postgres_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            with open(sql_file, 'r', encoding="utf-8") as f:
                sql_query = f.read()
            if bind is None:
                cur.execute(sql_query)
            else:
                cur.execute(sql_query, bind)
            return [dict(row) for row in cur.fetchall()]
'''

def execute_query_select(sql_file, bind):
    with postgres_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            with open(sql_file, 'r', encoding="utf-8") as f:
                sql_query = f.read()
            if bind is None:
                bind = {}
                #cur.execute(sql_query)
            #else:
                #cur.execute(sql_query, bind)
            cur.execute(sql_query, bind)
            return [dict(row) for row in cur.fetchall()]


# クエリ
def execute_query(sql_file, bind=None):
    with postgres_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            with open(sql_file, 'r', encoding="utf-8") as f:
                sql_query = f.read()
            if bind is None:
                cur.execute(sql_query)
            else:
                cur.execute(sql_query, bind)
