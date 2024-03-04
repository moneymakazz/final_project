import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import shutil
import os

# Устанавливаем соединение с базой данных PostgreSQL
conf = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "kcbr82pg",
    "port": "5432"
}
conn = psycopg2.connect(**conf)
cursor = conn.cursor()
cursor.execute("create schema if not exists bank")
cursor.execute("set search_path to bank")
conn.commit()


# Функция загрузки данных из SQL скрипта
def sql_load(file_path, conf, schema_name):
    try:
        conf = psycopg2.connect(**conf)
        cursor = conn.cursor()

        with open(file_path, 'r', encoding='utf-8') as file:
            script = file.read()
            cursor.execute(script)
        conn.commit()
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")


# Функция загрузки данных из csv файла и перенос файла в архив
def csv2sql(file_path, conf, table_name, schema_name):
    try:
        engine = create_engine(f'postgresql://{conf["user"]}:{conf["password"]}@{conf["host"]}:{conf["port"]}/{conf["database"]}')
        df = pd.read_csv(file_path, delimiter=';')
        df.to_sql(table_name, engine, schema=schema_name, if_exists='replace', index=False)
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join('archive', f'{file_name}.backup')
            shutil.move(file_path, destination_path)
        conn.commit()
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")


# Функция загрузки данных из Excel файла и перенос файла в архив
def excel2sql(file_path, conf, table_name, schema_name):
    try:
        engine = create_engine(f'postgresql://{conf["user"]}:{conf["password"]}@{conf["host"]}:{conf["port"]}/{conf["database"]}')
        df = pd.read_excel(file_path)
        df.to_sql(table_name, engine, schema_name, if_exists='replace', index=False)
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join('archive', f'{file_name}.backup')
            shutil.move(file_path, destination_path)
        conn.commit()
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")