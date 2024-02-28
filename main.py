import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import shutil
import os
import csv

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


# Функция для загрузки данных из SQL скрипта
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


# sql_load('ddl_dml.sql', conf, 'bank')

# Функция для загрузки данных из csv файла и перенос файла в архив
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

# csv2sql("data/transactions_01032021.txt", conf, 'STG_TRANSACTIONS', 'bank')


# Загрузка данных из Excel и перенос файла в архив
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


# excel2sql("data/terminals_01032021.xlsx", conf, 'STG_TERMINALS', 'bank')
# excel2sql("data/passport_blacklist_01032021.xlsx", conf, 'STG_PASSPORT_BLACKLIST', 'bank')




# Функция для создания таблиц фактов
def create_dwh_fact():
    # Создание таблицы фактов транзакций
    cursor.execute("""
            CREATE TABLE if not exists "DWH_FACT_TRANSACTIONS" (
                trans_id VARCHAR(128),
                trans_date TIMESTAMP,
                card_num VARCHAR(128),
                oper_type VARCHAR(128),
                amt NUMERIC,
                oper_result VARCHAR(128),
                terminal VARCHAR(128)
            )
        """)
    # Создание таблицы фактов черного списка паспортов
    cursor.execute("""
            CREATE TABLE if not exists "DWH_FACT_PASSPORT_BLACKLIST" (
                passport_num VARCHAR(128),
                entry_dt DATE
            )
            """)
    conn.commit()

# create_dwh_fact()

# Функция для добавления данных в таблицы фактов
def update_dwh_fact():
    # Добавление данных из стейдж таблицы в таблицу фактов транзакций
    cursor.execute("""
            INSERT INTO "DWH_FACT_TRANSACTIONS" (
                trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal
            )
            SELECT
                transaction_id, transaction_date::TIMESTAMP, card_num, oper_type,
                CAST(REPLACE(amount, ',', '.') AS NUMERIC), oper_result, terminal
                from "STG_TRANSACTIONS"
            """)
    # Добавление данных из стейдж таблицы в таблицу фактов черного списка паспортов
    cursor.execute("""
            INSERT INTO "DWH_FACT_PASSPORT_BLACKLIST" (
                passport_num, entry_dt
            )
            SELECT
                passport::VARCHAR, date::DATE
            FROM "STG_PASSPORT_BLACKLIST"
            """)
    conn.commit()

# update_dwh_fact()



# Функция для создания таблиц типа SCD1
def create_dwh_dim_tables():
    # Создание таблицы клиентов
    cursor.execute("""
            CREATE TABLE if not exists "DWH_DIM_CLIENTS" (
                client_id VARCHAR(128),
                last_name VARCHAR(128),
                first_name VARCHAR(128),
                patronymic VARCHAR(128),
                date_of_birth DATE,
                passport_num VARCHAR(128),
                passport_valid_to DATE,
                phone VARCHAR(128),
                create_dt DATE default current_date,
                update_dt DATE default current_date
            )
            """)
    # Создание таблицы аккаунтов
    cursor.execute("""
            CREATE TABLE if not exists "DWH_DIM_ACCOUNTS" (
                account_num VARCHAR(128),
                valid_to DATE,
                client VARCHAR(128),
                create_dt DATE default current_date,
                update_dt DATE default current_date
            )
            """)
   # Создание таблицы c карточками
    cursor.execute("""
            CREATE TABLE if not exists "DWH_DIM_CARDS" (
                card_num VARCHAR(128),
                account_num VARCHAR(128),
                create_dt DATE default current_date,
                update_dt DATE default current_date
            )
            """)
    conn.commit()

# create_dwh_dim_tables()



# Функция для добавления данных в таблицы типа SCD1
def update_dwh_dim_tables():
    cursor.execute("""
            INSERT INTO "DWH_DIM_CARDS" (
                card_num, account_num
            )
            SELECT
                card_num, account
                from cards
            """)
    cursor.execute("""
            INSERT INTO "DWH_DIM_ACCOUNTS" (
                account_num, valid_to, client
            )
            SELECT
                account, valid_to, client
                from accounts
            """)
    cursor.execute("""
            INSERT INTO "DWH_DIM_CLIENTS" (
                client_id, last_name, first_name, patronymic, date_of_birth,
                passport_num, passport_valid_to, phone
            )
            SELECT
                client_id, last_name, first_name, patronymic, date_of_birth,
                passport_num, passport_valid_to, phone
                from clients
            """)
    cursor.execute('drop table if exists cards')
    cursor.execute('drop table if exists accounts')
    cursor.execute('drop table if exists clients')
    conn.commit()

# update_dwh_dim_tables()

def create_function():
    cursor.execute("""
            CREATE OR REPLACE FUNCTION update_trigger_function()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.update_dt = now();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            DROP TRIGGER IF EXISTS trg_update_dwh_dim_clients on "DWH_DIM_CLIENTS";
            DROP TRIGGER IF EXISTS trg_update_dwh_dim_clients on "DWH_DIM_ACCOUNTS";
            DROP TRIGGER IF EXISTS trg_update_dwh_dim_clients on "DWH_DIM_CARDS";
            
        """)
    conn.commit()

# create_function()

def create_trigger():
    cursor.execute("""
            CREATE TRIGGER trg_update_dwh_dim_clients
            BEFORE UPDATE ON "DWH_DIM_CLIENTS"
            FOR EACH ROW
            EXECUTE FUNCTION update_trigger_function();
            
            CREATE TRIGGER trg_update_dwh_dim_clients
            BEFORE UPDATE ON "DWH_DIM_ACCOUNTS"
            FOR EACH ROW
            EXECUTE FUNCTION update_trigger_function();
            
            CREATE TRIGGER trg_update_dwh_dim_clients
            BEFORE UPDATE ON "DWH_DIM_CARDS"
            FOR EACH ROW
            EXECUTE FUNCTION update_trigger_function();
            
            
    """)
    conn.commit()

# create_trigger()

# Функция для создания исторической таблицы терминалов типа SCD2 и представления с этими данными
def create_terminal_hist():
    cursor.execute("""
        CREATE TABLE if not exists "DWH_DIM_TERMINALS_HIST" (
            id serial primary key,
            terminal_id VARCHAR(128),
            terminal_type VARCHAR(128),
            terminal_city VARCHAR(128),
            terminal_address VARCHAR(128),
            effective_from TIMESTAMP default CURRENT_TIMESTAMP,
            effective_to TIMESTAMP default (to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')),
            deleted_flg INTEGER default 0
            )
        """)

    cursor.execute("""DROP VIEW if exists view_terminal_hist""")

    cursor.execute("""
        CREATE VIEW view_terminal_hist AS
        SELECT
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address
        FROM "DWH_DIM_TERMINALS_HIST"
        WHERE current_timestamp BETWEEN effective_from and effective_to;
    """)

# create_terminal_hist()

# Функция для создания таблицы с новыми данными о терминалах
def create_terminals_new_rows():
    cursor.execute("""
        CREATE TABLE tmp_new_rows AS
            SELECT
                t1.*
            FROM "STG_TERMINALS" t1
            LEFT JOIN view_terminal_hist t2 ON t1.terminal_id = t2.terminal_id
            WHERE t2.terminal_id is null
        """)
    conn.commit()

# create_terminals_new_rows()

def create_deleted_terminals_rows():
    cursor.execute("""
        CREATE TABLE tmp_deleted_rows AS
            SELECT
                t1.*
            FROM view_terminal_hist t1
            LEFT JOIN "STG_TERMINALS" t2 ON t1.terminal_id = t2.terminal_id
            WHERE t2.terminal_id is null
        """)
    conn.commit()

# create_deleted_terminals_rows()

def create_update_terminals_rows():
    cursor.execute("""
        CREATE TABLE tmp_updated_rows AS
            SELECT t1.*
            FROM "STG_TERMINALS" t1
            inner join view_terminal_hist t2 on t1.terminal_id = t2.terminal_id
            and (
                t1.terminal_type <> t2.terminal_type
                or t1.terminal_city <> t2.terminal_city
                or t1.terminal_address <> t2.terminal_address
            )
        """)
    conn.commit()

# create_update_terminals_rows()

# Функция для добавления данных в таблицу terminal_hist
def update_terminal_hist():
    # Добавление новых данных
    cursor.execute("""
        UPDATE "DWH_DIM_TERMINALS_HIST"
        SET effective_to = date_trunc('second', now() - interval '1 second'),
        deleted_flg = 1
        WHERE terminal_id in(SELECT terminal_id from tmp_deleted_rows)
        AND effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
        """)

    cursor.execute("""
        INSERT INTO "DWH_DIM_TERMINALS_HIST" (
            terminal_id, terminal_type, terminal_city, terminal_address
        )
        SELECT terminal_id, terminal_type, terminal_city, terminal_address
            from tmp_new_rows
        """)
    cursor.execute("""
            UPDATE "DWH_DIM_TERMINALS_HIST"
            SET effective_to = DATE_TRUNC('second', now() - interval '1 second')
            WHERE terminal_id in(SELECT terminal_id FROM tmp_updated_rows)
            and effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
        """)
    cursor.execute("""
          INSERT INTO "DWH_DIM_TERMINALS_HIST" (
              terminal_id, terminal_type, terminal_city, terminal_address
          )
          SELECT terminal_id, terminal_type, terminal_city, terminal_address
              from tmp_updated_rows
          """)

    conn.commit()

# update_terminal_hist()

def remove_tmp_tables():
    cursor.execute("DROP TABLE if exists tmp_new_rows")
    cursor.execute("DROP TABLE if exists tmp_deleted_rows")
    cursor.execute("DROP TABLE if exists tmp_updated_rows")
    conn.commit()


# remove_tmp_tables()

# Создание таблицы витрины с отчетом
def create_rep_fraud():
    cursor.execute("""
        CREATE TABLE if not exists "REP_FRAUD" (
            event_dt TIMESTAMP,
            passport VARCHAR(128),
            fio VARCHAR(128),
            phone VARCHAR(128),
            event_type VARCHAR(128),
            report_dt TIMESTAMP default CURRENT_TIMESTAMP
            )
        """)
    conn.commit()

# create_rep_fraud()




# Создание представления для получения данных о просроченных паспортах
def check_passport_valid_to():
    cursor.execute("""
        CREATE VIEW view_passport_valid_to AS
        SELECT 
                t1.trans_date AS event_dt,
                t4.passport_num AS passport,
                CONCAT_WS(' ', t4.last_name, t4.first_name, t4.patronymic) AS fio,
                phone,
                now() AS report_dt

        FROM  "DWH_FACT_TRANSACTIONS" t1
        JOIN  "DWH_DIM_CARDS" t2 ON t1.card_num = t2.card_num
        JOIN  "DWH_DIM_ACCOUNTS" t3 ON t2.account_num = t3.account_num
        JOIN  "DWH_DIM_CLIENTS" t4 ON t3.client = t4.client_id
        WHERE CAST(t4.passport_valid_to as DATE) < DATE(t1.trans_date) AND t1.oper_result = 'SUCCESS'
        """)
    conn.commit()



# Создание представления для получения данных о недействующем договоре
def check_account_valid_to():
    cursor.execute(""" 
        CREATE VIEW view_account_valid_to AS
        SELECT
                t4.trans_date as event_dt,
                t1.passport_num as passport,
                concat_ws(' ', t1.last_name, t1.first_name, t1.patronymic) as fio,
                t1.phone as phone,
                now() as report_dt
                
        FROM "DWH_DIM_CLIENTS" t1
        JOIN "DWH_DIM_ACCOUNTS" t2 ON t1.client_id = t2.client
        JOIN "DWH_DIM_CARDS" t3 ON t2.account_num = t3.account_num
        JOIN "DWH_FACT_TRANSACTIONS" t4 ON t3.card_num = t4.card_num 
        WHERE t4.oper_result = 'SUCCESS' and cast(t4.trans_date as DATE) > DATE(t2.valid_to) 
    """)
    conn.commit()


# Добавление данных в витрину
def update_rep_fraud():
    cursor.execute("""
        INSERT INTO "REP_FRAUD" (
            event_dt, passport, fio, phone, event_type, report_dt
        )
        SELECT 
            event_dt, passport, fio, phone, 'account expired', report_dt
        FROM view_account_valid_to
    """)
    cursor.execute("""
        INSERT INTO "REP_FRAUD" (
            event_dt, passport, fio, phone, event_type, report_dt
        )
        SELECT
            event_dt, passport, fio, phone, 'passport expired', report_dt
        FROM view_passport_valid_to
        """)
    conn.commit()




# Удаление представлений перед новым запуском
def remove_view():
    cursor.execute("""DROP VIEW view_passport_valid_to""")
    cursor.execute("""DROP VIEW view_account_valid_to""")
    conn.commit()



# Функция для удаления таблиц типа SCD1 перед новым запуском программы
def remove_dim_tables():
    cursor.execute("""DROP TABLE if exists "DWH_DIM_CLIENTS" """)
    cursor.execute("""DROP TABLE if exists "DWH_DIM_ACCOUNTS" """)
    cursor.execute("""DROP TABLE if exists "DWH_DIM_CARDS" """)
    conn.commit()



def remove_fact_passport_table():
    cursor.execute("""DROP TABLE if exists "DWH_FACT_PASSPORT_BLACKLIST" """)
    conn.commit()



remove_tmp_tables()
remove_fact_passport_table()
remove_dim_tables()
sql_load('ddl_dml.sql', conf, 'bank')
csv2sql("data/transactions_01032021.txt", conf, 'STG_TRANSACTIONS', 'bank')
excel2sql("data/terminals_01032021.xlsx", conf, 'STG_TERMINALS', 'bank', )
excel2sql("data/passport_blacklist_01032021.xlsx", conf, 'STG_PASSPORT_BLACKLIST', 'bank')
create_dwh_fact()
update_dwh_fact()
create_dwh_dim_tables()
update_dwh_dim_tables()
create_function()
create_trigger()
create_terminal_hist()
create_terminals_new_rows()
create_deleted_terminals_rows()
create_update_terminals_rows()
update_terminal_hist()
create_rep_fraud()
check_passport_valid_to()
check_account_valid_to()
update_rep_fraud()
remove_view()

