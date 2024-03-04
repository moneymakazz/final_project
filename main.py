from py_scripts.download_data import conf, sql_load, csv2sql, excel2sql
from py_scripts.fact_and_dim_tables import (
    create_dwh_fact, create_dwh_dim_tables,
    update_dwh_fact, update_dwh_dim_tables
)
from py_scripts.service import create_function, create_trigger
from py_scripts.terminals_hist import (
    create_terminal_hist, create_terminals_new_rows,
    create_update_terminals_rows, create_deleted_terminals_rows,
    update_terminal_hist
)
from py_scripts.rep_fraud import create_rep_fraud, update_rep_fraud
from py_scripts.clear_data import remove_view, remove_tables, remove_tmp_tables
from py_scripts.check_transactions import (
    check_different_city, check_passport_valid_to,
    check_account_valid_to, check_passport_in_blacklist
)

if __name__ == "__main__":
    remove_tmp_tables()
    remove_tables()
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
    check_passport_in_blacklist()
    check_different_city()
    update_rep_fraud()
    remove_view()















# # Устанавливаем соединение с базой данных PostgreSQL
# conf = {
#     "host": "localhost",
#     "database": "postgres",
#     "user": "postgres",
#     "password": "kcbr82pg",
#     "port": "5432"
# }
# conn = psycopg2.connect(**conf)
# cursor = conn.cursor()
# cursor.execute("create schema if not exists bank")
# cursor.execute("set search_path to bank")
# conn.commit()
#
#
# # Функция загрузки данных из SQL скрипта
# def sql_load(file_path, conf, schema_name):
#     try:
#         conf = psycopg2.connect(**conf)
#         cursor = conn.cursor()
#
#         with open(file_path, 'r', encoding='utf-8') as file:
#             script = file.read()
#             cursor.execute(script)
#         conn.commit()
#     except Exception as e:
#         print(f"Произошла ошибка: {str(e)}")
#
#
# # Функция загрузки данных из csv файла и перенос файла в архив
# def csv2sql(file_path, conf, table_name, schema_name):
#     try:
#         engine = create_engine(f'postgresql://{conf["user"]}:{conf["password"]}@{conf["host"]}:{conf["port"]}/{conf["database"]}')
#         df = pd.read_csv(file_path, delimiter=';')
#         df.to_sql(table_name, engine, schema=schema_name, if_exists='replace', index=False)
#         if os.path.exists(file_path):
#             file_name = os.path.basename(file_path)
#             destination_path = os.path.join('archive', f'{file_name}.backup')
#             shutil.move(file_path, destination_path)
#         conn.commit()
#     except Exception as e:
#         print(f"Произошла ошибка: {str(e)}")
#
#
# # Функция загрузки данных из Excel файла и перенос файла в архив
# def excel2sql(file_path, conf, table_name, schema_name):
#     try:
#         engine = create_engine(f'postgresql://{conf["user"]}:{conf["password"]}@{conf["host"]}:{conf["port"]}/{conf["database"]}')
#         df = pd.read_excel(file_path)
#         df.to_sql(table_name, engine, schema_name, if_exists='replace', index=False)
#         if os.path.exists(file_path):
#             file_name = os.path.basename(file_path)
#             destination_path = os.path.join('archive', f'{file_name}.backup')
#             shutil.move(file_path, destination_path)
#         conn.commit()
#     except Exception as e:
#         print(f"Произошла ошибка: {str(e)}")


# # Функция создания таблиц фактов
# def create_dwh_fact():
#     # Создание таблицы фактов транзакций
#     cursor.execute("""
#             CREATE TABLE IF NOT EXISTS "DWH_FACT_TRANSACTIONS" (
#                 trans_id VARCHAR(128),
#                 trans_date TIMESTAMP,
#                 card_num VARCHAR(128),
#                 oper_type VARCHAR(128),
#                 amt NUMERIC,
#                 oper_result VARCHAR(128),
#                 terminal VARCHAR(128)
#             )
#         """)
#     # Создание таблицы фактов черного списка паспортов
#     cursor.execute("""
#             CREATE TABLE IF NOT EXISTS "DWH_FACT_PASSPORT_BLACKLIST" (
#                 passport_num VARCHAR(128),
#                 entry_dt DATE
#             )
#             """)
#     conn.commit()
#
#
# # Функция добавления данных в таблицы фактов
# def update_dwh_fact():
#     # Добавление данных из стейдж таблицы в таблицу фактов транзакций
#     cursor.execute("""
#             INSERT INTO "DWH_FACT_TRANSACTIONS" (
#                 trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal
#             )
#             SELECT
#                 transaction_id, transaction_date::TIMESTAMP, card_num, oper_type,
#                 CAST(REPLACE(amount, ',', '.') AS NUMERIC), oper_result, terminal
#                 FROM "STG_TRANSACTIONS"
#             """)
#     # Добавление данных из стейдж таблицы в таблицу фактов черного списка паспортов
#     cursor.execute("""
#             INSERT INTO "DWH_FACT_PASSPORT_BLACKLIST" (
#                 passport_num, entry_dt
#             )
#             SELECT
#                 passport::VARCHAR, date::DATE
#             FROM "STG_PASSPORT_BLACKLIST"
#             """)
#     conn.commit()
#
#
# # Функция создания таблиц типа SCD1
# def create_dwh_dim_tables():
#     # Создание таблицы клиентов
#     cursor.execute("""
#             CREATE TABLE IF NOT EXISTS "DWH_DIM_CLIENTS" (
#                 client_id VARCHAR(128),
#                 last_name VARCHAR(128),
#                 first_name VARCHAR(128),
#                 patronymic VARCHAR(128),
#                 date_of_birth DATE,
#                 passport_num VARCHAR(128),
#                 passport_valid_to DATE,
#                 phone VARCHAR(128),
#                 create_dt DATE default current_date,
#                 update_dt DATE default current_date
#             )
#             """)
#     # Создание таблицы аккаунтов
#     cursor.execute("""
#             CREATE TABLE IF NOT EXISTS "DWH_DIM_ACCOUNTS" (
#                 account_num VARCHAR(128),
#                 valid_to DATE,
#                 client VARCHAR(128),
#                 create_dt DATE default current_date,
#                 update_dt DATE default current_date
#             )
#             """)
#    # Создание таблицы с картами
#     cursor.execute("""
#             CREATE TABLE IF NOT EXISTS "DWH_DIM_CARDS" (
#                 card_num VARCHAR(128),
#                 account_num VARCHAR(128),
#                 create_dt DATE default current_date,
#                 update_dt DATE default current_date
#             )
#             """)
#     conn.commit()
#
#
# # Функция добавления данных в таблицы типа SCD1
# def update_dwh_dim_tables():
#     cursor.execute("""
#             INSERT INTO "DWH_DIM_CARDS" (
#                 card_num, account_num
#             )
#             SELECT
#                 card_num, account
#                 from cards
#             """)
#     cursor.execute("""
#             INSERT INTO "DWH_DIM_ACCOUNTS" (
#                 account_num, valid_to, client
#             )
#             SELECT
#                 account, valid_to, client
#                 from accounts
#             """)
#     cursor.execute("""
#             INSERT INTO "DWH_DIM_CLIENTS" (
#                 client_id, last_name, first_name, patronymic, date_of_birth,
#                 passport_num, passport_valid_to, phone
#             )
#             SELECT
#                 client_id, last_name, first_name, patronymic, date_of_birth,
#                 passport_num, passport_valid_to, phone
#                 from clients
#             """)
#     cursor.execute('DROP TABLE IF EXISTS cards')
#     cursor.execute('DROP TABLE IF EXISTS accounts')
#     cursor.execute('DROP TABLE IF EXISTS clients')
#     conn.commit()

# # Функция создания хранимой функции для обновления значения update_dt, в случае изменения данных в таблицах типа SCD1
# def create_function():
#     cursor.execute("""
#             CREATE OR REPLACE FUNCTION update_trigger_function()
#             RETURNS TRIGGER AS $$
#             BEGIN
#                 NEW.update_dt = NOW();
#                 RETURN NEW;
#             END;
#             $$ LANGUAGE plpgsql;
#
#             DROP TRIGGER IF EXISTS trg_update_dwh_dim_clients on "DWH_DIM_CLIENTS";
#             DROP TRIGGER IF EXISTS trg_update_dwh_dim_clients on "DWH_DIM_ACCOUNTS";
#             DROP TRIGGER IF EXISTS trg_update_dwh_dim_clients on "DWH_DIM_CARDS";
#             """)
#     conn.commit()
#
# # Функция создания триггера, который запускает функцию update_trigger_function
# def create_trigger():
#     cursor.execute("""
#             CREATE TRIGGER trg_update_dwh_dim_clients
#             BEFORE UPDATE ON "DWH_DIM_CLIENTS"
#             FOR EACH ROW
#             EXECUTE FUNCTION update_trigger_function();
#
#             CREATE TRIGGER trg_update_dwh_dim_clients
#             BEFORE UPDATE ON "DWH_DIM_ACCOUNTS"
#             FOR EACH ROW
#             EXECUTE FUNCTION update_trigger_function();
#
#             CREATE TRIGGER trg_update_dwh_dim_clients
#             BEFORE UPDATE ON "DWH_DIM_CARDS"
#             FOR EACH ROW
#             EXECUTE FUNCTION update_trigger_function();
#             """)
#     conn.commit()


# Функция создания исторической таблицы терминалов типа SCD2 и представления с этими данными
# def create_terminal_hist():
#     cursor.execute("""
#         CREATE TABLE IF NOT EXISTS "DWH_DIM_TERMINALS_HIST" (
#             id SERIAL PRIMARY KEY,
#             terminal_id VARCHAR(128),
#             terminal_type VARCHAR(128),
#             terminal_city VARCHAR(128),
#             terminal_address VARCHAR(128),
#             effective_from TIMESTAMP default CURRENT_TIMESTAMP,
#             effective_to TIMESTAMP default (to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')),
#             deleted_flg INTEGER default 0
#             )
#         """)
#
#     cursor.execute("""DROP VIEW IF EXISTS "STG_VIEW_TERMINAL_HIST" """)
#
#     cursor.execute("""
#         CREATE VIEW "STG_VIEW_TERMINAL_HIST" AS
#         SELECT
#             terminal_id,
#             terminal_type,
#             terminal_city,
#             terminal_address
#         FROM "DWH_DIM_TERMINALS_HIST"
#         WHERE deleted_flg = 0
#         and CURRENT_TIMESTAMP BETWEEN effective_from and effective_to;
#     """)
#
#
# # Функция создания таблицы с новыми данными о терминалах
# def create_terminals_new_rows():
#     cursor.execute("""
#         CREATE TABLE "STG_TMP_NEW_ROWS" AS
#             SELECT
#                 t1.*
#             FROM "STG_TERMINALS" t1
#             LEFT JOIN "STG_VIEW_TERMINAL_HIST" t2 ON t1.terminal_id = t2.terminal_id
#             WHERE t2.terminal_id IS NULL
#         """)
#     conn.commit()
#
# # Функция создания таблицы с удаленными данными о терминалах
# def create_deleted_terminals_rows():
#     cursor.execute("""
#         CREATE TABLE "STG_TMP_DELETED_ROWS" AS
#             SELECT
#                 t1.*
#             FROM "STG_VIEW_TERMINAL_HIST" t1
#             LEFT JOIN "STG_TERMINALS" t2
#             ON t1.terminal_id = t2.terminal_id
#             WHERE t2.terminal_id IS NULL
#         """)
#     conn.commit()
#
#
# # Функция создания таблицы с измененными данными о терминалах
# def create_update_terminals_rows():
#     cursor.execute("""
#         CREATE TABLE "STG_TMP_UPDATED_ROWS" AS
#             SELECT t2.*
#             FROM "STG_VIEW_TERMINAL_HIST" t1
#             JOIN "STG_TERMINALS" t2
#             ON t1.terminal_id = t2.terminal_id
#             AND (
#                 t1.terminal_type <> t2.terminal_type
#                 OR t1.terminal_city <> t2.terminal_city
#                 OR t1.terminal_address <> t2.terminal_address
#             )
#         """)
#     conn.commit()
#
#
# # Функция добавления данных в таблицу "DWH_DIM_TERMINALS_HIST"
# def update_terminal_hist():
#     # Добавление новых данных
#     cursor.execute("""
#            INSERT INTO "DWH_DIM_TERMINALS_HIST" (
#                terminal_id, terminal_type, terminal_city, terminal_address
#            )
#            SELECT terminal_id, terminal_type, terminal_city, terminal_address
#                from "STG_TMP_NEW_ROWS"
#            """)
#     # Обновление данных, которые изменились с указанием времени на текущее минус 1 секунда
#     cursor.execute("""
#         UPDATE "DWH_DIM_TERMINALS_HIST"
#         SET effective_to = date_trunc('second', NOW() - INTERVAL '1 second')
#         WHERE terminal_id in(SELECT terminal_id from "STG_TMP_UPDATED_ROWS")
#         AND effective_to = TO_TIMESTAMP('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
#         """)
#     # Добавление обновленных данных
#     cursor.execute("""
#         INSERT INTO "DWH_DIM_TERMINALS_HIST" (
#             terminal_id, terminal_type, terminal_city, terminal_address
#         )
#         SELECT terminal_id, terminal_type, terminal_city, terminal_address
#             from "STG_TMP_UPDATED_ROWS"
#         """)
#     # Обновление данных, которые были удалены с указанием времени на текущее минус 1 секунда, с установкой флага в значение 1
#     cursor.execute("""
#             UPDATE "DWH_DIM_TERMINALS_HIST"
#             SET effective_to = DATE_TRUNC('second', NOW() - INTERVAL '1 second'), deleted_flg = 1
#             WHERE terminal_id in(SELECT terminal_id FROM "STG_TMP_DELETED_ROWS")
#             and effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
#         """)
#     # Добавление удаленных данных
#     cursor.execute("""
#           INSERT INTO "DWH_DIM_TERMINALS_HIST" (
#               terminal_id, terminal_type, terminal_city, terminal_address
#           )
#           SELECT terminal_id, terminal_type, terminal_city, terminal_address
#               from "STG_TMP_DELETED_ROWS"
#           """)
#     conn.commit()


# Создание таблицы витрины с отчетом
# def create_rep_fraud():
#     cursor.execute("""
#         CREATE TABLE if not exists "REP_FRAUD" (
#             event_dt TIMESTAMP,
#             passport VARCHAR(128),
#             fio VARCHAR(128),
#             phone VARCHAR(128),
#             event_type VARCHAR(128),
#             report_dt TIMESTAMP default CURRENT_TIMESTAMP
#             )
#         """)
#     conn.commit()


# # Создание представления для получения данных о просроченных паспортах
# def check_passport_valid_to():
#     cursor.execute("""
#         CREATE VIEW "STG_VIEW_PASSPORT_VALID_TO" AS
#         SELECT
#                 t1.trans_date AS event_dt,
#                 t4.passport_num AS passport,
#                 CONCAT_WS(' ', t4.last_name, t4.first_name, t4.patronymic) AS fio,
#                 phone,
#                 NOW() AS report_dt
#
#         FROM  "DWH_FACT_TRANSACTIONS" t1
#         JOIN  "DWH_DIM_CARDS" t2 ON t1.card_num = t2.card_num
#         JOIN  "DWH_DIM_ACCOUNTS" t3 ON t2.account_num = t3.account_num
#         JOIN  "DWH_DIM_CLIENTS" t4 ON t3.client = t4.client_id
#         WHERE t4.passport_valid_to::DATE < t1.trans_date::DATE AND t1.oper_result = 'SUCCESS'
#         """)
#     conn.commit()
#
#
#
# # Функция создания представления для получения данных о недействующих договорах
# def check_account_valid_to():
#     cursor.execute("""
#         CREATE VIEW "STG_VIEW_ACCOUNT_VALID_TO" AS
#         SELECT
#                 t4.trans_date AS event_dt,
#                 t1.passport_num AS passport,
#                 CONCAT_WS(' ', t1.last_name, t1.first_name, t1.patronymic) as fio,
#                 t1.phone as phone,
#                 NOW() as report_dt
#
#         FROM "DWH_DIM_CLIENTS" t1
#         JOIN "DWH_DIM_ACCOUNTS" t2 ON t1.client_id = t2.client
#         JOIN "DWH_DIM_CARDS" t3 ON t2.account_num = t3.account_num
#         JOIN "DWH_FACT_TRANSACTIONS" t4 ON t3.card_num = t4.card_num
#         WHERE t4.trans_date::DATE > t2.valid_to::DATE AND t4.oper_result = 'SUCCESS'
#     """)
#     conn.commit()
#
# # Функция создания представления для получения данных о паспортах из черного списка
# def check_passport_in_blacklist():
#     cursor.execute("""
#         CREATE VIEW "STG_VIEW_PASSPORT_BLOCKED" AS
#         SELECT
#                 t4.trans_date AS event_dt,
#                 t1.passport_num AS passport,
#                 CONCAT_WS(' ', t1.last_name, t1.first_name, t1.patronymic) AS fio,
#                 t1.phone AS phone,
#                 NOW() AS report_dt
#
#         FROM "DWH_DIM_CLIENTS" t1
#         JOIN "DWH_DIM_ACCOUNTS" t2 ON t1.client_id = t2.client
#         JOIN "DWH_DIM_CARDS" t3 ON t2.account_num = t3.account_num
#         JOIN "DWH_FACT_TRANSACTIONS" t4 ON t3.card_num = t4.card_num
#         JOIN "DWH_FACT_PASSPORT_BLACKLIST" t5 ON t1.passport_num = t5.passport_num
#         AND t5.entry_dt < t4.trans_date AND t4.oper_result = 'SUCCESS'
#     """)
#     conn.commit()
#
#
# # Функция создания представлений для получения данных
# def check_different_city():
#     # Данные о картах и количестве разных городов больше 1, где были совершены операции
#     cursor.execute("""
#             CREATE VIEW "STG_VIEW_FOR_DIFFERENT_CITY"  AS
#             SELECT
#     		    card_num,
#     		    COUNT(DISTINCT t2.terminal_city) AS cnt_city
#             FROM "DWH_FACT_TRANSACTIONS" t1
#             LEFT JOIN "DWH_DIM_TERMINALS_HIST" t2 ON t1.terminal = t2.terminal_id
#             GROUP BY t1.card_num
#             HAVING COUNT(DISTINCT t2.terminal_city) > 1
#         """)
#     # Данные о клиентах, которые совершали операции в разных городах в течении часа
#     cursor.execute("""
#         CREATE VIEW "STG_VIEW_DIFFERENT_CITY" AS
#         SELECT
# 		    t1.trans_date AS event_dt,
# 		    t4.passport_num AS passport,
# 		    CONCAT(t4.last_name, ' ', t4.first_name, ' ', t4.patronymic) AS fio,
# 		    t4.phone,
# 		    NOW() AS report_dt
#         FROM (
# 	        SELECT
# 	            card_num,
# 	            MIN(current_city_date) AS trans_date
#             FROM (
# 	            SELECT
# 	                card_num,
# 	                terminal_city AS current_city,
# 	                trans_date AS current_city_date,
# 	                LAG(terminal_city) OVER(PARTITION BY card_num ORDER BY trans_date) AS last_city,
# 	                LAG(trans_date) OVER(PARTITION BY card_num ORDER BY trans_date) AS last_city_date
#                 FROM (
# 	                SELECT
# 	                    t1.card_num,
# 	                    t1.trans_date,
# 	                    t3.terminal_city
#                     FROM "DWH_FACT_TRANSACTIONS" t1
#                     JOIN "STG_VIEW_FOR_DIFFERENT_CITY" t2 ON t1.card_num = t2.card_num
#                     LEFT JOIN "DWH_DIM_TERMINALS_HIST" t3 ON t1.terminal = t3.terminal_id
#                     )
#                     )
#                     WHERE (current_city_date - last_city_date) < INTERVAL '1 hour' AND last_city != current_city
#                     GROUP BY card_num) t1
#                     LEFT JOIN "DWH_DIM_CARDS" t2 ON t1.card_num = t2.card_num
#                     LEFT JOIN "DWH_DIM_ACCOUNTS" t3 ON t2.account_num = t3.account_num
#                     LEFT JOIN "DWH_DIM_CLIENTS" t4 ON t3.client = t4.client_id
#                 """)
#     conn.commit()



# Добавление данных в витрину
# def update_rep_fraud():
#     cursor.execute("""
#         MERGE INTO "REP_FRAUD" AS target
#         USING "STG_VIEW_ACCOUNT_VALID_TO" AS source ON target.event_dt = source.event_dt
#         AND target.passport = source.passport
#         AND target.fio = source.fio
#         AND target.phone = source.phone
#         AND target.event_type = 'account expired'
#         WHEN NOT MATCHED THEN INSERT(event_dt, passport, fio, phone, event_type, report_dt)
#         VALUES(source.event_dt, source.passport, source.fio, source.phone, 'account expired', source.report_dt)
#         """)
#     cursor.execute("""
#         MERGE INTO "REP_FRAUD" AS target
#         USING "STG_VIEW_PASSPORT_VALID_TO" AS source ON target.event_dt = source.event_dt
#         AND target.passport = source.passport
#         AND target.fio = source.fio
#         AND target.phone = source.phone
#         AND target.event_type = 'passport expired'
#         WHEN NOT MATCHED THEN INSERT(event_dt, passport, fio, phone, event_type, report_dt)
#         VALUES(source.event_dt, source.passport, source.fio, source.phone, 'passport expired', source.report_dt)
#         """)
#     cursor.execute("""
#         MERGE INTO "REP_FRAUD" AS target
#         USING "STG_VIEW_PASSPORT_BLOCKED" AS source ON target.event_dt = source.event_dt
#         AND target.passport = source.passport
#         AND target.fio = source.fio
#         AND target.phone = source.phone
#         AND target.event_type = 'passport blocked'
#         WHEN NOT MATCHED THEN INSERT(event_dt, passport, fio, phone, event_type, report_dt)
#         VALUES(source.event_dt, source.passport, source.fio, source.phone, 'passport blocked', source.report_dt)
#         """)
#     cursor.execute("""
#         MERGE INTO "REP_FRAUD" AS target
#         USING "STG_VIEW_DIFFERENT_CITY" AS source ON target.event_dt = source.event_dt
#         AND target.passport = source.passport
#         AND target.fio = source.fio
#         AND target.phone = source.phone
#         AND target.event_type = 'different city'
#         WHEN NOT MATCHED THEN INSERT(event_dt, passport, fio, phone, event_type, report_dt)
#         VALUES(source.event_dt, source.passport, source.fio, source.phone, 'different city', source.report_dt)
#         """)
#     conn.commit()

#
# # Удаление представлений перед новым запуском
# def remove_view():
#     cursor.execute("""DROP VIEW "STG_VIEW_PASSPORT_VALID_TO" """)
#     cursor.execute("""DROP VIEW "STG_VIEW_ACCOUNT_VALID_TO" """)
#     cursor.execute("""DROP VIEW "STG_VIEW_PASSPORT_BLOCKED" """)
#     cursor.execute("""DROP VIEW "STG_VIEW_DIFFERENT_CITY" """)
#     cursor.execute("""DROP VIEW "STG_VIEW_FOR_DIFFERENT_CITY" """)
#     conn.commit()
#
#
#
# # Функция удаления таблиц перед новым запуском программы
# def remove_tables():
#     cursor.execute("""DROP TABLE IF EXISTS "DWH_DIM_CLIENTS" """)
#     cursor.execute("""DROP TABLE IF EXISTS "DWH_DIM_ACCOUNTS" """)
#     cursor.execute("""DROP TABLE IF EXISTS "DWH_DIM_CARDS" """)
#     cursor.execute("""DROP TABLE IF EXISTS "DWH_FACT_PASSPORT_BLACKLIST" """)
#     conn.commit()
#
# def remove_tmp_tables():
#     cursor.execute("""DROP TABLE IF EXISTS "STG_TMP_NEW_ROWS" """)
#     cursor.execute("""DROP TABLE IF EXISTS "STG_TMP_DELETED_ROWS" """)
#     cursor.execute("""DROP TABLE IF EXISTS "STG_TMP_UPDATED_ROWS" """)
#     conn.commit()




# remove_tmp_tables()
# remove_tables()
# sql_load('ddl_dml.sql', conf, 'bank')
# csv2sql("data/transactions_01032021.txt", conf, 'STG_TRANSACTIONS', 'bank')
# excel2sql("data/terminals_01032021.xlsx", conf, 'STG_TERMINALS', 'bank', )
# excel2sql("data/passport_blacklist_01032021.xlsx", conf, 'STG_PASSPORT_BLACKLIST', 'bank')
# create_dwh_fact()
# update_dwh_fact()
# create_dwh_dim_tables()
# update_dwh_dim_tables()
# create_function()
# create_trigger()
# create_terminal_hist()
# create_terminals_new_rows()
# create_deleted_terminals_rows()
# create_update_terminals_rows()
# update_terminal_hist()
# create_rep_fraud()
# check_passport_valid_to()
# check_account_valid_to()
# check_passport_in_blacklist()
# check_different_city()
# update_rep_fraud()
# remove_view()


