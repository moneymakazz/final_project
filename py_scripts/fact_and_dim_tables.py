from .download_data import cursor, conn

# Функция создания таблиц фактов
def create_dwh_fact():
    # Создание таблицы фактов транзакций
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS "DWH_FACT_TRANSACTIONS" (
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
            CREATE TABLE IF NOT EXISTS "DWH_FACT_PASSPORT_BLACKLIST" (
                passport_num VARCHAR(128),
                entry_dt DATE
            )
            """)
    conn.commit()


# Функция добавления данных в таблицы фактов
def update_dwh_fact():
    # Добавление данных из стейдж таблицы в таблицу фактов транзакций
    cursor.execute("""
            INSERT INTO "DWH_FACT_TRANSACTIONS" (
                trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal
            )
            SELECT
                transaction_id, transaction_date::TIMESTAMP, card_num, oper_type,
                CAST(REPLACE(amount, ',', '.') AS NUMERIC), oper_result, terminal
                FROM "STG_TRANSACTIONS"
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


# Функция создания таблиц типа SCD1
def create_dwh_dim_tables():
    # Создание таблицы клиентов
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS "DWH_DIM_CLIENTS" (
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
            CREATE TABLE IF NOT EXISTS "DWH_DIM_ACCOUNTS" (
                account_num VARCHAR(128),
                valid_to DATE,
                client VARCHAR(128),
                create_dt DATE default current_date,
                update_dt DATE default current_date
            )
            """)
   # Создание таблицы с картами
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS "DWH_DIM_CARDS" (
                card_num VARCHAR(128),
                account_num VARCHAR(128),
                create_dt DATE default current_date,
                update_dt DATE default current_date
            )
            """)
    conn.commit()


# Функция добавления данных в таблицы типа SCD1
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
    cursor.execute('DROP TABLE IF EXISTS cards')
    cursor.execute('DROP TABLE IF EXISTS accounts')
    cursor.execute('DROP TABLE IF EXISTS clients')
    conn.commit()