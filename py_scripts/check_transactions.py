from .download_data import cursor, conn

# Создание представления для получения данных о просроченных паспортах
def check_passport_valid_to():
    cursor.execute("""
        CREATE VIEW "STG_VIEW_PASSPORT_VALID_TO" AS
        SELECT 
                t1.trans_date AS event_dt,
                t4.passport_num AS passport,
                CONCAT_WS(' ', t4.last_name, t4.first_name, t4.patronymic) AS fio,
                phone,
                NOW() AS report_dt

        FROM  "DWH_FACT_TRANSACTIONS" t1
        JOIN  "DWH_DIM_CARDS" t2 ON t1.card_num = t2.card_num
        JOIN  "DWH_DIM_ACCOUNTS" t3 ON t2.account_num = t3.account_num
        JOIN  "DWH_DIM_CLIENTS" t4 ON t3.client = t4.client_id
        WHERE t4.passport_valid_to::DATE < t1.trans_date::DATE AND t1.oper_result = 'SUCCESS'
        """)
    conn.commit()


# Функция создания представления для получения данных о недействующих договорах
def check_account_valid_to():
    cursor.execute(""" 
        CREATE VIEW "STG_VIEW_ACCOUNT_VALID_TO" AS
        SELECT
                t4.trans_date AS event_dt,
                t1.passport_num AS passport,
                CONCAT_WS(' ', t1.last_name, t1.first_name, t1.patronymic) as fio,
                t1.phone as phone,
                NOW() as report_dt

        FROM "DWH_DIM_CLIENTS" t1
        JOIN "DWH_DIM_ACCOUNTS" t2 ON t1.client_id = t2.client
        JOIN "DWH_DIM_CARDS" t3 ON t2.account_num = t3.account_num
        JOIN "DWH_FACT_TRANSACTIONS" t4 ON t3.card_num = t4.card_num 
        WHERE t4.trans_date::DATE > t2.valid_to::DATE AND t4.oper_result = 'SUCCESS' 
    """)
    conn.commit()


# Функция создания представления для получения данных о паспортах из черного списка
def check_passport_in_blacklist():
    cursor.execute("""
        CREATE VIEW "STG_VIEW_PASSPORT_BLOCKED" AS
        SELECT
                t4.trans_date AS event_dt,
                t1.passport_num AS passport,
                CONCAT_WS(' ', t1.last_name, t1.first_name, t1.patronymic) AS fio,
                t1.phone AS phone,
                NOW() AS report_dt

        FROM "DWH_DIM_CLIENTS" t1
        JOIN "DWH_DIM_ACCOUNTS" t2 ON t1.client_id = t2.client
        JOIN "DWH_DIM_CARDS" t3 ON t2.account_num = t3.account_num
        JOIN "DWH_FACT_TRANSACTIONS" t4 ON t3.card_num = t4.card_num
        JOIN "DWH_FACT_PASSPORT_BLACKLIST" t5 ON t1.passport_num = t5.passport_num
        AND t5.entry_dt < t4.trans_date AND t4.oper_result = 'SUCCESS'
    """)
    conn.commit()


# Функция создания представлений для получения данных
def check_different_city():
    # Данные о картах и количестве разных городов больше 1, где были совершены операции
    cursor.execute("""
            CREATE VIEW "STG_VIEW_FOR_DIFFERENT_CITY"  AS
            SELECT
    		    card_num,
    		    COUNT(DISTINCT t2.terminal_city) AS cnt_city
            FROM "DWH_FACT_TRANSACTIONS" t1
            LEFT JOIN "DWH_DIM_TERMINALS_HIST" t2 ON t1.terminal = t2.terminal_id
            GROUP BY t1.card_num
            HAVING COUNT(DISTINCT t2.terminal_city) > 1
        """)
    # Данные о клиентах, которые совершали операции в разных городах в течении часа
    cursor.execute("""
        CREATE VIEW "STG_VIEW_DIFFERENT_CITY" AS
        SELECT
		    t1.trans_date AS event_dt,
		    t4.passport_num AS passport,
		    CONCAT(t4.last_name, ' ', t4.first_name, ' ', t4.patronymic) AS fio,
		    t4.phone,
		    NOW() AS report_dt
        FROM (
	        SELECT
	            card_num,
	            MIN(current_city_date) AS trans_date
            FROM (
	            SELECT
	                card_num,
	                terminal_city AS current_city,
	                trans_date AS current_city_date,
	                LAG(terminal_city) OVER(PARTITION BY card_num ORDER BY trans_date) AS last_city,
	                LAG(trans_date) OVER(PARTITION BY card_num ORDER BY trans_date) AS last_city_date
                FROM (
	                SELECT
	                    t1.card_num,
	                    t1.trans_date,
	                    t3.terminal_city
                    FROM "DWH_FACT_TRANSACTIONS" t1
                    JOIN "STG_VIEW_FOR_DIFFERENT_CITY" t2 ON t1.card_num = t2.card_num
                    LEFT JOIN "DWH_DIM_TERMINALS_HIST" t3 ON t1.terminal = t3.terminal_id
                    )
                    )
                    WHERE (current_city_date - last_city_date) < INTERVAL '1 hour' AND last_city != current_city
                    GROUP BY card_num) t1
                    LEFT JOIN "DWH_DIM_CARDS" t2 ON t1.card_num = t2.card_num
                    LEFT JOIN "DWH_DIM_ACCOUNTS" t3 ON t2.account_num = t3.account_num
                    LEFT JOIN "DWH_DIM_CLIENTS" t4 ON t3.client = t4.client_id
                """)
    conn.commit()