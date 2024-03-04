from .download_data import cursor, conn

def create_terminal_hist():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS "DWH_DIM_TERMINALS_HIST" (
            id SERIAL PRIMARY KEY,
            terminal_id VARCHAR(128),
            terminal_type VARCHAR(128),
            terminal_city VARCHAR(128),
            terminal_address VARCHAR(128),
            effective_from TIMESTAMP default CURRENT_TIMESTAMP,
            effective_to TIMESTAMP default (to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')),
            deleted_flg INTEGER default 0
            )
        """)

    cursor.execute("""DROP VIEW IF EXISTS "STG_VIEW_TERMINAL_HIST" """)

    cursor.execute("""
        CREATE VIEW "STG_VIEW_TERMINAL_HIST" AS
        SELECT
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address
        FROM "DWH_DIM_TERMINALS_HIST"
        WHERE deleted_flg = 0 
        and CURRENT_TIMESTAMP BETWEEN effective_from and effective_to;
    """)

# Функция создания таблицы с новыми данными о терминалах
def create_terminals_new_rows():
    cursor.execute("""
        CREATE TABLE "STG_TMP_NEW_ROWS" AS
            SELECT
                t1.*
            FROM "STG_TERMINALS" t1
            LEFT JOIN "STG_VIEW_TERMINAL_HIST" t2 ON t1.terminal_id = t2.terminal_id
            WHERE t2.terminal_id IS NULL
        """)
    conn.commit()

# Функция создания таблицы с удаленными данными о терминалах
def create_deleted_terminals_rows():
    cursor.execute("""
        CREATE TABLE "STG_TMP_DELETED_ROWS" AS
            SELECT
                t1.*
            FROM "STG_VIEW_TERMINAL_HIST" t1
            LEFT JOIN "STG_TERMINALS" t2 
            ON t1.terminal_id = t2.terminal_id
            WHERE t2.terminal_id IS NULL
        """)
    conn.commit()


# Функция создания таблицы с измененными данными о терминалах
def create_update_terminals_rows():
    cursor.execute("""
        CREATE TABLE "STG_TMP_UPDATED_ROWS" AS
            SELECT t2.*
            FROM "STG_VIEW_TERMINAL_HIST" t1
            JOIN "STG_TERMINALS" t2 
            ON t1.terminal_id = t2.terminal_id
            AND (
                t1.terminal_type <> t2.terminal_type
                OR t1.terminal_city <> t2.terminal_city
                OR t1.terminal_address <> t2.terminal_address
            )
        """)
    conn.commit()


# Функция добавления данных в таблицу "DWH_DIM_TERMINALS_HIST"
def update_terminal_hist():
    # Добавление новых данных
    cursor.execute("""
           INSERT INTO "DWH_DIM_TERMINALS_HIST" (
               terminal_id, terminal_type, terminal_city, terminal_address
           )
           SELECT terminal_id, terminal_type, terminal_city, terminal_address
               from "STG_TMP_NEW_ROWS"
           """)
    # Обновление данных, которые изменились с указанием времени на текущее минус 1 секунда
    cursor.execute("""
        UPDATE "DWH_DIM_TERMINALS_HIST"
        SET effective_to = date_trunc('second', NOW() - INTERVAL '1 second')
        WHERE terminal_id in(SELECT terminal_id from "STG_TMP_UPDATED_ROWS")
        AND effective_to = TO_TIMESTAMP('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
        """)
    # Добавление обновленных данных
    cursor.execute("""
        INSERT INTO "DWH_DIM_TERMINALS_HIST" (
            terminal_id, terminal_type, terminal_city, terminal_address
        )
        SELECT terminal_id, terminal_type, terminal_city, terminal_address
            from "STG_TMP_UPDATED_ROWS"
        """)
    # Обновление данных, которые были удалены с указанием времени на текущее минус 1 секунда, с установкой флага в значение 1
    cursor.execute("""
            UPDATE "DWH_DIM_TERMINALS_HIST"
            SET effective_to = DATE_TRUNC('second', NOW() - INTERVAL '1 second'), deleted_flg = 1
            WHERE terminal_id in(SELECT terminal_id FROM "STG_TMP_DELETED_ROWS")
            and effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
        """)
    # Добавление удаленных данных
    cursor.execute("""
          INSERT INTO "DWH_DIM_TERMINALS_HIST" (
              terminal_id, terminal_type, terminal_city, terminal_address
          )
          SELECT terminal_id, terminal_type, terminal_city, terminal_address
              from "STG_TMP_DELETED_ROWS"
          """)
    conn.commit()