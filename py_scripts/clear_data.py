from .download_data import cursor, conn

# Удаление представлений перед новым запуском
def remove_view():
    cursor.execute("""DROP VIEW "STG_VIEW_PASSPORT_VALID_TO" """)
    cursor.execute("""DROP VIEW "STG_VIEW_ACCOUNT_VALID_TO" """)
    cursor.execute("""DROP VIEW "STG_VIEW_PASSPORT_BLOCKED" """)
    cursor.execute("""DROP VIEW "STG_VIEW_DIFFERENT_CITY" """)
    cursor.execute("""DROP VIEW "STG_VIEW_FOR_DIFFERENT_CITY" """)
    conn.commit()



# Функция удаления таблиц перед новым запуском программы
def remove_tables():
    cursor.execute("""DROP TABLE IF EXISTS "DWH_DIM_CLIENTS" """)
    cursor.execute("""DROP TABLE IF EXISTS "DWH_DIM_ACCOUNTS" """)
    cursor.execute("""DROP TABLE IF EXISTS "DWH_DIM_CARDS" """)
    cursor.execute("""DROP TABLE IF EXISTS "DWH_FACT_PASSPORT_BLACKLIST" """)
    conn.commit()

def remove_tmp_tables():
    cursor.execute("""DROP TABLE IF EXISTS "STG_TMP_NEW_ROWS" """)
    cursor.execute("""DROP TABLE IF EXISTS "STG_TMP_DELETED_ROWS" """)
    cursor.execute("""DROP TABLE IF EXISTS "STG_TMP_UPDATED_ROWS" """)
    conn.commit()