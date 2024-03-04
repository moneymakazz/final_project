from .download_data import cursor, conn

# Функция создания хранимой функции для обновления значения update_dt, в случае изменения данных в таблицах типа SCD1
def create_function():
    cursor.execute("""
            CREATE OR REPLACE FUNCTION update_trigger_function()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.update_dt = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            DROP TRIGGER IF EXISTS trg_update_dwh_dim_clients on "DWH_DIM_CLIENTS";
            DROP TRIGGER IF EXISTS trg_update_dwh_dim_clients on "DWH_DIM_ACCOUNTS";
            DROP TRIGGER IF EXISTS trg_update_dwh_dim_clients on "DWH_DIM_CARDS";
            """)
    conn.commit()


# Функция создания триггера, который запускает функцию update_trigger_function
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
