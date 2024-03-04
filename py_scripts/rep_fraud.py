from .download_data import cursor, conn

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

def update_rep_fraud():
    cursor.execute("""
        MERGE INTO "REP_FRAUD" AS target
        USING "STG_VIEW_ACCOUNT_VALID_TO" AS source ON target.event_dt = source.event_dt
        AND target.passport = source.passport
        AND target.fio = source.fio
        AND target.phone = source.phone
        AND target.event_type = 'account expired'
        WHEN NOT MATCHED THEN INSERT(event_dt, passport, fio, phone, event_type, report_dt)
        VALUES(source.event_dt, source.passport, source.fio, source.phone, 'account expired', source.report_dt)
        """)
    cursor.execute("""
        MERGE INTO "REP_FRAUD" AS target
        USING "STG_VIEW_PASSPORT_VALID_TO" AS source ON target.event_dt = source.event_dt
        AND target.passport = source.passport
        AND target.fio = source.fio
        AND target.phone = source.phone
        AND target.event_type = 'passport expired'
        WHEN NOT MATCHED THEN INSERT(event_dt, passport, fio, phone, event_type, report_dt)
        VALUES(source.event_dt, source.passport, source.fio, source.phone, 'passport expired', source.report_dt)
        """)
    cursor.execute("""
        MERGE INTO "REP_FRAUD" AS target
        USING "STG_VIEW_PASSPORT_BLOCKED" AS source ON target.event_dt = source.event_dt
        AND target.passport = source.passport
        AND target.fio = source.fio
        AND target.phone = source.phone
        AND target.event_type = 'passport blocked'
        WHEN NOT MATCHED THEN INSERT(event_dt, passport, fio, phone, event_type, report_dt)
        VALUES(source.event_dt, source.passport, source.fio, source.phone, 'passport blocked', source.report_dt)
        """)
    cursor.execute("""
        MERGE INTO "REP_FRAUD" AS target
        USING "STG_VIEW_DIFFERENT_CITY" AS source ON target.event_dt = source.event_dt
        AND target.passport = source.passport
        AND target.fio = source.fio
        AND target.phone = source.phone
        AND target.event_type = 'different city'
        WHEN NOT MATCHED THEN INSERT(event_dt, passport, fio, phone, event_type, report_dt)
        VALUES(source.event_dt, source.passport, source.fio, source.phone, 'different city', source.report_dt)
        """)
    conn.commit()