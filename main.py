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















