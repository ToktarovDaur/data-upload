from transform_data import process_dates, subset, process_for_table
from dbUtils import insert_data

def upload_data(data, table, columns, cnxn, cursor, **kwargs):
    processed_data = process_for_table(data, table, columns, **kwargs)
    if not processed_data:
        return
    values = ','.join(['?' for _ in columns])
    sql_insert = f"INSERT INTO {table}({','.join(columns)}) VALUES({values})"
    insert_data(sql_insert, processed_data, cnxn, cursor)