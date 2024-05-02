from sqlite3 import connect

DB_PATH = f'bitrix.db'


def connection_to_db(cursor=True):
    cnxn = connect(DB_PATH)
    if cursor:
        return cnxn, cnxn.cursor()
    return cnxn


def execute(sql, cnxn, cursor):
    cursor.execute(sql)
    cnxn.commit()


def insert_data(sql, data, cnxn, cursor):
    cursor.executemany(sql, data)
    cnxn.commit()


def delete_dim(table, project_id, cnxn, cursor):
    sql = f"DELETE FROM {table} WHERE project_id='{project_id}'"
    execute(sql, cnxn, cursor)


def load_to_db(data, table, columns, cnxn, cursor):
    if data:
        values = ','.join(['?' for _ in range(len(columns))])
        sql = f"INSERT INTO {table}({','.join(columns)}) VALUES({values})"
        insert_data(sql, data, cnxn, cursor)


def close_connections(*connections):
    for conn in connections:
        conn.close()