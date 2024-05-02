from pandas import read_excel, read_sql


def export_data_to_csv(tables, project_id, path, cnxn):
    for table_name in tables:
        sql = f'SELECT * FROM {table_name} WHERE project_id={project_id}'
        df = read_sql(sql=sql, con=cnxn)
        if not df.empty:
            df.to_csv(path_or_buf=f'{path}\\{table_name}.csv', index=False, sep=';')