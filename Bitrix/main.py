from dbUtils import connection_to_db, execute, close_connections
from extract_data import get_from_api, load_delta, tasks_history
from warningsUtils import ignore_warnings
from jsonUtils import read_json
from os import chdir, makedirs
from os.path import exists
from dfUtils import *
from time import time
import extract_data

# DICTIONARIES=["users","status_dic","departments_dic"]
DICTIONARIES = ["dim_status", "dim_deals_categories", "dim_custom_fields"]
EXPORT_DIRECTORY = ''

# DICTIONARIES = ["deals_categories"]
# DICTIONARIES = ["custom_fields"]

def main(project_id, token, exception_tables, cnxn, cursor):
    start = time()
    tables_json = read_json("template.json")

    for table, desc in tables_json.items():
        if table not in exception_tables:
            desc["token"] = token
            print(f"Loading table {table}")
            if table in DICTIONARIES:
                if table != "custom_fields":
                    execute(f"DELETE FROM {desc['table_name']} WHERE project_id = '{project_id}';", cnxn, cursor)
                add_param = {}
                if table == "dim_deals_categories":
                    add_param.update({"entityTypeId": 2})
                get_from_api(token, desc["api_url"], desc["table_name"], desc["columns"], add_param, cnxn, cursor,
                             project_id=project_id)
                continue
            if table == "tasks_history":
                # execute(f'TRUNCATE TABLE {desc["table_name"]};',conn,cursor)
                tasks_history(desc, cnxn, cursor)
                continue
            load_delta(desc, cnxn, cursor, project_id)
    print("Refreshing all tables")
    # execute("EXEC refresh_crm;",conn,cursor)

    end = time()
    print(f"Execution time {round((end - start) / 60)} minutes")


if __name__ == '__main__':
    ignore_warnings()
    chdir_path = __file__.rsplit('\\', 1)[0]
    chdir(chdir_path)
    cnxn, cursor = connection_to_db()
    with open("ddl.txt") as ddl_file:
        ddls = ddl_file.read()

    with open("procedures.sql") as proc:
        procedures = proc.read()

    tables = [table for table in read_json('template.json')]
    cursor.executescript(ddls)
    df = read_excel("bitrix_accs.xlsx")
    for _, row_values in df.iterrows():
        print(f'PROJECT NAME: {row_values["project_name"]}')
        extract_data.MAIN_URL = row_values["url"]
        extract_data.ID = row_values["account_id"]
        extract_data.START_DATE = row_values["start_date"]
        exception_tables = row_values["exception_tables"].replace(' ', '').split(',') if str(row_values["exception_tables"]) != 'nan' else []

        main(row_values["project_id"], row_values["token"], exception_tables, cnxn, cursor)
        path = f'{chdir_path}\\{row_values["project_name"]}' if str(row_values["export_directory"]) == 'nan' else f'{row_values["export_directory"]}\\{row_values["project_name"]}'
        if not exists(path):
            makedirs(path)
        cursor.executescript(procedures)
        export_data_to_csv(tables, row_values["project_id"], path, cnxn)

    close_connections(cursor, cnxn)