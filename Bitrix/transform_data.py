import json
from datetime import datetime
from dbUtils import load_to_db, delete_dim

DIM_TABLES = {"dim_brand": ["UF_CRM_1612504156", "UF_CRM_1687705618"], "dim_mark": ["UF_CRM_1611565554",
                                                                                    "UF_CRM_1592391634",
                                                                                    "UF_CRM_1680491934",
                                                                                    "UF_CRM_1611036044",
                                                                                    "UF_CRM_1686814065"],
              "dim_complect": ["UF_CRM_1592392009",
                               "UF_CRM_1611036159",
                               "UF_CRM_1611565674",
                               "UF_CRM_1680492775", "UF_CRM_1687514794"], "dim_kpp": ["UF_CRM_1611221720"] }


def process_dims(data, source_id):
    with open("dims_fields.json") as dim_file:
        DIM_TABLES=json.load(dim_file)
    for table in DIM_TABLES:
        delete_dim(table, source_id)
        process_keys = DIM_TABLES[table]
        process_values = []
        for key in process_keys:
            if key in data and "items" in data[key]:
                to_process = [{"uf_id": key, **row} for row in data[key]["items"]]
                process_values.extend(to_process)
        insert_data = []
        for row in process_values:
            insert_data.append((*row.values(), source_id))
        load_to_db(insert_data, table, ["uf_id", "id", "name", "source_id"])


def process_task_history(data, **kwargs):
    processed = []
    for change in data:
        transformed_date = datetime.strptime(change["createdDate"], "%d.%m.%Y %H:%M:%S")
        new_row = (kwargs["task_id"], transformed_date, change["field"], change["value"]["from"], change["value"]["to"])
        processed.append(new_row)
    return processed


def process_tasks(data, columns):
    dic_data = []
    for row in data:
        if "ufCrmTask" not in row:
            continue
        if not row['ufCrmTask']:
            continue
        dic_row = {key: None for key in columns}
        for key in row:
            if key.lower() in columns:
                dic_row[key.lower()] = row[key]
        tasks_id = [*dic_row["ufcrmtask"]]
        for crm_id in tasks_id:
            dic_row["ufcrmtask"] = crm_id
            dic_data.append(tuple(dic_row.values()))
    return dic_data


def process_fields(data):
    processed = []
    for row in data:
        for key, value in row.items():
            if value:
                processed.append((row["ID"], key, f"{value}", row["DATE_MODIFY"], row["domain"], row["user_name"]))

    return processed


def subset(data, columns):
    dic_data = []
    for row in data:
        dic_row = {key: None for key in columns}
        for key in row:
            if key.lower() in columns:
                dic_row[key.lower()] = row[key]
        if "uf_department" in columns and dic_row["uf_department"]:
            dic_row["uf_department"] = ','.join([f"{dep_id}" for dep_id in dic_row["uf_department"]])
        dic_data.append(tuple(dic_row.values()))
    return dic_data


def process_dates(data):
    for elem in data:
        for key in elem:
            if ('date' in key.lower() or 'time' in key.lower() or "deadline" in key.lower()) and isinstance(elem[key], str):
                elem[key] = elem[key][:19]
    return data


def process_for_table(data, table_name, columns, **kwargs):
    to_process = data["result"]

    if table_name in ("deals_stage_history_tmp", "leads_stage_history_tmp"):
        to_process = to_process["items"]
    if table_name == "dim_deals_categories":
        to_process = to_process["categories"]

    # if table_name == "bitrix.custom_fields":
    #     process_dims(to_process, kwargs["source_id"])
    #     return
    to_process = [{**row, **kwargs} for row in to_process]
    if table_name in ("deals_custom_fields_tmp", "leads_custom_fields_tmp"):
        processed_data = process_fields(process_dates(to_process))
    elif table_name == "tasks_tmp":
        processed_data = process_tasks(process_dates(to_process), columns)

    elif table_name == "tasks_history_tmp":
        processed_data = process_task_history(to_process, **kwargs)

    else:
        processed_data = subset(process_dates(to_process), columns)

    return processed_data