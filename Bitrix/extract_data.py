from datetime import datetime, timedelta
from load_data import upload_data
import requests
import time

MAIN_URL = u'https://bitrix.example.kz/rest'
ID = "12345"
REQ_ATT_LIMIT = 10
START_DATE = datetime(2023, 1, 1)
# DEALS_COLUMNS=[
#       "ID",
#       "STAGE_ID",
#       "OPPORTUNITY",
#       "LEAD_ID",
#       "DATE_CREATE",
#       "DATE_MODIFY",
#       "CATEGORY_ID",
#       "UTM_SOURCE",
#       "UF_CRM_1612504156",
#       "UF_CRM_1687705618",
#       "UF_CRM_1611565554",
#       "UF_CRM_1592391634",
#       "UF_CRM_1680491934",
#       "UF_CRM_1611036044",
#       "UF_CRM_1686814065",
#       "UF_CRM_1592392009",
#       "UF_CRM_1611036159",
#       "UF_CRM_1611565674",
#       "UF_CRM_1680492775",
#       "UF_CRM_1687514794",
#       "UF_CRM_1611221720",
#       "UF_CRM_1586841138",
#       "UF_CRM_1686906300",
#       "UF_CRM_1614075735",
#       "UF_CRM_1690542962",
#       "UF_CRM_1605276676",
#       "UF_CRM_1604656131",
#       "UF_CRM_1686750597",
#       "UF_CRM_1690870330",
#       "UF_CRM_1603233435",
#       "UF_CRM_1609922283",
#       "UF_CRM_1686844663",
#       "UF_CRM_1686844828",
#       "UF_CRM_1586840541",
#       "UF_CRM_1686842116",
#       "UF_CRM_1605868368244",
#       "UF_CRM_1686842954",
#       "UF_CRM_1592414506625",
#       "UF_CRM_1614571555",
#       "UF_CRM_1686936620"
#     ]

def tasks_history(desc, conn, cursor):
    cursor.execute(f"""SELECT DISTINCT id FROM tasks_tmp tt 
                       EXCEPT SELECT DISTINCT task_id  FROM tasks_history_tmp
                       WHERE field_name='STATUS' AND value_after='5';""")
    tasks_list = cursor.fetchall()
    print(len(tasks_list))
    for task in tasks_list:
        task_id = task[0]
        add_params = {"taskId": task_id}
        get_from_api(desc["token"], desc["api_url"], desc["table_name"], desc["columns"], add_params, conn, cursor,
                     task_id=task_id)


def date_range_params(desc, lower_date, upper_date):
    return {f'filter[>={desc["delta_field"]}]': str(lower_date),
            f'filter[<={desc["delta_field"]}]': str(upper_date),
            f'order[{desc["delta_field"]}]': "ASC",
            "order[ID]": "ASC"}


def load_delta(desc, conn, cursor, project_id):
    cursor.execute(
        f"""SELECT MAX({desc["delta_field"] if desc["table_name"] != "tasks_tmp" else "changeddate"}) FROM {desc["table_name"]} WHERE project_id='{project_id}';""")
    max_time = cursor.fetchone()[0]

    # print(max_time)
    # print(type(max_time))
    max_time = datetime.strptime(max_time, '%Y-%m-%dT%H:%M:%S') if max_time else START_DATE
    cur_date = datetime.now() - timedelta(hours=3)
    start_date = max_time
    end_date = max_time + timedelta(days=1)
    while end_date < cur_date:
        print(start_date, '||', end_date)
        add_params = date_range_params(desc, start_date, end_date)
        if desc["table_name"] in ("deals_stage_history_tmp"):
            add_params.update({"entityTypeId": 2})

        if desc["table_name"] in ("leads_stage_history_tmp"):
            add_params.update({"entityTypeId": 1})

        # if desc["table_name"]=="fact_deals":
        #     add_params.update({"select[]": DEALS_COLUMNS})

        get_from_api(desc["token"], desc["api_url"], desc["table_name"], desc["columns"], add_params, conn, cursor, project_id=project_id)
        start_date, end_date = end_date, end_date + timedelta(days=1)
    print(start_date, '||', cur_date)
    add_params = date_range_params(desc, start_date, cur_date)
    if desc["table_name"] in ("deals_stage_history_tmp"):
        add_params.update({"entityTypeId": 2})

    if desc["table_name"] in ("leads_stage_history_tmp"):
        add_params.update({"entityTypeId": 1})


    get_from_api(desc["token"], desc["api_url"], desc["table_name"], desc["columns"], add_params, conn, cursor,
                project_id=project_id)


def get_from_api(token, table_url, table_name, columns, add_params, cnxn, cursor, **kwargs):
    start = 0
    req_attempt = 0
    url = f"https://{MAIN_URL}/rest/{ID}/{token}/{table_url}"
    while True:
        if (req_attempt >= REQ_ATT_LIMIT):
            print(f"Tried more than {REQ_ATT_LIMIT} requests")
            return
        try:
            r = requests.get(url=url, params={**add_params, "start": start}, verify=False)
            r.raise_for_status()
            data = r.json()
            upload_data(data, table_name, columns, cnxn, cursor, **kwargs)
            break
        except requests.exceptions.HTTPError as errh:
            if r.status_code == 400:
                return
            print(errh)
            time.sleep(90)
            req_attempt += 1
            print(f"Request attempt #{req_attempt + 1}")
        except Exception as e:
            print(e)
            time.sleep(90)
            req_attempt += 1
            print(f"Request attempt #{req_attempt + 1}")
    time.sleep(2)
    if "total" in data:
        total = data["total"]
        print('Size:', total)
    if "next" not in data:
        return
    req_attempt = 0
    start = data["next"]
    while True:
        if (req_attempt >= REQ_ATT_LIMIT):
            print(f"Tried more than {REQ_ATT_LIMIT} requests")
            return
        try:
            r = requests.get(url=url, params={**add_params, "start": start}, verify=False)
            r.raise_for_status()
            data = r.json()
            upload_data(data, table_name, columns, cnxn, cursor, **kwargs)
        except requests.exceptions.HTTPError as errh:
            if r.status_code == 400:
                break
            print(errh)
            time.sleep(90)
            req_attempt += 1
            print(f"Request attempt #{req_attempt + 1}")
        except Exception as e:
            print(e)
            time.sleep(90)
            req_attempt += 1
            print(f"Request attempt #{req_attempt + 1}")
            continue
        if "next" in data:
            start = data["next"]
            time.sleep(2)
        else:
            break