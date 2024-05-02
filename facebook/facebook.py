from pandas import DataFrame, concat, read_excel, read_csv, merge, to_datetime
from datetime import timedelta, datetime, date
from requests import get, Session
from os.path import exists, join
from random import uniform
from os import makedirs
from time import sleep
from json import dumps
import csv

class FacebookMarketing:
    def __init__(self):
        
        self.path_for_temp = r'C:\Users\Geometry\Geometry\Мастерская - Документы\MetaBI разработка\Marketing\marketing_materials\marketing_temp_tables'
        self.path = r'C:\Users\Geometry\Geometry\Мастерская - Документы\MetaBI разработка\Marketing\funnel_staging'
        self.file_path = r'C:\Users\Geometry\Dropbox\Admin\pilot\dims.xlsx'

        try:
            self.main()
        except Exception as e:
            print(f"An error occurred in the main process: {str(e)}")

    def params(self, fields, access_token, time_range=None):
        return {
            "fields": fields,
            "time_range": dumps(time_range),
            "level": "ad",
            "limit": 1000,
            "time_increment": 1,
            "access_token": access_token
        }

    def request(self, sess, url, params):
        response = sess.get(url, params=params)
        return response.json(), response.status_code

    def check_len(self, val):
        if len(val):
            return True
        else:
            return False

    def is_in(self, string, val):
        if string in val:
            return True
        else:
            return False

    def get_time_range(self, start_date):
        return {
            "since": start_date.isoformat(),
            "until": (start_date + timedelta(days=7)).isoformat()
        }

    def append_data(self, full_data, data, project_name, source_id):
        data = DataFrame(data["data"])
        data["project_name"] = project_name
        data["source_id"] = source_id
        data = data.rename(columns={'date_start': 'date'})
        full_data.append(data)

    def dropbox_file(self):
        sheet_name = 'dim_marketing_account'
        dim_marketing_account = read_excel(self.file_path, sheet_name=sheet_name)
        return dim_marketing_account

    def add_days(self, date, n):
        return date + timedelta(days=n)

    def get_latest_api_version(self):
        api_url = "https://graph.facebook.com/"
        response = get(api_url)
        if response.status_code == 200:
            data = response.json()
            if 'api_version' in data:
                return data['api_version']
        return "v17.0"

    def url(self, account_id, source, v=None):
        if v is None:
            v = self.get_latest_api_version()
        main_url = f"https://graph.facebook.com/{v}/act_"
        return f"{main_url}{int(account_id)}/{source}"

    def get_data(self, sess, start_date, account_id, full_data, project_name, source_id, access_token):
        time_range = self.get_time_range(start_date)
        fields = "account_id, ad_id, ad_name, campaign_id, campaign_name, date_start, impressions, reach, clicks, spend"
        try:
            data, status_code = self.request(sess, self.url(account_id, 'insights'), self.params(fields, access_token, time_range))
            if status_code != 200:
                print(f"API request failed with error code {status_code}")
                print(f"data: {data}")
            else:
                if self.is_in("data", data):
                    print(len(data["data"]))
                    if self.check_len(data["data"]):
                        self.append_data(full_data, data, project_name, source_id)
        except Exception as e:
            print(f"An error occurred during the API request: {str(e)}")

        return self.add_days(start_date, 7)

    def get_start_date(self, account_id, source_id, project_name):
        if exists(self.folders_for_temp(project_name, 'initial_data.csv', 'facebook')):
            try:
                csv_df = read_csv(self.folders_for_temp(project_name, 'initial_data.csv', 'facebook'))
                df = csv_df[(csv_df['account_id'] == account_id) & (csv_df['source_id'] == source_id)]
                if not df.empty:
                    max_date = datetime.strptime(df['date'].max(), "%Y-%m-%d").date()
                    csv_df = csv_df[(csv_df['date'] != max_date.strftime("%Y-%m-%d")) |
                                    (csv_df['account_id'] != account_id) | (csv_df['source_id'] != source_id)]
                    return csv_df, max_date
                else:
                    return csv_df, None
            except Exception as e:
                print(f"An error occurred while reading the CSV file: {str(e)}")
        return [], None

    def initial_data(self, main_df, csv_file, project_name):
        try:
            # main_df = main_df.rename(columns={'date_start': 'date'})
            # print(main_df.columns)
            main_df.drop_duplicates().to_csv(self.folders_for_temp(project_name, csv_file, 'facebook'), index=False)
        except Exception as e:
            print(f"An error occurred while writing the data to CSV: {str(e)}")

    def fact_marketing(self, csv_file, project_name):
        if exists(self.folders_for_temp(project_name, csv_file, 'facebook')):
            try:
                df = read_csv(self.folders_for_temp(project_name, csv_file, 'facebook'))
                selected_columns = ['account_id', 'ad_id', 'date', 'impressions', 'reach', 'clicks',
                                    'spend', 'project_name', 'source_id']
                fact = df[selected_columns]
                dim_ad = read_csv(self.folders(project_name, 'dim_ad.csv'))
                merged_df = merge(fact, dim_ad[['account_id', 'ad_id', 'source_id', 'cons_ad_id', 'project_name']],
                                  on=['account_id', 'ad_id', 'source_id', 'project_name'], how='left')
                fact_marketing = merged_df[['cons_ad_id', 'date', 'impressions', 'reach', 'clicks', 'spend',
                                            'project_name', 'source_id']]
                fact_marketing.to_csv(self.folders(project_name, 'fact_marketing.csv'), index=False)
            except Exception as e:
                print(f"An error occurred while processing fact marketing data: {str(e)}")
        else:
            print("No initial data found.")

    def dim_table(self, csv_file, project_name):
        try:
            with open(self.folders_for_temp(project_name, csv_file, 'facebook'), "r", encoding="utf-8") as file:
                header = next(csv.reader(file))
                data = [row for row in csv.reader(file)]

            df = DataFrame(data, columns=header)
            dim_columns = df[['account_id', 'ad_id', 'ad_name', 'campaign_name', 'project_name', 'source_id']]
            dim_ad = dim_columns.drop_duplicates(subset=['ad_id'])
            dim_ad = dim_ad.assign(cons_ad_id=range(len(dim_ad)))

            dim_ad.to_csv(self.folders(project_name, 'dim_ad.csv'), index=False)
        except Exception as e:
            print(f"An error occurred while processing dim table data: {str(e)}")

    def dim_ad(self, project_name):
        if exists(self.folders(project_name, 'dim_ad.csv')):
            try:
                final_dim_ad = read_csv(self.folders(project_name, 'dim_ad.csv'))
                dim_columns = final_dim_ad[['cons_ad_id', 'ad_id', 'ad_name', 'campaign_name', 'project_name', 'source_id']]
                final_dim_ad = dim_columns.drop_duplicates(subset=['cons_ad_id'])

                final_dim_ad.to_csv(self.folders(project_name, 'dim_ad.csv'), index=False)
            except IOError as e:
                print(f"An error occurred while processing dim ad data: {str(e)}")
        else:
            print('dim_ad.csv file not found in the specified folder:', self.folders(project_name, 'dim_ad.csv'))

    def folders(self, project_name, csv_file):
        project_folder_path = join(self.path, project_name)
        if not exists(project_folder_path):
            makedirs(project_folder_path)
        return join(project_folder_path, csv_file)

    def folders_for_temp(self, project_name, csv_file, source_name):
        project_folder_path = join(self.path_for_temp, project_name)
        if not exists(project_folder_path):
            makedirs(project_folder_path)

        source_folder_path = join(project_folder_path, source_name)
        if not exists(source_folder_path):
            makedirs(source_folder_path)
        return join(source_folder_path, csv_file)


    def main(self):
        for id, name in self.dropbox_file().iterrows():
            full_data_insights = []
            end_date = datetime.now().date()

            source_id = name['source_id']
            source_name = name['source_name']
            account_id = name['account_id']
            project_name = name['project_name']
            access_token = name['access_token']
            print('access_token', access_token)
            print(self.url(account_id, 'insights'))

            if source_name.lower() == 'facebook':
                if not exists(self.folders_for_temp(project_name, 'initial_data.csv', 'facebook')):
                    start_date = date(2022, 1, 1)
                else:
                    csv_df, start_date = self.get_start_date(account_id, source_id, project_name)
                    if len(csv_df):
                        full_data_insights.append(csv_df)
                if start_date is None:
                    start_date = date(2022, 1, 1)

                sleep(uniform(0.3, 1.5))
                with Session() as sess:
                    while start_date <= end_date:
                        try:
                            start_date = self.get_data(sess, start_date, account_id, full_data_insights, project_name, source_id, access_token)
                            print(start_date)
                        except Exception as e:
                            print(f"An error occurred while getting data: {str(e)}")

                if self.check_len(full_data_insights):
                    main_df = concat(full_data_insights)

                    try:
                        self.initial_data(main_df, 'initial_data.csv', project_name)
                    except Exception as e:
                        print(e, 2)

                    # try:
                    #     self.dim_table('initial_data.csv', project_name)
                    # except Exception as e:
                    #     print(e, 2)
                    #
                    # try:
                    #     self.fact_marketing('initial_data.csv', project_name)
                    # except Exception as e:
                    #     print(e, 2)
                    #
                    # try:
                    #     self.dim_ad(project_name)
                    # except Exception as e:
                    #     print(e, 2)
            else:
                print("Skipping non-Facebook source:", source_name)


marketing = FacebookMarketing()
