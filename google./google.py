from pandas import read_excel, read_csv, concat, DataFrame, merge, to_datetime
from google.ads.googleads.errors import GoogleAdsException
from google.ads.googleads.client import GoogleAdsClient
from datetime import datetime, date
from os.path import exists, join
from os import makedirs
import csv

class GoogleMarketing:
    def __init__(self):

        self.path_for_temp = r'C:\Users\Geometry\Geometry\Мастерская - Документы\MetaBI разработка\Marketing\marketing_materials\marketing_temp_tables'
        self.path = r'C:\Users\Geometry\Geometry\Мастерская - Документы\MetaBI разработка\Marketing\funnel_staging'
        self.file_path = r'C:\Users\Geometry\Dropbox\Admin\pilot\dims.xlsx'

        self.page_size = 1000
        self.googleads_client = None
        try:
            self.run()
        except Exception as e:
            print(f"An error occurred in the main process: {str(e)}")

    def query(self, start_date, end_date):
        return f"""SELECT ad_group.id, ad_group.name, campaign.id, campaign.name, 
        metrics.impressions, metrics.clicks, metrics.engagements, metrics.cost_micros, segments.date FROM ad_group_ad
        WHERE segments.date >= '{start_date}' AND segments.date <= '{end_date}'"""

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


    def dropbox_file(self):
        sheet_name = 'dim_marketing_account'
        dim_marketing_account = read_excel(self.file_path, sheet_name=sheet_name)
        return dim_marketing_account

    def initial_data(self, main_df, csv_file, project_name):
        try:
            main_df.drop_duplicates().to_csv(self.folders_for_temp(project_name, csv_file, 'google'), index=False)
        except Exception as e:
            print(e, 2)

    def check_len(self, val):
        if len(val):
            return True
        else:
            return False

    def get_start_date(self, account_id, source_id, project_name):
        if exists(self.folders_for_temp(project_name, 'initial_data.csv', 'google')):
            try:
                csv_df = read_csv(self.folders_for_temp(project_name, 'initial_data.csv', 'google'))
                print("Existing data has length:", len(csv_df))
                df = csv_df[(csv_df['account_id'] == account_id) & (csv_df['source_id'] == source_id)]
                if not df.empty:
                    max_date = datetime.strptime(df['date'].max(), "%Y-%m-%d").date()
                    csv_df = csv_df[(csv_df['date'] != max_date.strftime("%Y-%m-%d")) |
                                    (csv_df['account_id'] != account_id) | (csv_df['source_id'] != source_id)]
                    print("Data without maximum date:", len(csv_df))
                    return csv_df, max_date
                else:
                    return csv_df, None
            except Exception as e:
                print(f"An error occurred while reading the CSV file: {str(e)}")
        return [], None

    def dim_table(self, csv_file, project_name):
        try:
            with open(self.folders_for_temp(project_name, csv_file, 'google'), "r", encoding="utf-8") as file:
                header = next(csv.reader(file))
                data = [row for row in csv.reader(file)]

            df = DataFrame(data, columns=header)
            dim_columns = df[['account_id', 'ad_id', 'ad_name', 'campaign_name', 'project_name', 'source_id']]
            dim_ad = dim_columns.drop_duplicates(subset=['ad_id'])
            dim_ad = dim_ad.assign(cons_ad_id=range(len(dim_ad)))

            dim_ad.to_csv(self.folders(project_name, 'dim_ad.csv'), index=False)
        except Exception as e:
            print(e, 2)

    def fact_marketing(self, csv_file, project_name):
        try:
            if exists(self.folders_for_temp(project_name, csv_file, 'google')):
                df = read_csv(self.folders_for_temp(project_name, csv_file, 'google'))
                selected_columns = ['account_id', 'ad_id', 'date', 'impressions', 'clicks',
                                    'spend', 'project_name', 'source_id']
                fact = df[selected_columns]
                dim_ad = read_csv(self.folders(project_name, 'dim_ad.csv'))
                merged_df = merge(fact, dim_ad[['account_id', 'ad_id', 'source_id', 'cons_ad_id', 'project_name']],
                                  on=['account_id', 'ad_id', 'source_id', 'project_name'], how='left')
                fact_marketing = merged_df[['cons_ad_id', 'date', 'impressions', 'clicks', 'spend', 'project_name', 'source_id']]
                fact_marketing.to_csv(self.folders(project_name, 'fact_marketing.csv'), index=False)
            else:
                print("No initial data found.")
        except Exception as e:
            print(e, 2)

    def dim_ad(self, project_name):
        if exists(self.folders(project_name, 'dim_ad.csv')):
            try:
                final_dim_ad = read_csv(self.folders(project_name, 'dim_ad.csv'))
                dim_columns = final_dim_ad[['cons_ad_id', 'ad_id', 'ad_name', 'campaign_name', 'project_name', 'source_id']]
                final_dim_ad = dim_columns.drop_duplicates(subset=['cons_ad_id'])

                final_dim_ad.to_csv(self.folders(project_name, 'dim_ad.csv'), index=False)
            except IOError as e:
                print('Error:', e)
        else:
            print('dim_ad.csv file not found in the specified folder:', self.folders(project_name, 'dim_ad.csv'))

    def get_data(self, results, account_id, project_name, source_id):
        results_df = []
        for row in results:
            try:
                cost = row.metrics.cost_micros / 1000000
                result_row = [row.ad_group.id, row.ad_group.name, row.campaign.id, row.campaign.name,
                              row.metrics.impressions, row.metrics.clicks, row.metrics.engagements, cost,
                              row.segments.date]
                results_df.append(result_row)
            except Exception as e:
                print(e, 2)

        df = DataFrame(results_df, columns=['ad_id', 'ad_name', 'campaign_id', 'campaign_name',
                                            'impressions', 'clicks', 'engagements', 'spend', 'date'])
        df = df.assign(
                    account_id=account_id,
                    project_name=project_name,
                    source_id=source_id
                )
        return df

    def main(self, account_id, page_size, project_name, source_id):
        ga_service = self.googleads_client.get_service("GoogleAdsService")
        full_data_insights = []

        end_date = datetime.now().date()
        if exists(self.folders_for_temp(project_name, 'initial_data.csv', 'google')):
            csv_df, start_date = self.get_start_date(account_id, source_id, project_name)
            # start_date = date(2019, 1, 8)
            if len(csv_df):
                full_data_insights.append(csv_df)
        else:
            start_date = date(2022, 1, 1)

        search_request = self.googleads_client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = str(account_id)
        search_request.query = self.query(start_date, end_date)
        search_request.page_size = page_size

        results = ga_service.search(request=search_request)
        df = self.get_data(results, account_id, project_name, source_id)
        print("New data has length:", len(df))

        if len(full_data_insights):
            full_data_insights.append(df)
            combined_df = concat(full_data_insights)
        else:
            combined_df = df

        if exists(self.folders_for_temp(project_name, 'initial_data.csv', 'google')):
            print("Updated data has length:", len(combined_df))
        self.initial_data(combined_df, 'initial_data.csv', project_name)
        # self.dim_table('initial_data.csv', project_name)
        # self.fact_marketing('initial_data.csv', project_name)
        # self.dim_ad(project_name)

        # combined_df.to_csv(self.folders(project_name, 'test.csv'), index=False)
        # print(f"Combined data saved to {self.folders(project_name, 'test.csv')}")

    def credentials(self, row):
        developer_token = row['developer_token']
        client_id = row['client_id']
        client_secret = row['client_secret']
        refresh_token = row['access_token']

        return {
            "developer_token": f"{developer_token}",
            "use_proto_plus": False,
            "client_id": f"{client_id}",
            "client_secret": f"{client_secret}",
            "refresh_token": f"{refresh_token}"
        }

    def run(self):
        try:
            for _, row in self.dropbox_file().iterrows():
                source_name = row['source_name']
                if source_name.lower() == 'google':
                    self.googleads_client = GoogleAdsClient.load_from_dict(self.credentials(row), version="v14")

                    account_id = row['account_id']
                    project_name = row['project_name']
                    source_id = row['source_id']
                    source_name = row['source_name']

                    self.main(account_id, self.page_size, project_name, source_id)
                else:
                    print("Skipping non-Google source:", source_name)

        except GoogleAdsException as ex:
            print(
                f'Request with ID "{ex.request_id}" failed with status '
                f'"{ex.error.code().name}" and includes the following errors:'
            )
            for error in ex.failure.errors:
                print(f'\tError with message "{error.message}".')
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        print(f"\t\tOn field: {field_path_element.field_name}")
            exit(1)


marketing = GoogleMarketing()
