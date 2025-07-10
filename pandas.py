#!/usr/bin/env python3

import numpy as np
import pandas as pd
import os
import psycopg2
from datetime import datetime, timedelta
import pyodbc
from smartsheet import Smartsheet, sheets
from sqlalchemy import text
from tqdm import tqdm
import io
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.files.file import File
import math
from decimal import Decimal, ROUND_HALF_UP, ROUND_UP

import get_db_connection

def update_driver_data(report_id, date_range, secondary_driver_hours):

    full_errors = True
    excessive_hour_threshold = 14

    conn = ClientContext("https://deliverit3.sharepoint.com/sites/DataAnalytics").with_credentials(
        UserCredential(
            os.environ.get('O365_USERNAME'),
            os.environ.get('O365_PASSWORD')
        )
    )

    file_obj = File.open_binary(conn, "/sites/DataAnalytics/Shared Documents/General/Daily Report Info.xlsx").content
    data = io.BytesIO(file_obj)

    dbEngine = get_db_connection.get_mysql_engine()

    if not secondary_driver_hours:
        with dbEngine.connect() as con:
            con.execute(text("DELETE FROM data_analytics_reporting.break_pods"))
            con.execute(text("DELETE FROM data_analytics_reporting.current_driver_errors"))
    
    def chunker(seq, size):
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    def insert_calculated_driver_data_with_progress(df):
        chunksize = int(len(df) / min(len(df), 20)) # 5%
        with tqdm(total=len(df)) as pbar:
            for i, cdf in enumerate(chunker(df, chunksize)):
                replace = "replace" if i == 0 else "append"
                cdf.to_sql('calculated_driver_data', dbEngine, schema='data_analytics_reporting', if_exists='append', index=False)
                pbar.update(chunksize)
    
    def insert_break_pods_with_progress(df):
        chunksize = int(len(df) / min(len(df), 20)) # 5%
        with tqdm(total=len(df)) as pbar:
            for i, cdf in enumerate(chunker(df, chunksize)):
                replace = "replace" if i == 0 else "append"
                cdf.to_sql('break_pods', dbEngine, schema='data_analytics_reporting', if_exists='append', index=False)
                pbar.update(chunksize)

    def insert_driver_errors_with_progress(df):
        chunksize = int(len(df) / min(len(df), 20)) # 5%
        with tqdm(total=len(df)) as pbar:
            for i, cdf in enumerate(chunker(df, chunksize)):
                replace = "replace" if i == 0 else "append"
                cdf.to_sql('current_driver_errors', dbEngine, schema='data_analytics_reporting', if_exists='append', index=False)
                pbar.update(chunksize)

    def round_half_up(x, places):
        if x >= 0:
            return float(Decimal("{:.6f}".format(x)).quantize(Decimal(places), rounding=ROUND_HALF_UP))
        else:
            return float(-Decimal("{:.6f}".format(abs(x))).quantize(Decimal(places), rounding=ROUND_HALF_UP))

    def round_up(n, decimals=0):
        n = ((n * (10**(decimals + 4))) // 1) / 10000
        if n >= 0:
            return math.ceil(n) / (10**decimals)
        else:
            return math.floor(n) / (10**decimals)

    end_of_shift_protocol_exceptions = pd.read_excel(data, sheet_name="End of Shift Protocol Exception")['Name'].str.upper().to_list()

    daily_error_exceptions = pd.read_excel(data, sheet_name="Daily Error Exceptions")

    daily_error_exceptions['Name'] = daily_error_exceptions['Name'].str.upper()
    daily_error_exceptions.set_index(keys=['Name', 'Date'], inplace=True)

    driver_hours_query = '''
        SELECT *
            
        FROM
            data_analytics_reporting.driver_hours

        WHERE
            report_id = '{}'
    '''.format(report_id)

    driver_hours = pd.read_sql(driver_hours_query, dbEngine)

    driver_hours['scheduled_time'] = pd.to_datetime(driver_hours['scheduled_time'], format='%H:%M:%S')
    driver_hours['scheduled_time'] = driver_hours.apply(lambda x: datetime(x['clock_in'].year, x['clock_in'].month, x['clock_in'].day, int(x['scheduled_time'].hour), int(x['scheduled_time'].minute)) if pd.notna(x['scheduled_time']) else np.nan, axis=1)
    driver_hours['date'] = pd.to_datetime(driver_hours['date'], format='%Y-%m-%d', errors='coerce')
    driver_hours['first_break'] = pd.to_datetime(driver_hours['first_break'], format='%Y-%m-%d', errors='coerce')
    driver_hours['second_break'] = pd.to_datetime(driver_hours['second_break'], format='%Y-%m-%d', errors='coerce')
    driver_hours['lunch_start_radio_button'] = pd.to_datetime(driver_hours['lunch_start_radio_button'], format='%Y-%m-%d', errors='coerce')
    driver_hours['lunch_start'] = pd.to_datetime(driver_hours['lunch_start'], format='%Y-%m-%d', errors='coerce')
    driver_hours['lunch_end'] = pd.to_datetime(driver_hours['lunch_end'], format='%Y-%m-%d', errors='coerce')
    driver_hours['sorting_start'] = pd.to_datetime(driver_hours['sorting_start'], format='%Y-%m-%d', errors='coerce')
    driver_hours['sorting_end'] = pd.to_datetime(driver_hours['sorting_end'], format='%Y-%m-%d', errors='coerce')
    driver_hours['pickup_dropoff_start'] = pd.to_datetime(driver_hours['pickup_dropoff_start'], format='%Y-%m-%d', errors='coerce')
    driver_hours['pickup_dropoff_end'] = pd.to_datetime(driver_hours['pickup_dropoff_start'], format='%Y-%m-%d', errors='coerce')
    driver_hours['line_haul_start'] = pd.to_datetime(driver_hours['line_haul_start'], format='%Y-%m-%d', errors='coerce')
    driver_hours['line_haul_end'] = pd.to_datetime(driver_hours['line_haul_end'], format='%Y-%m-%d', errors='coerce')
    driver_hours['vehicle_maintenance_start'] = pd.to_datetime(driver_hours['vehicle_maintenance_start'], format='%Y-%m-%d', errors='coerce')
    driver_hours['vehicle_maintenance_end'] = pd.to_datetime(driver_hours['vehicle_maintenance_end'], format='%Y-%m-%d', errors='coerce')

    employee_directory_query = '''
        SELECT
            department_no,
            last_name,
            first_name,
            job_title,
            employee_status,
            location,
            hire_date,
            termination_date,
            pay_rate_1 AS non_production_rate,
            pay_rate_7 AS production_base_rate

        FROM
            data_analytics_reporting.employee_directory  

        WHERE
            report_id = (
                SELECT COALESCE(
                    (SELECT report_id
                    FROM data_analytics_reporting.employee_directory
                    WHERE report_id > ADDDATE('{}', INTERVAL 5 DAY)
                    ORDER BY report_id
                    LIMIT 1), 
                    MAX(report_id))
                FROM data_analytics_reporting.employee_directory
            )
    '''.format(date_range[1])

    employee_directory = pd.read_sql(employee_directory_query, dbEngine)
    employee_directory['department_no'] = employee_directory['department_no'].astype('str')
    employee_directory['full_name'] = employee_directory['last_name'] + ", " + employee_directory['first_name']
    employee_directory['production_base_rate'] = np.where(employee_directory['production_base_rate'] == 0.0, employee_directory['non_production_rate'], employee_directory['production_base_rate'])

    concise_name_alias = pd.read_sql("SELECT * FROM data_analytics_reporting.concise_name_alias", dbEngine)

    concise_name_alias['concise_name'] = concise_name_alias['concise_name'].str.upper()
    concise_name_alias['alias'] = concise_name_alias['alias'].str.upper()

    delivery_data_query = '''
        SELECT
            assignee_name,
            delivery_time,
            order_number

        FROM
            data_analytics_reporting.payroll_delivery_data

        WHERE
                delivery_time >= '{}' AND delivery_time <= '{}'
            AND assignee_name IS NOT NULL
    '''.format(date_range[0], date_range[1])

    delivery_data = pd.read_sql(delivery_data_query, dbEngine)

    delivery_data['assignee_name'] = delivery_data['assignee_name'].str.upper()
    delivery_data['date'] = delivery_data['delivery_time'].dt.normalize()

    clean_driver_names = pd.read_sql("SELECT * FROM data_analytics_reporting.clean_driver_names", dbEngine)

    clean_driver_names['assignee_name'] = clean_driver_names['assignee_name'].str.upper()
    clean_driver_names.set_index(keys=['assignee_name'], inplace=True)
    clean_driver_names['driver_name'] = clean_driver_names['sage_name'].str.upper()

    delivery_data = delivery_data.join(clean_driver_names.loc[:, ['driver_name']], on='assignee_name')

    delivery_data = delivery_data.loc[~delivery_data['driver_name'].isna()]
    delivery_data = delivery_data.rename(columns={'driver_name':'final_name'})

    driver_hours['combined_name'] = (driver_hours['last_name'] + ", " + driver_hours['first_name']).str.upper()

    driver_hours = driver_hours.join(concise_name_alias.set_index('concise_name'), on='combined_name')
    driver_hours.loc[(driver_hours['hub'] == 'TUS') & (driver_hours['combined_name'] == 'PEREZ, JOSE'),'alias'] = 'PEREZ, JOSE DE JESUS'

    driver_hours['final_name'] = driver_hours.apply(lambda x: x['combined_name'] if pd.isnull(x['alias']) else x['alias'], axis=1).str.upper()
    driver_hours.sort_values(by=['last_name', 'first_name', 'date', 'clock_in', 'clock_out', 'total_hours', 'reg_hours', 'ot_hours', 'dt_hours', 'miles'], inplace=True)

    driver_hours = driver_hours.join(daily_error_exceptions, on=['final_name', 'date'])

    
    print("Updating lunch PODs...")
    
    pod_list = delivery_data.loc[:, ['final_name', 'date']].drop_duplicates()
    comm_list = delivery_data.loc[:, ['final_name', 'date', 'delivery_time']]
    comm_list = comm_list.join(driver_hours.loc[:, ['final_name', 'date', 'lunch_start']].drop_duplicates().set_index(keys=['final_name', 'date']), on=['final_name', 'date'])
    comm_list = comm_list.loc[comm_list['lunch_start'] < comm_list['delivery_time']].drop(columns=['lunch_start']).groupby(by=['final_name', 'date'])['delivery_time']

    first_pod_1 = comm_list.nsmallest(n=1).groupby(level=['final_name', 'date']).last().rename("first_lunch_pod_1")
    pod_list = pod_list.join(first_pod_1, on=['final_name', 'date'])
    first_pod_2 = comm_list.nsmallest(n=2).groupby(level=['final_name', 'date']).last().rename("first_lunch_pod_2")
    pod_list = pod_list.join(first_pod_2, on=['final_name', 'date'])
    first_pod_3 = comm_list.nsmallest(n=3).groupby(level=['final_name', 'date']).last().rename("first_lunch_pod_3")
    pod_list = pod_list.join(first_pod_3, on=['final_name', 'date'])
    first_pod_4 = comm_list.nsmallest(n=4).groupby(level=['final_name', 'date']).last().rename("first_lunch_pod_4")
    pod_list = pod_list.join(first_pod_4, on=['final_name', 'date'])
    first_pod_5 = comm_list.nsmallest(n=5).groupby(level=['final_name', 'date']).last().rename("first_lunch_pod_5")
    pod_list = pod_list.join(first_pod_5, on=['final_name', 'date'])

    pod_list.sort_values(by=['final_name', 'date'], inplace=True)

    pod_list.set_index(['final_name', 'date'], inplace=True)

    driver_hours = driver_hours.join(pod_list, on=['final_name', 'date'])

    pod_diff_list = delivery_data.loc[:, ['final_name', 'date', 'delivery_time']].sort_values(by=['final_name', 'delivery_time'])

    pod_diff_list['post_deliver_time'] = pod_diff_list['delivery_time'].shift(-1)
    pod_diff_list['break_diff'] = (pod_diff_list['post_deliver_time'] - pod_diff_list['delivery_time']) / np.timedelta64(1, 'h')
    pod_diff_list['post_final_name'] = pod_diff_list['final_name'].shift(-1)
    pod_diff_list['post_date'] = pod_diff_list['date'].shift(-1)

    pod_diff_list = pod_diff_list.loc[(pod_diff_list['break_diff'] >= 0.5) & (pod_diff_list['final_name'] == pod_diff_list['post_final_name']) & (pod_diff_list['date'] == pod_diff_list['post_date'])]

    pod_diff_list = pod_diff_list.drop_duplicates(subset=['final_name', 'date'], keep='first').drop(columns=['post_final_name', 'post_date']).rename(columns={'delivery_time' : 'first_pod_before_break', 'post_deliver_time' : 'first_pod_after_break'})

    pod_lunch_list = delivery_data.loc[:, ['final_name', 'date', 'delivery_time']].join(driver_hours.loc[:, ['final_name', 'date', 'lunch_start', 'lunch_end']].set_index(['final_name', 'date']), on=['final_name', 'date']).sort_values(by=['final_name', 'delivery_time'])

    pod_lunch_list['delivery_time'] = pod_lunch_list['delivery_time'].dt.floor('Min')

    pod_lunch_list = pod_lunch_list.loc[(pod_lunch_list['delivery_time'] > pod_lunch_list['lunch_start']) & (pod_lunch_list['delivery_time'] < pod_lunch_list['lunch_end'])]

    lunch_pod_group = pod_lunch_list.groupby(by=['final_name', 'date'])['delivery_time'].count()
    lunch_pod_group.name = "lunch_pod_count"

    pod_diff_list = pod_diff_list.join(lunch_pod_group, on=['final_name', 'date'])

    insert_break_pods_with_progress(pod_diff_list)
    pod_diff_list.set_index(['final_name', 'date'], inplace=True)

    driver_hours = driver_hours.join(pod_diff_list, on=['final_name', 'date'])

    driver_hours['final_lunch_time'] = pd.Series(np.where((((driver_hours['lunch_time'].notna()) & (driver_hours['lunch_time'] > 0))), driver_hours['lunch_time'], (driver_hours['total_hours'] - (driver_hours['reg_hours'] + driver_hours['ot_hours'] + driver_hours['dt_hours'])))).apply(lambda x: round_half_up(x, "0.01"))


    print("Updating driver hours...")
    
    driver_hours['line'] = driver_hours.groupby(['final_name', 'date']).cumcount() + 1
    driver_hours['week_number'] = driver_hours['clock_in'].apply(lambda x: np.nan if pd.isna(x) else x.isocalendar().week - (x.isoweekday() < 7) + 2)
    driver_hours['day'] = driver_hours['clock_in'].apply(lambda x: x.isoweekday() % 7 + 1)

    current_hub = driver_hours[['final_name', 'route', 'hub']].drop_duplicates(subset=['final_name'])
    current_hub['final_name_upper'] = current_hub['final_name'].str.upper()
    employee_directory['full_name_upper'] = employee_directory.drop_duplicates(subset=['full_name'])['full_name'].str.upper()
    current_hub = current_hub.join(employee_directory.set_index('full_name_upper'), on='final_name_upper')

    current_hub.rename(columns={'employee_status': 'status', 'department_no': 'dep_no', 'job_title': 'title', 'location': 'current_hub'}, inplace=True)
    current_hub.drop(columns=['route', 'hub', 'last_name', 'first_name', 'full_name', 'final_name_upper'], inplace=True)
    current_hub.set_index('final_name', inplace=True)

    driver_hours = driver_hours.join(current_hub, on=['final_name'])
    driver_hours['hub'] = driver_hours['current_hub']

    driver_hours['current_payroll_period'] = (driver_hours['date'] >= date_range[0]) & (driver_hours['date'] <= date_range[1])
    driver_hours['line_to_show_on_timecard'] = ((driver_hours['total_hours'] > 0) & (pd.notna(driver_hours['total_hours']))) | (pd.notna(driver_hours['lunch_start']))
    driver_hours['detailed_payroll_filter'] = (driver_hours['dep_no'].isin(["10", "20", "55"]))
    driver_hours['per_stop_driver'] = (driver_hours['hub'].isin(["CAJ", "CNG", "EST", "ING", "LAN", "NSD", "ONT", "ONX", "PMF", "RIV", "SFS", "SND", "TUS", "VLY"])) & (driver_hours['dep_no'] == "55")

    hours_group = driver_hours.loc[:, ['final_name', 'date', 'clock_in', 'clock_out', 'total_hours', 'reg_hours', 'ot_hours', 'dt_hours', 'lunch_start', 'final_lunch_time']].groupby(by=['final_name', 'date'])[['clock_in', 'clock_out', 'total_hours', 'reg_hours', 'ot_hours', 'dt_hours', 'lunch_start', 'final_lunch_time']].agg({'clock_in' : 'min', 'clock_out' : 'max', 'total_hours' : ['sum', 'max'], 'reg_hours' : 'sum', 'ot_hours' : 'sum', 'dt_hours' : 'sum', 'lunch_start' : 'count', 'final_lunch_time' : 'sum'})
    hours_group.columns = hours_group.columns.to_flat_index()
    hours_group.rename(columns={('clock_in', 'min'):'first_clock_in', ('clock_out', 'max'):'last_clock_out', ('total_hours', 'sum') : 'total_hours_sum', ('total_hours', 'max') : 'total_hours_max', ('reg_hours', 'sum') : 'reg_hours_sum', ('ot_hours', 'sum') : 'ot_hours_sum', ('dt_hours', 'sum') : 'dt_hours_sum', ('lunch_start', 'count') : 'lunch_start_count', ('final_lunch_time', 'sum') : 'final_lunch_time_sum'}, inplace=True)

    driver_hours = driver_hours.join(hours_group, on=['final_name', 'date'])

    driver_hours['post_final_name'] = driver_hours['final_name'].shift(-1)
    driver_hours['post_date'] = driver_hours['date'].shift(-1)
    driver_hours['post_clock_in'] = driver_hours['clock_in'].shift(-1)

    conditions = [
        ((driver_hours['lunch_start'].notna()) & ((driver_hours['lunch_start'] + pd.to_timedelta((driver_hours['total_hours'].fillna(value=0)  - (driver_hours['reg_hours'].fillna(value=0) + driver_hours['ot_hours'].fillna(value=0) + driver_hours['dt_hours'].fillna(value=0))), unit='hours')) < driver_hours['clock_out'])),
        ((driver_hours['total_hours_sum'] < 6) | ((driver_hours['lunch_start'].isna()) & (driver_hours['lunch_start_count'] > 0)) | ((driver_hours['lunch_start_count'] == 0) & (driver_hours['total_hours'] != driver_hours['total_hours_max'])) | (~driver_hours['line_to_show_on_timecard']) | ((driver_hours['final_name'] == driver_hours['post_final_name']) & (driver_hours['date'] == driver_hours['post_date']) & (driver_hours['lunch_start'].isna()) & (((driver_hours['post_clock_in'] - driver_hours['clock_out']) / np.timedelta64(1, 'h')) > 0.25))),
        ((driver_hours['lunch_start_radio_button'].notna()) & (driver_hours['lunch_start_radio_button'] < (driver_hours['first_clock_in'] + pd.Timedelta(hours=4, minutes=59))) & ((driver_hours['lunch_start_radio_button'] + pd.to_timedelta((driver_hours['total_hours'].fillna(value=0)  - (driver_hours['reg_hours'].fillna(value=0) + driver_hours['ot_hours'].fillna(value=0) + driver_hours['dt_hours'].fillna(value=0))), unit='hours')) < driver_hours['clock_out'])),
        ((driver_hours['first_pod_before_break'].notna()) & (driver_hours['first_pod_before_break'] > driver_hours['clock_in']) & ((driver_hours['first_pod_before_break'] + pd.to_timedelta((driver_hours['total_hours'].fillna(value=0)  - (driver_hours['reg_hours'].fillna(value=0) + driver_hours['ot_hours'].fillna(value=0) + driver_hours['dt_hours'].fillna(value=0))), unit='hours')) < driver_hours['clock_out']))
    ]
    choices = [
        driver_hours['lunch_start'],
        pd.NaT,
        driver_hours['lunch_start_radio_button'],
        driver_hours['first_pod_before_break']
    ]
    driver_hours['adj_lunch_start'] = pd.Series(np.select(conditions, choices, default=(driver_hours['first_clock_in'] + pd.Timedelta(hours=4, minutes=59))), dtype='datetime64[ns]')

    conditions = [
        (driver_hours['adj_lunch_start'].isna()),
        ((driver_hours['total_hours_sum'] > 12.5) & (driver_hours['lunch_start_count'] == 0)),
        (((driver_hours['lunch_end'] - driver_hours['lunch_start']) > pd.Timedelta(minutes=1)) & (driver_hours['lunch_end'] < driver_hours['clock_out']))
    ]
    choices = [
        pd.NaT,
        (driver_hours['adj_lunch_start'] + pd.Timedelta(minutes=30)),
        driver_hours['lunch_end']
    ]
    driver_hours['adj_lunch_end'] = pd.Series(np.select(conditions, choices, default=(driver_hours['adj_lunch_start'] + pd.to_timedelta((driver_hours['total_hours'].fillna(value=0)  - (driver_hours['reg_hours'].fillna(value=0) + driver_hours['ot_hours'].fillna(value=0) + driver_hours['dt_hours'].fillna(value=0))), unit='hours'))), dtype='datetime64[ns]')

    driver_hours['adj_lunch_start_2'] = pd.Series(np.where(((driver_hours['total_hours_sum'] > 12.5) & (driver_hours['lunch_start_count'] == 0)), 
    (driver_hours['adj_lunch_end'] + pd.Timedelta(hours=4)), pd.NaT), dtype='datetime64[ns]')

    driver_hours['adj_lunch_end_2'] = pd.Series(np.where(driver_hours['adj_lunch_start_2'].isna(), pd.NaT, (driver_hours['adj_lunch_start_2'] + pd.Timedelta(minutes=30))), dtype='datetime64[ns]')

    second_lunch_group = driver_hours.groupby(['final_name'])[['adj_lunch_start_2']].count()
    second_lunch_group.rename(columns={'adj_lunch_start_2' : 'adj_lunch_start_2_count'}, inplace=True)

    driver_hours = driver_hours.join(second_lunch_group, on='final_name')

    driver_hours['include_2nd_lunch_on_timecard'] = driver_hours['adj_lunch_start_2_count'] > 0

    summarized_delivery_data_query = '''
        SELECT
            driver_name AS final_name,
            driver_code,
            delivery_date AS date,
            packages AS sdd_packages,
            stops AS sdd_stops,
            service_amount AS sdd_service_amount,
            amount_charged AS sdd_amount_charged,
            signature_time_deduction AS sdd_signature_time_deduction,
            signature_time_deductions_taken AS sdd_signature_time_deductions_taken,
            on_time AS on_time_percent,
            first_pod_1 AS sdd_first_pod_1,
            first_pod_2 AS sdd_first_pod_2,
            first_pod_3 AS sdd_first_pod_3,
            first_pod_4 AS sdd_first_pod_4,
            first_pod_5 AS sdd_first_pod_5,
            first_pod_6 AS sdd_first_pod_6,
            first_pod_7 AS sdd_first_pod_7,
            first_pod_8 AS sdd_first_pod_8,
            first_pod_9 AS sdd_first_pod_9,
            first_pod_10 AS sdd_first_pod_10,
            last_pod_1 AS sdd_last_pod_1,
            last_pod_2 AS sdd_last_pod_2,
            last_pod_3 AS sdd_last_pod_3,
            last_pod_4 AS sdd_last_pod_4,
            last_pod_5 AS sdd_last_pod_5,
            last_pod_6 AS sdd_last_pod_6,
            last_pod_7 AS sdd_last_pod_7,
            last_pod_8 AS sdd_last_pod_8,
            last_pod_9 AS sdd_last_pod_9,
            last_pod_10 AS sdd_last_pod_10

        FROM
            data_analytics_reporting.summarized_delivery_data

        WHERE
                delivery_date >= '{}'
            AND delivery_date <= '{}'
    '''.format(date_range[0], date_range[1])

    summarized_delivery_data = pd.read_sql(summarized_delivery_data_query, dbEngine)

    other_delivery_data_query = '''
        SELECT *

        FROM
            data_analytics_reporting.other_delivery_data

        WHERE
                delivery_date >= '{}'
            AND delivery_date <= '{}'
    '''.format(date_range[0], date_range[1])

    other_delivery_data = pd.read_sql(other_delivery_data_query, dbEngine)
    other_delivery_data.rename(columns={'driver_name' : 'final_name', 'delivery_date' : 'date'}, inplace=True)

    detailed_delivery_data_query = '''
        SELECT *

        FROM
            data_analytics_reporting.detailed_delivery_data

        WHERE
                delivery_date >= '{}'
            AND delivery_date <= '{}'
    '''.format(date_range[0], date_range[1])

    detailed_delivery_data = pd.read_sql(detailed_delivery_data_query, dbEngine)
    detailed_delivery_data.rename(columns={'driver_name' : 'final_name', 'delivery_date' : 'date'}, inplace=True)

    stops_no_hours_list = summarized_delivery_data.groupby(['final_name', 'date']).agg({'sdd_stops' : 'sum', 'sdd_first_pod_1' : 'min', 'sdd_first_pod_2' : 'min', 'sdd_first_pod_10' : 'min', 'sdd_last_pod_10' : 'max', 'sdd_last_pod_2' : 'max', 'sdd_last_pod_1' : 'max'}).reset_index()
    stops_no_hours_list = stops_no_hours_list.join(driver_hours.loc[:, ['final_name', 'date', 'first_clock_in', 'last_clock_out']].drop_duplicates().set_index(keys=['final_name', 'date']), on=['final_name', 'date'])

    stops_no_hours_list = stops_no_hours_list.join(employee_directory.set_index('full_name_upper'), on='final_name')

    stops_no_hours_list = stops_no_hours_list.loc[(stops_no_hours_list['first_clock_in'].isna()) & (stops_no_hours_list['sdd_stops'] >= 20) & (stops_no_hours_list['employee_status'] == "A") & ((stops_no_hours_list['department_no'].isin(["10", "55"])) | ((stops_no_hours_list['department_no'] == "20") & (stops_no_hours_list['job_title'].str.contains(pat="relief", case=False, na=False)))), ['final_name', 'location', 'date', 'sdd_stops', 'sdd_first_pod_1', 'sdd_first_pod_2', 'sdd_first_pod_10', 'sdd_last_pod_10', 'sdd_last_pod_2', 'sdd_last_pod_1']]
    stops_no_hours_list.rename(columns={'final_name' : 'name', 'location' : 'hub'}, inplace=True)

    stops_no_hours_list['pod_errors'] = stops_no_hours_list['sdd_stops'].astype('str') + " stops but no hours"
    stops_no_hours_list.drop(columns=['sdd_stops'], inplace=True)
    stops_no_hours_list['clock_in'] = stops_no_hours_list['date']

    cov_driver_hours = driver_hours.loc[driver_hours['detailed_payroll_filter']].groupby(by=['dep_no', 'title', 'status', 'route', 'hire_date', 'final_name', 'current_hub', 'clock_in', 'clock_out', 'lunch_start', 'lunch_end', 'adj_lunch_start', 'adj_lunch_end', 'sorting_start', 'sorting_end', 'pickup_dropoff_start', 'pickup_dropoff_end', 'line_haul_start', 'line_haul_end', 'vehicle_maintenance_start', 'vehicle_maintenance_end'], dropna=False)[['total_hours', 'reg_hours', 'ot_hours', 'dt_hours', 'final_lunch_time', 'sorting_time', 'pickup_dropoff_time', 'line_haul_time', 'vehicle_maintenance_time', 'lunch_pod_count']].sum().sort_values(by=['dep_no', 'status', 'final_name', 'clock_in', 'clock_out', 'adj_lunch_start', 'total_hours'], ascending=[False, True, True, True, True, True, False], na_position='last').reset_index()

    if "sorting_time" not in cov_driver_hours.columns:
        cov_driver_hours['sorting_time'] = 0.0
    if "pickup_dropoff_time" not in cov_driver_hours.columns:
        cov_driver_hours['pickup_dropoff_time'] = 0.0
    if "line_haul_time" not in cov_driver_hours.columns:
        cov_driver_hours['line_haul_time'] = 0.0
    if "vehicle_maintenance_time" not in cov_driver_hours.columns:
        cov_driver_hours['vehicle_maintenance_time'] = 0.0

    cov_driver_hours.rename(columns={'current_hub' : 'hub'}, inplace=True)

    cov_driver_hours['date'] = cov_driver_hours['clock_in'].dt.normalize()
    b =  cov_driver_hours['date'] + pd.offsets.Week(weekday=5)
    m2 = cov_driver_hours['date'] != (b - pd.offsets.Week())
    cov_driver_hours['week_ending'] = cov_driver_hours['date'].mask(m2, b)

    cov_driver_hours = cov_driver_hours.join(summarized_delivery_data.loc[:, ['final_name', 'date', 'on_time_percent', 'sdd_first_pod_1', 'sdd_first_pod_2', 'sdd_first_pod_3', 'sdd_first_pod_4', 'sdd_first_pod_5', 'sdd_first_pod_6', 'sdd_first_pod_7', 'sdd_first_pod_8', 'sdd_first_pod_9', 'sdd_first_pod_10', 'sdd_last_pod_10', 'sdd_last_pod_9', 'sdd_last_pod_8', 'sdd_last_pod_7', 'sdd_last_pod_6', 'sdd_last_pod_5', 'sdd_last_pod_4', 'sdd_last_pod_3', 'sdd_last_pod_2', 'sdd_last_pod_1']].drop_duplicates().set_index(keys=['final_name', 'date']), on=['final_name', 'date'])

    summarized_detailed_delivery_data = detailed_delivery_data.loc[:, ['final_name', 'date']].drop_duplicates()
    comm_list = detailed_delivery_data.loc[:, ['final_name', 'date', 'delivery_time']]

    comm_list = comm_list.drop_duplicates().groupby(by=['final_name', 'date'])['delivery_time']
    first_pod_1 = comm_list.nsmallest(n=1).groupby(level=['final_name', 'date']).last().rename("ddd_first_pod_1")
    summarized_detailed_delivery_data = summarized_detailed_delivery_data.join(first_pod_1, on=['final_name', 'date'])
    last_pod_1 = comm_list.nlargest(n=1).groupby(level=['final_name', 'date']).last().rename("ddd_last_pod_1")
    summarized_detailed_delivery_data = summarized_detailed_delivery_data.join(last_pod_1, on=['final_name', 'date'])

    comm_list = detailed_delivery_data.loc[:, ['final_name', 'date', 'delivery_time']].sort_values(by=['final_name', 'delivery_time'])
    comm_list = comm_list.drop_duplicates(subset=['final_name', 'date', 'delivery_time']).groupby(by=['final_name', 'date'])['delivery_time']

    first_pod_2 = comm_list.nsmallest(n=2).groupby(level=['final_name', 'date']).last().rename("ddd_first_pod_2")
    summarized_detailed_delivery_data = summarized_detailed_delivery_data.join(first_pod_2, on=['final_name', 'date'])
    first_pod_3 = comm_list.nsmallest(n=3).groupby(level=['final_name', 'date']).last().rename("ddd_first_pod_3")
    summarized_detailed_delivery_data = summarized_detailed_delivery_data.join(first_pod_3, on=['final_name', 'date'])
    first_pod_4 = comm_list.nsmallest(n=4).groupby(level=['final_name', 'date']).last().rename("ddd_first_pod_4")
    summarized_detailed_delivery_data = summarized_detailed_delivery_data.join(first_pod_4, on=['final_name', 'date'])
    first_pod_5 = comm_list.nsmallest(n=5).groupby(level=['final_name', 'date']).last().rename("ddd_first_pod_5")
    summarized_detailed_delivery_data = summarized_detailed_delivery_data.join(first_pod_5, on=['final_name', 'date'])
    first_pod_6 = comm_list.nsmallest(n=6).groupby(level=['final_name', 'date']).last().rename("ddd_first_pod_6")
    summarized_detailed_delivery_data = summarized_detailed_delivery_data.join(first_pod_6, on=['final_name', 'date'])
    first_pod_7 = comm_list.nsmallest(n=7).groupby(level=['final_name', 'date']).last().rename("ddd_first_pod_7")
    summarized_detailed_delivery_data = summarized_detailed_delivery_data.join(first_pod_7, on=['final_name', 'date'])
    first_pod_8 = comm_list.nsmallest(n=8).groupby(level=['final_name', 'date']).last().rename("ddd_first_pod_8")
    summarized_detailed_delivery_data = summarized_detailed_delivery_data.join(first_pod_8, on=['final_name', 'date'])
    first_pod_9 = comm_list.nsmallest(n=9).groupby(level=['final_name', 'date']).last().rename("ddd_first_pod_9")
    summarized_detailed_delivery_data = summarized_detailed_delivery_data.join(first_pod_9, on=['final_name', 'date'])
    first_pod_10 = comm_list.nsmallest(n=10).groupby(level=['final_name', 'date']).last().rename("ddd_first_pod_10")
    summarized_detailed_delivery_data = summarized_detailed_delivery_data.join(first_pod_10, on=['final_name', 'date'])

    summarized_detailed_delivery_data['ddd_first_pod_10'] = pd.to_datetime(np.where(summarized_detailed_delivery_data['ddd_first_pod_9'] == summarized_detailed_delivery_data['ddd_first_pod_10'], pd.NaT, summarized_detailed_delivery_data['ddd_first_pod_10'].astype(str)))
    summarized_detailed_delivery_data['ddd_first_pod_9'] = pd.to_datetime(np.where(summarized_detailed_delivery_data['ddd_first_pod_8'] == summarized_detailed_delivery_data['ddd_first_pod_9'], pd.NaT, summarized_detailed_delivery_data['ddd_first_pod_9'].astype(str)))
    summarized_detailed_delivery_data['ddd_first_pod_8'] = pd.to_datetime(np.where(summarized_detailed_delivery_data['ddd_first_pod_7'] == summarized_detailed_delivery_data['ddd_first_pod_8'], pd.NaT, summarized_detailed_delivery_data['ddd_first_pod_8'].astype(str)))
    summarized_detailed_delivery_data['ddd_first_pod_7'] = pd.to_datetime(np.where(summarized_detailed_delivery_data['ddd_first_pod_6'] == summarized_detailed_delivery_data['ddd_first_pod_7'], pd.NaT, summarized_detailed_delivery_data['ddd_first_pod_7'].astype(str)))
    summarized_detailed_delivery_data['ddd_first_pod_6'] = pd.to_datetime(np.where(summarized_detailed_delivery_data['ddd_first_pod_5'] == summarized_detailed_delivery_data['ddd_first_pod_6'], pd.NaT, summarized_detailed_delivery_data['ddd_first_pod_6'].astype(str)))
    summarized_detailed_delivery_data['ddd_first_pod_5'] = pd.to_datetime(np.where(summarized_detailed_delivery_data['ddd_first_pod_4'] == summarized_detailed_delivery_data['ddd_first_pod_5'], pd.NaT, summarized_detailed_delivery_data['ddd_first_pod_5'].astype(str)))
    summarized_detailed_delivery_data['ddd_first_pod_4'] = pd.to_datetime(np.where(summarized_detailed_delivery_data['ddd_first_pod_3'] == summarized_detailed_delivery_data['ddd_first_pod_4'], pd.NaT, summarized_detailed_delivery_data['ddd_first_pod_4'].astype(str)))
    summarized_detailed_delivery_data['ddd_first_pod_3'] = pd.to_datetime(np.where(summarized_detailed_delivery_data['ddd_first_pod_2'] == summarized_detailed_delivery_data['ddd_first_pod_3'], pd.NaT, summarized_detailed_delivery_data['ddd_first_pod_3'].astype(str)))
    summarized_detailed_delivery_data['ddd_first_pod_2'] = pd.to_datetime(np.where(summarized_detailed_delivery_data['ddd_first_pod_1'] == summarized_detailed_delivery_data['ddd_first_pod_2'], pd.NaT, summarized_detailed_delivery_data['ddd_first_pod_2'].astype(str)))

    comm_list = detailed_delivery_data.loc[:, ['final_name', 'date', 'delivery_time']].sort_values(by=['final_name', 'delivery_time'], ascending=False)
    comm_list = comm_list.drop_duplicates(subset=['final_name', 'date', 'delivery_time']).groupby(by=['final_name', 'date'])['delivery_time']

    last_pod_2 = comm_list.nlargest(n=2).groupby(level=['final_name', 'date']).last().rename("ddd_last_pod_2")
    summarized_detailed_delivery_data = summarized_detailed_delivery_data.join(last_pod_2, on=['final_name', 'date'])
    last_pod_3 = comm_list.nlargest(n=3).groupby(level=['final_name', 'date']).last().rename("ddd_last_pod_3")
    summarized_detailed_delivery_data = summarized_detailed_delivery_data.join(last_pod_3, on=['final_name', 'date'])
    last_pod_4 = comm_list.nlargest(n=4).groupby(level=['final_name', 'date']).last().rename("ddd_last_pod_4")
    summarized_detailed_delivery_data = summarized_detailed_delivery_data.join(last_pod_4, on=['final_name', 'date'])
    last_pod_5 = comm_list.nlargest(n=5).groupby(level=['final_name', 'date']).last().rename("ddd_last_pod_5")
    summarized_detailed_delivery_data = summarized_detailed_delivery_data.join(last_pod_5, on=['final_name', 'date'])
    last_pod_6 = comm_list.nlargest(n=6).groupby(level=['final_name', 'date']).last().rename("ddd_last_pod_6")
    summarized_detailed_delivery_data = summarized_detailed_delivery_data.join(last_pod_6, on=['final_name', 'date'])
    last_pod_7 = comm_list.nlargest(n=7).groupby(level=['final_name', 'date']).last().rename("ddd_last_pod_7")
    summarized_detailed_delivery_data = summarized_detailed_delivery_data.join(last_pod_7, on=['final_name', 'date'])
    last_pod_8 = comm_list.nlargest(n=8).groupby(level=['final_name', 'date']).last().rename("ddd_last_pod_8")
    summarized_detailed_delivery_data = summarized_detailed_delivery_data.join(last_pod_8, on=['final_name', 'date'])
    last_pod_9 = comm_list.nlargest(n=9).groupby(level=['final_name', 'date']).last().rename("ddd_last_pod_9")
    summarized_detailed_delivery_data = summarized_detailed_delivery_data.join(last_pod_9, on=['final_name', 'date'])
    last_pod_10 = comm_list.nlargest(n=10).groupby(level=['final_name', 'date']).last().rename("ddd_last_pod_10")
    summarized_detailed_delivery_data = summarized_detailed_delivery_data.join(last_pod_10, on=['final_name', 'date'])

    summarized_detailed_delivery_data['ddd_last_pod_10'] = pd.to_datetime(np.where(summarized_detailed_delivery_data['ddd_last_pod_9'] == summarized_detailed_delivery_data['ddd_last_pod_10'], pd.NaT, summarized_detailed_delivery_data['ddd_last_pod_10'].astype(str)))
    summarized_detailed_delivery_data['ddd_last_pod_9'] = pd.to_datetime(np.where(summarized_detailed_delivery_data['ddd_last_pod_10'] == summarized_detailed_delivery_data['ddd_last_pod_9'], pd.NaT, summarized_detailed_delivery_data['ddd_last_pod_9'].astype(str)))
    summarized_detailed_delivery_data['ddd_last_pod_8'] = pd.to_datetime(np.where(summarized_detailed_delivery_data['ddd_last_pod_9'] == summarized_detailed_delivery_data['ddd_last_pod_8'], pd.NaT, summarized_detailed_delivery_data['ddd_last_pod_8'].astype(str)))
    summarized_detailed_delivery_data['ddd_last_pod_7'] = pd.to_datetime(np.where(summarized_detailed_delivery_data['ddd_last_pod_8'] == summarized_detailed_delivery_data['ddd_last_pod_7'], pd.NaT, summarized_detailed_delivery_data['ddd_last_pod_7'].astype(str)))
    summarized_detailed_delivery_data['ddd_last_pod_6'] = pd.to_datetime(np.where(summarized_detailed_delivery_data['ddd_last_pod_7'] == summarized_detailed_delivery_data['ddd_last_pod_6'], pd.NaT, summarized_detailed_delivery_data['ddd_last_pod_6'].astype(str)))
    summarized_detailed_delivery_data['ddd_last_pod_5'] = pd.to_datetime(np.where(summarized_detailed_delivery_data['ddd_last_pod_6'] == summarized_detailed_delivery_data['ddd_last_pod_5'], pd.NaT, summarized_detailed_delivery_data['ddd_last_pod_5'].astype(str)))
    summarized_detailed_delivery_data['ddd_last_pod_4'] = pd.to_datetime(np.where(summarized_detailed_delivery_data['ddd_last_pod_5'] == summarized_detailed_delivery_data['ddd_last_pod_4'], pd.NaT, summarized_detailed_delivery_data['ddd_last_pod_4'].astype(str)))
    summarized_detailed_delivery_data['ddd_last_pod_3'] = pd.to_datetime(np.where(summarized_detailed_delivery_data['ddd_last_pod_4'] == summarized_detailed_delivery_data['ddd_last_pod_3'], pd.NaT, summarized_detailed_delivery_data['ddd_last_pod_3'].astype(str)))
    summarized_detailed_delivery_data['ddd_last_pod_2'] = pd.to_datetime(np.where(summarized_detailed_delivery_data['ddd_last_pod_3'] == summarized_detailed_delivery_data['ddd_last_pod_2'], pd.NaT, summarized_detailed_delivery_data['ddd_last_pod_2'].astype(str)))

    summarized_detailed_delivery_data.set_index(['final_name', 'date'], inplace=True)

    cov_driver_hours = cov_driver_hours.join(summarized_detailed_delivery_data, on=['final_name', 'date'])

    summarized_delivery_data_sums = summarized_delivery_data.groupby(by=['final_name', 'date'])[['sdd_packages', 'sdd_stops', 'sdd_service_amount', 'sdd_amount_charged', 'sdd_signature_time_deduction', 'sdd_signature_time_deductions_taken']].sum()

    cov_driver_hours = cov_driver_hours.join(summarized_delivery_data_sums, on=['final_name', 'date'])

    hours_group = cov_driver_hours.groupby(by=['final_name', 'date'])[['clock_in', 'clock_out', 'total_hours', 'reg_hours', 'ot_hours', 'dt_hours', 'lunch_start', 'final_lunch_time']].agg({'clock_in' : 'min', 'clock_out' : 'max', 'total_hours' : ['sum', 'max'], 'reg_hours' : 'sum', 'ot_hours' : 'sum', 'dt_hours' : 'sum', 'lunch_start' : 'count', 'final_lunch_time' : 'sum'})
    hours_group.columns = hours_group.columns.to_flat_index()
    hours_group.rename(columns={('clock_in', 'min'):'first_clock_in', ('clock_out', 'max'):'last_clock_out', ('total_hours', 'sum') : 'total_hours_sum', ('total_hours', 'max') : 'total_hours_max', ('reg_hours', 'sum') : 'reg_hours_sum', ('ot_hours', 'sum') : 'ot_hours_sum', ('dt_hours', 'sum') : 'dt_hours_sum', ('lunch_start', 'count') : 'lunch_start_count', ('final_lunch_time', 'sum') : 'final_lunch_time_sum'}, inplace=True)

    cov_driver_hours = cov_driver_hours.join(hours_group, on=['final_name', 'date'])

    daily_driver_code = detailed_delivery_data.groupby(['final_name', 'date'])['driver_code'].max()
    daily_driver_code.name = "driver_code"
    cov_driver_hours = cov_driver_hours.join(daily_driver_code, on=['final_name', 'date'])

    weekly_driver_code = summarized_delivery_data.loc[:, ['final_name', 'date', 'driver_code']]
    weekly_driver_code.rename(columns={'driver_code':'weekly_driver_code'}, inplace=True)
    b =  weekly_driver_code['date'] + pd.offsets.Week(weekday=5)
    m2 = weekly_driver_code['date'] != (b - pd.offsets.Week())
    weekly_driver_code['week_ending'] = weekly_driver_code['date'].mask(m2, b)
    weekly_driver_code = weekly_driver_code.drop(columns=['date']).drop_duplicates(subset=['final_name', 'week_ending']).set_index(keys=['final_name', 'week_ending'])
    cov_driver_hours = cov_driver_hours.join(weekly_driver_code, on=['final_name', 'week_ending'])

    cov_driver_hours['driver_code'].fillna(value=cov_driver_hours['weekly_driver_code'], inplace=True)
    cov_driver_hours['driver_code'].fillna(value=cov_driver_hours['hub'], inplace=True)

    hub_info = pd.DataFrame({
        'hub' : ['BAY', 'CAJ', 'CAJ/ONT', 'CNG', 'CST', 'EST', 'FAT', 'FAT/VIS', 'ING', 'LAN', 'MOV', 'NSD', 'ONT', 'ONT/CAJ', 'ONX', 'PMF', 'SFS', 'SJC', 'SND', 'TUS', 'VIS', 'VLY'],
        'rate_for_hub' : [0, 0, 0, 1.55, 0, 1.6, 0, 0, 1.55, 1.55, 0, 1.6, 1.6, 0, 1.55, 1.6, 1.6, 0, 1.6, 1.6, 0, 1.55, ]
    }).set_index(keys='hub')

    cov_driver_hours = cov_driver_hours.join(hub_info, on=[cov_driver_hours['driver_code'].str[:3]])
    cov_driver_hours = cov_driver_hours.drop(columns=['key_0'])

    cov_driver_hours['rate_for_hub'].fillna(value=0.0, inplace=True)
    cov_driver_hours['pay_type'] = np.where(((cov_driver_hours['rate_for_hub'] > 0) & (cov_driver_hours['dep_no'] == "55")), "PS", "PRODUCTIVITY")

    production_pay_summary = detailed_delivery_data.groupby(['final_name', 'date'])[['final_stop_count', 'stop_count_old_model', 'full_stop_rate', 'vape_stops', 'discounted_stops', 'amount_charged', 'package_count']].sum()

    production_pay_summary = cov_driver_hours.loc[:, ['final_name', 'date', 'rate_for_hub']].drop_duplicates().join(production_pay_summary, on=['final_name', 'date']).set_index(keys=['final_name', 'date'])

    production_pay_summary['total_production_pay'] = ((production_pay_summary['rate_for_hub'] * production_pay_summary['full_stop_rate']) + ((production_pay_summary['rate_for_hub'] + 1) * production_pay_summary['vape_stops']) + ((production_pay_summary['rate_for_hub'] * 0.5).apply(lambda x: round_up(x, 2)) * production_pay_summary['discounted_stops'])).apply(lambda x: round_half_up(x, "0.01"))

    production_pay_summary.drop(columns=['rate_for_hub'], inplace=True)

    cov_driver_hours = cov_driver_hours.join(production_pay_summary, on=['final_name', 'date'])

    supplemental_pay = pd.DataFrame({
        'route' : ['SND821', 'SND808', 'NSD809', 'SND836', 'EST552', 'VLY100', 'VLY109', 'ONX130', 'ONX139', 'LAN202', 'EST500'],
        'coverage_area' : ['El Cajon', 'Carmel Valley', 'Rancho Santa Fe', 'El Cajon / Jamul', '', '', '', '', '', '', ''],
        'route_supplemental_pay' : [10.00, 5.00, 12.50, 10.00, 10.00, 5.00, 5.00, 10.00, 5.00, 15.00, 5.00]
    })

    supplemental_pay = supplemental_pay.join(cov_driver_hours.loc[:,['final_name', 'driver_code']].drop_duplicates().set_index(keys='driver_code'), on='route')
    supplemental_pay['final_name'] = supplemental_pay['final_name'].fillna(value="Missing final_name")

    cov_driver_hours['total_applicable_hours'] = cov_driver_hours['reg_hours'] + cov_driver_hours['ot_hours'] + cov_driver_hours['dt_hours']

    multiline_group = cov_driver_hours.groupby(by=['final_name', 'date'])['clock_in'].count()
    multiline_group.name = "multiline"

    cov_driver_hours = cov_driver_hours.join(multiline_group, on=['final_name', 'date'])

    cov_driver_hours['stop_count'] = (cov_driver_hours['sdd_stops'].apply(lambda x: round_half_up(x, "0.01")) / cov_driver_hours['multiline']).fillna(0).apply(lambda x: round_half_up(x, "0.01"))
    cov_driver_hours['final_stop_count'] = (cov_driver_hours['final_stop_count'].apply(lambda x: round_half_up(x, "0.01")) / cov_driver_hours['multiline']).fillna(0)
    cov_driver_hours['stop_count_old_model'] = (cov_driver_hours['stop_count_old_model'].apply(lambda x: round_half_up(x, "0.01")) / cov_driver_hours['multiline']).fillna(0)
    cov_driver_hours['vape_stops'] = (cov_driver_hours['vape_stops'].apply(lambda x: round_half_up(x, "0.01")) / cov_driver_hours['multiline']).fillna(0)
    cov_driver_hours['discounted_stops'] = (cov_driver_hours['discounted_stops'].apply(lambda x: round_half_up(x, "0.01")) / cov_driver_hours['multiline']).fillna(0)
    cov_driver_hours['amount_charged'] = (cov_driver_hours['amount_charged'].apply(lambda x: round_half_up(x, "0.01")) / cov_driver_hours['multiline']).fillna(0).apply(lambda x: round_half_up(x, "0.01"))
    cov_driver_hours['package_count'] = (cov_driver_hours['package_count'].apply(lambda x: round_half_up(x, "0.01")) / cov_driver_hours['multiline']).fillna(0).apply(lambda x: round_half_up(x, "0.01"))
    cov_driver_hours['sdd_packages'] = (cov_driver_hours['sdd_packages'].apply(lambda x: round_half_up(x, "0.01")) / cov_driver_hours['multiline']).fillna(0).apply(lambda x: round_half_up(x, "0.01"))
    cov_driver_hours['sdd_amount_charged'] = (cov_driver_hours['sdd_amount_charged'].apply(lambda x: round_half_up(x, "0.01")) / cov_driver_hours['multiline']).fillna(0).apply(lambda x: round_half_up(x, "0.01"))
    cov_driver_hours['sdd_signature_time_deduction'] = (cov_driver_hours['sdd_signature_time_deduction'].apply(lambda x: round_half_up(x, "0.01")) / cov_driver_hours['multiline'].apply(lambda x: round_half_up(x, "0.01"))).fillna(0).apply(lambda x: round_half_up(x, "0.01"))
    cov_driver_hours['sdd_signature_time_deductions_taken'] = (cov_driver_hours['sdd_signature_time_deductions_taken'].apply(lambda x: round_half_up(x, "0.01")) / cov_driver_hours['multiline']).fillna(0).apply(lambda x: round_half_up(x, "0.01"))

    min_lunch_group = cov_driver_hours.loc[(cov_driver_hours['adj_lunch_start'].notna()) & (cov_driver_hours['final_lunch_time'] > 0)].groupby(by=['final_name', 'date'])[['adj_lunch_start', 'adj_lunch_end']].min()
    min_lunch_group.rename(columns={'adj_lunch_start' : 'min_starting_lunch_time', 'adj_lunch_end' : 'min_ending_lunch_time'}, inplace=True)

    cov_driver_hours = cov_driver_hours.join(min_lunch_group, on=['final_name', 'date'])

    cov_driver_hours['min_ending_lunch_time'] = np.where(((cov_driver_hours['final_lunch_time'] == 1) & (cov_driver_hours['multiline'] == 1) & ((((cov_driver_hours['clock_out'] - cov_driver_hours['lunch_start']) / np.timedelta64(1, 'h')).apply(lambda x: round_half_up(x, "0.01")) == 0) | (cov_driver_hours['lunch_start'].isna()))), cov_driver_hours['min_starting_lunch_time'] + pd.Timedelta(minutes=30), cov_driver_hours['min_ending_lunch_time'])

    conditions = [((cov_driver_hours['adj_lunch_start'].notna()) & (cov_driver_hours['multiline'] > 1) & (cov_driver_hours['final_lunch_time'] > 0)), cov_driver_hours['adj_lunch_start'].notna()]
    choices = [cov_driver_hours['adj_lunch_start'], cov_driver_hours['min_starting_lunch_time']]
    cov_driver_hours['first_lunch_starting_time'] = pd.Series(np.select(conditions, choices, default=pd.NaT), dtype='datetime64[ns]')

    conditions = [((cov_driver_hours['adj_lunch_end'].notna()) & (cov_driver_hours['multiline'] > 1) & (cov_driver_hours['final_lunch_time'] > 0)), cov_driver_hours['adj_lunch_end'].notna()]
    choices = [cov_driver_hours['adj_lunch_end'], cov_driver_hours['min_ending_lunch_time']]
    cov_driver_hours['first_lunch_ending_time'] = pd.Series(np.select(conditions, choices, default=pd.NaT), dtype='datetime64[ns]')

    cov_driver_hours['second_lunch_starting_time'] = pd.Series(np.where(((cov_driver_hours['final_lunch_time'] == 1) & (cov_driver_hours['multiline'] == 1) & ((((cov_driver_hours['clock_out'] - cov_driver_hours['lunch_start'].replace(to_replace=pd.NaT, value=datetime(1900, 1, 1, 1, 0, 0, 0))) / np.timedelta64(1, 'h')).apply(lambda x: round_half_up(x, "0.01")) == 0) | (cov_driver_hours['lunch_start'].isna()))), (cov_driver_hours['clock_in'] + timedelta(0, 0, 0, 0, 29, 10, 0)).astype(str), pd.NaT), dtype='datetime64[ns]')

    cov_driver_hours['second_lunch_ending_time'] = pd.Series(np.where((cov_driver_hours['second_lunch_starting_time'].isna()), pd.NaT, (cov_driver_hours['second_lunch_starting_time'] + timedelta(0, 0, 0, 0, 30, 0, 0)).astype(str)), dtype='datetime64[ns]')

    cov_driver_hours['time_of_first_lunch'] = pd.Series(np.where(cov_driver_hours['second_lunch_starting_time'].isna(), cov_driver_hours['final_lunch_time'], ((cov_driver_hours['first_lunch_ending_time'] - cov_driver_hours['first_lunch_starting_time']) / np.timedelta64(1, 'h')))).apply(lambda x: round_half_up(x, "0.01"))

    cov_driver_hours['time_of_second_lunch'] = ((cov_driver_hours['second_lunch_ending_time'] - cov_driver_hours['second_lunch_starting_time']) / np.timedelta64(1, 'h')).fillna(0).apply(lambda x: round_half_up(x, "0.01"))

    cov_driver_hours['first_pod'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['ddd_first_pod_1'], cov_driver_hours['sdd_first_pod_1'])

    conditions = [cov_driver_hours['pay_type'] == "PS", cov_driver_hours['sdd_first_pod_2'] >= cov_driver_hours['first_clock_in'], cov_driver_hours['sdd_first_pod_3'] >= cov_driver_hours['first_clock_in'], cov_driver_hours['sdd_first_pod_4'] >= cov_driver_hours['first_clock_in'], cov_driver_hours['sdd_first_pod_5'] >= cov_driver_hours['first_clock_in'], cov_driver_hours['sdd_first_pod_6'] >= cov_driver_hours['first_clock_in'], cov_driver_hours['sdd_first_pod_7'] >= cov_driver_hours['first_clock_in'], cov_driver_hours['sdd_first_pod_8'] >= cov_driver_hours['first_clock_in'], cov_driver_hours['sdd_first_pod_9'] >= cov_driver_hours['first_clock_in'], cov_driver_hours['sdd_first_pod_10'] >= cov_driver_hours['first_clock_in']]
    choices = [cov_driver_hours['ddd_first_pod_2'], cov_driver_hours['sdd_first_pod_2'], cov_driver_hours['sdd_first_pod_3'], cov_driver_hours['sdd_first_pod_4'], cov_driver_hours['sdd_first_pod_5'], cov_driver_hours['sdd_first_pod_6'], cov_driver_hours['sdd_first_pod_7'], cov_driver_hours['sdd_first_pod_8'], cov_driver_hours['sdd_first_pod_9'], cov_driver_hours['sdd_first_pod_10']]
    cov_driver_hours['second_pod'] = pd.Series(np.select(conditions, choices, default=pd.NaT), dtype='datetime64[ns]')

    cov_driver_hours = cov_driver_hours.join(detailed_delivery_data.loc[:, ['final_name', 'date', 'delivery_time', 'pod_name']].drop_duplicates(subset=['final_name', 'date', 'delivery_time']).set_index(keys=['final_name', 'date', 'delivery_time']), on=['final_name', 'date', 'first_pod']).rename(columns={'pod_name' : 'first_pod_name'})

    cov_driver_hours['last_pod'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['ddd_last_pod_1'], cov_driver_hours['sdd_last_pod_1'])

    conditions = [cov_driver_hours['first_pod'] == cov_driver_hours['last_pod'], cov_driver_hours['last_pod'] > cov_driver_hours['last_clock_out']]
    choices = [pd.NaT, cov_driver_hours['last_clock_out']]
    cov_driver_hours['last_pod_to_use'] = pd.Series(np.select(conditions, choices, default=cov_driver_hours['last_pod']), dtype='datetime64[ns]')
    cov_driver_hours['last_pod_to_use_wo_seconds'] = cov_driver_hours['last_pod_to_use'].dt.floor('Min')

    comm_list = detailed_delivery_data.loc[:, ['final_name', 'date', 'delivery_time']]

    comm_list = comm_list.join(cov_driver_hours.loc[:, ['final_name', 'date', 'last_pod_to_use']].set_index(keys=['final_name', 'date']), on=['final_name', 'date'])
    comm_list = comm_list.loc[comm_list['delivery_time'] < comm_list['last_pod_to_use']]

    comm_list = comm_list.drop_duplicates().groupby(by=['final_name', 'date'])['delivery_time']

    second_to_last_pod = comm_list.nlargest(n=1).groupby(level=['final_name', 'date']).last().rename("second_to_last_pod_ps")
    cov_driver_hours = cov_driver_hours.join(second_to_last_pod, on=['final_name', 'date'])

    conditions = [
        cov_driver_hours['sdd_last_pod_2'] >= cov_driver_hours['first_clock_in'],
        cov_driver_hours['sdd_last_pod_3'] >= cov_driver_hours['first_clock_in'],
        cov_driver_hours['sdd_last_pod_4'] >= cov_driver_hours['first_clock_in'],
        cov_driver_hours['sdd_last_pod_5'] >= cov_driver_hours['first_clock_in'],
        cov_driver_hours['sdd_last_pod_6'] >= cov_driver_hours['first_clock_in'],
        cov_driver_hours['sdd_last_pod_7'] >= cov_driver_hours['first_clock_in'],
        cov_driver_hours['sdd_last_pod_8'] >= cov_driver_hours['first_clock_in'],
        cov_driver_hours['sdd_last_pod_9'] >= cov_driver_hours['first_clock_in']
    ]
    choices = [
        cov_driver_hours['sdd_last_pod_2'],
        cov_driver_hours['sdd_last_pod_3'],
        cov_driver_hours['sdd_last_pod_4'],
        cov_driver_hours['sdd_last_pod_5'],
        cov_driver_hours['sdd_last_pod_6'],
        cov_driver_hours['sdd_last_pod_7'],
        cov_driver_hours['sdd_last_pod_8'],
        cov_driver_hours['sdd_last_pod_9'],
    ]
    cov_driver_hours['second_to_last_pod_prod'] = pd.Series(np.select(conditions, choices, default=cov_driver_hours['sdd_last_pod_10']), dtype='datetime64[ns]')

    cov_driver_hours['second_to_last_pod'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['second_to_last_pod_ps'], cov_driver_hours['second_to_last_pod_prod'])

    comm_list = detailed_delivery_data.loc[:, ['final_name', 'date', 'delivery_time']]

    comm_list = comm_list.join(cov_driver_hours.loc[:, ['final_name', 'date', 'min_ending_lunch_time']].set_index(keys=['final_name', 'date']), on=['final_name', 'date'])
    comm_list['min_ending_lunch_time'] = comm_list['min_ending_lunch_time'].dt.floor('Min')
    comm_list = comm_list.loc[comm_list['delivery_time'] > comm_list['min_ending_lunch_time']]

    comm_list = comm_list.drop_duplicates().groupby(by=['final_name', 'date'])['delivery_time']

    min_post_lunch_deliver_time = comm_list.nsmallest(n=1).groupby(level=['final_name', 'date']).last().rename("min_post_lunch_deliver_time")
    cov_driver_hours = cov_driver_hours.join(min_post_lunch_deliver_time, on=['final_name', 'date'])

    comm_list = detailed_delivery_data.loc[:, ['final_name', 'date', 'delivery_time']]

    comm_list = comm_list.join(cov_driver_hours.loc[:, ['final_name', 'date', 'first_clock_in']].set_index(keys=['final_name', 'date']), on=['final_name', 'date'])
    comm_list = comm_list.loc[comm_list['delivery_time'] > comm_list['first_clock_in']]

    comm_list = comm_list.drop_duplicates().groupby(by=['final_name', 'date'])['delivery_time']

    min_first_clock_in_deliver_time = comm_list.nsmallest(n=1).groupby(level=['final_name', 'date']).last().rename("min_first_clock_in_deliver_time")
    cov_driver_hours = cov_driver_hours.join(min_first_clock_in_deliver_time, on=['final_name', 'date'])

    conditions = [
        (cov_driver_hours['first_pod'] == cov_driver_hours['last_pod']) | ((cov_driver_hours['first_pod'].isna()) & (cov_driver_hours['last_pod'].isna())),
        ((cov_driver_hours['first_pod'] > cov_driver_hours['min_starting_lunch_time']) & (cov_driver_hours['first_pod'] < cov_driver_hours['min_ending_lunch_time']) & (cov_driver_hours['pay_type'] == "PS")),
        ((cov_driver_hours['first_pod'] > cov_driver_hours['min_starting_lunch_time']) & (cov_driver_hours['first_pod'] < cov_driver_hours['min_ending_lunch_time'])),
        ((((cov_driver_hours['second_pod'] - cov_driver_hours['first_pod']) / np.timedelta64(1, 'h')).apply(lambda x: round_half_up(x, "0.01")) >= 1) | ((((cov_driver_hours['second_pod'] - cov_driver_hours['first_pod']) / np.timedelta64(1, 'h')).apply(lambda x: round_half_up(x, "0.01")) >= 0.65) & (cov_driver_hours['first_pod_name'] == "86"))),
        cov_driver_hours['first_pod'] >= cov_driver_hours['first_clock_in']
    ]
    choices = [
        pd.NaT,
        cov_driver_hours['min_post_lunch_deliver_time'],
        cov_driver_hours['second_pod'],
        cov_driver_hours['second_pod'],
        cov_driver_hours['first_pod']
    ]
    cov_driver_hours['first_pod_to_use'] = pd.Series(np.select(conditions, choices, default=cov_driver_hours['min_first_clock_in_deliver_time']), dtype='datetime64[ns]')

    conditions = [
        cov_driver_hours['clock_out'] < cov_driver_hours['first_pod_to_use'],
        ((cov_driver_hours['sorting_end'].notna()) & (cov_driver_hours['sorting_end'] < cov_driver_hours['first_pod_to_use']))
    ]
    choices = [
        cov_driver_hours['reg_hours'] + cov_driver_hours['ot_hours'] + cov_driver_hours['dt_hours'],
        (cov_driver_hours['sorting_end'] - cov_driver_hours['sorting_start']) / np.timedelta64(1, 'h')
    ]
    cov_driver_hours['sort_time'] = np.select(conditions, choices, default=0)

    curr_name = ""
    curr_date = datetime.min

    for row_number, row in cov_driver_hours.iterrows():
        
        if ((curr_name != cov_driver_hours.loc[row_number, 'final_name']) | (curr_date != cov_driver_hours.loc[row_number, 'date'])):
            curr_name = cov_driver_hours.loc[row_number, 'final_name']
            curr_date = cov_driver_hours.loc[row_number, 'date']
            running_break_time = 0
        
        running_break_time += cov_driver_hours.loc[row_number, 'total_applicable_hours']
        cov_driver_hours.loc[row_number, 'running_break_time'] = running_break_time
        
    curr_name = ""
    curr_date = datetime.min
    running_partial_breaks_for_day = Decimal(0.0)

    for row_number, row in cov_driver_hours.iterrows():
        
        if ((curr_name != cov_driver_hours.loc[row_number, 'final_name']) | (curr_date != cov_driver_hours.loc[row_number, 'date'])):
            curr_name = cov_driver_hours.loc[row_number, 'final_name']
            curr_date = cov_driver_hours.loc[row_number, 'date']
            running_partial_breaks_for_day = Decimal(0.0)

        if (cov_driver_hours.loc[row_number, 'running_break_time'] < 2.0):
            cov_driver_hours.loc[row_number, 'total_breaks_for_day'] = 0
        elif ((cov_driver_hours.loc[row_number, 'multiline'] == 1) & (cov_driver_hours.loc[row_number, 'total_applicable_hours'] >= 2.0)):
            cov_driver_hours.loc[row_number, 'total_breaks_for_day'] = (Decimal(cov_driver_hours.loc[row_number, 'total_applicable_hours']).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) / 4).quantize(Decimal('1'), rounding=ROUND_HALF_UP)
        else:
            cov_driver_hours.loc[row_number, 'total_breaks_for_day'] = (Decimal(cov_driver_hours.loc[row_number, 'running_break_time']).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) / 4).quantize(Decimal('1'), rounding=ROUND_HALF_UP) - running_partial_breaks_for_day
        
        running_partial_breaks_for_day += Decimal(cov_driver_hours.loc[row_number, 'total_breaks_for_day'])

    cov_driver_hours['total_breaks_for_day'] = cov_driver_hours['total_breaks_for_day'].astype('float64')
        
    cov_driver_hours['total_break_time'] = (cov_driver_hours['total_breaks_for_day'] * (1/6)).apply(lambda x: round_half_up(x, "0.01"))

    full_stop_group = cov_driver_hours.loc[(cov_driver_hours['full_stop_rate'] > 1), ['final_name', 'date', 'full_stop_rate']].drop_duplicates().groupby(by=['final_name', 'date'])['full_stop_rate'].sum()
    full_stop_group.name = "daily_full_stop_rate"

    cov_driver_hours = cov_driver_hours.join(full_stop_group, on=['final_name', 'date'])

    cov_driver_hours['full_stop_rate'] = (cov_driver_hours['daily_full_stop_rate'] / cov_driver_hours['multiline']).replace([np.inf, -np.inf, np.nan], 0.0)

    cov_driver_hours = cov_driver_hours.join(supplemental_pay.loc[:, ['final_name', 'route_supplemental_pay']].set_index(keys='final_name'), on='final_name')
    cov_driver_hours['route_supplemental_pay'].fillna(value=0, inplace=True)

    cov_driver_hours['total_production_pay'] = cov_driver_hours['total_production_pay'] + np.where(cov_driver_hours['full_stop_rate'] > 1, cov_driver_hours['route_supplemental_pay'], 0)

    cov_driver_hours['total_break_time_non_production'] = np.where(((cov_driver_hours['total_production_pay'].isna()) | (cov_driver_hours['total_production_pay'] <= cov_driver_hours['rate_for_hub'])), cov_driver_hours['total_break_time'], 0)

    cov_driver_hours['total_break_time_production'] = cov_driver_hours['total_break_time'] - cov_driver_hours['total_break_time_non_production']

    sorting_subfunction_count = detailed_delivery_data.loc[:, ['final_name', 'date', 'delivery_time']].join(cov_driver_hours.loc[:, ['final_name', 'date', 'sorting_start', 'sorting_end']].set_index(keys=['final_name', 'date']), on=['final_name', 'date'])

    sorting_subfunction_count = sorting_subfunction_count.loc[(sorting_subfunction_count['sorting_start'].notna()) & ((sorting_subfunction_count['delivery_time'] >= sorting_subfunction_count['sorting_start']) & (sorting_subfunction_count['delivery_time'] <= sorting_subfunction_count['sorting_end']))].groupby(by=['final_name', 'date', 'sorting_start'])['delivery_time'].count()
    sorting_subfunction_count.name = "sorting_subfunction_count"

    cov_driver_hours = cov_driver_hours.join(sorting_subfunction_count, on=['final_name', 'date', 'sorting_start'])
    cov_driver_hours['sorting_subfunction_count'].fillna(value=0, inplace=True)

    conditions = [
        ((cov_driver_hours['sorting_time'] == 0) | (cov_driver_hours['pay_type'] == "PRODUCTIVITY")),
        (cov_driver_hours['sorting_subfunction_count'] > 0),
        ((cov_driver_hours['sorting_start'] >= cov_driver_hours['first_pod_to_use']) & (cov_driver_hours['sorting_end'] <= cov_driver_hours['last_pod_to_use']))
    ]
    choices = [
        0,
        0,
        cov_driver_hours['sorting_time']
    ]
    cov_driver_hours['sorting_time_reduction'] = np.select(conditions, choices, default=0)

    pickup_dropoff_subfunction_count = detailed_delivery_data.loc[:, ['final_name', 'date', 'delivery_time']].join(cov_driver_hours.loc[:, ['final_name', 'date', 'pickup_dropoff_start', 'pickup_dropoff_end']].set_index(keys=['final_name', 'date']), on=['final_name', 'date'])

    pickup_dropoff_subfunction_count = pickup_dropoff_subfunction_count.loc[(pickup_dropoff_subfunction_count['pickup_dropoff_start'].notna()) & ((pickup_dropoff_subfunction_count['delivery_time'] >= pickup_dropoff_subfunction_count['pickup_dropoff_start']) & (pickup_dropoff_subfunction_count['delivery_time'] <= pickup_dropoff_subfunction_count['pickup_dropoff_end']))].groupby(by=['final_name', 'date', 'pickup_dropoff_start'])['delivery_time'].count()
    pickup_dropoff_subfunction_count.name = "pickup_dropoff_subfunction_count"

    cov_driver_hours = cov_driver_hours.join(pickup_dropoff_subfunction_count, on=['final_name', 'date', 'pickup_dropoff_start'])
    cov_driver_hours['pickup_dropoff_subfunction_count'].fillna(value=0, inplace=True)

    conditions = [
        ((cov_driver_hours['pickup_dropoff_time'] == 0) | (cov_driver_hours['pay_type'] == "PRODUCTIVITY")),
        (cov_driver_hours['pickup_dropoff_subfunction_count'] > 0),
        ((cov_driver_hours['pickup_dropoff_start'] >= cov_driver_hours['first_pod_to_use']) & (cov_driver_hours['pickup_dropoff_end'] <= cov_driver_hours['last_pod_to_use']))
    ]
    choices = [
        0,
        0,
        cov_driver_hours['pickup_dropoff_time']
    ]
    cov_driver_hours['pickup_dropoff_time_reduction'] = np.select(conditions, choices, default=0)

    line_haul_subfunction_count = detailed_delivery_data.loc[:, ['final_name', 'date', 'delivery_time']].join(cov_driver_hours.loc[:, ['final_name', 'date', 'line_haul_start', 'line_haul_end']].set_index(keys=['final_name', 'date']), on=['final_name', 'date'])

    line_haul_subfunction_count = line_haul_subfunction_count.loc[(line_haul_subfunction_count['line_haul_start'].notna()) & ((line_haul_subfunction_count['delivery_time'] >= line_haul_subfunction_count['line_haul_start']) & (line_haul_subfunction_count['delivery_time'] <= line_haul_subfunction_count['line_haul_end']))].groupby(by=['final_name', 'date', 'line_haul_start'])['delivery_time'].count()
    line_haul_subfunction_count.name = "line_haul_subfunction_count"

    cov_driver_hours = cov_driver_hours.join(line_haul_subfunction_count, on=['final_name', 'date', 'line_haul_start'])
    cov_driver_hours['line_haul_subfunction_count'].fillna(value=0, inplace=True)

    conditions = [
        ((cov_driver_hours['line_haul_time'] == 0) | (cov_driver_hours['pay_type'] == "PRODUCTIVITY")),
        (cov_driver_hours['line_haul_subfunction_count'] > 0),
        ((cov_driver_hours['line_haul_start'] >= cov_driver_hours['first_pod_to_use']) & (cov_driver_hours['line_haul_end'] <= cov_driver_hours['last_pod_to_use']))
    ]
    choices = [
        0,
        0,
        cov_driver_hours['line_haul_time']
    ]
    cov_driver_hours['line_haul_time_reduction'] = np.select(conditions, choices, default=0)

    vehicle_maintenance_subfunction_count = detailed_delivery_data.loc[:, ['final_name', 'date', 'delivery_time']].join(cov_driver_hours.loc[:, ['final_name', 'date', 'vehicle_maintenance_start', 'vehicle_maintenance_end']].set_index(keys=['final_name', 'date']), on=['final_name', 'date'])

    vehicle_maintenance_subfunction_count = vehicle_maintenance_subfunction_count.loc[(vehicle_maintenance_subfunction_count['vehicle_maintenance_start'].notna()) & ((vehicle_maintenance_subfunction_count['delivery_time'] >= vehicle_maintenance_subfunction_count['vehicle_maintenance_start']) & (vehicle_maintenance_subfunction_count['delivery_time'] <= vehicle_maintenance_subfunction_count['vehicle_maintenance_end']))].groupby(by=['final_name', 'date', 'vehicle_maintenance_start'])['delivery_time'].count()
    vehicle_maintenance_subfunction_count.name = "vehicle_maintenance_subfunction_count"

    cov_driver_hours = cov_driver_hours.join(vehicle_maintenance_subfunction_count, on=['final_name', 'date', 'vehicle_maintenance_start'])
    cov_driver_hours['vehicle_maintenance_subfunction_count'].fillna(value=0, inplace=True)

    conditions = [
        ((cov_driver_hours['vehicle_maintenance_time'] == 0) | (cov_driver_hours['pay_type'] == "PRODUCTIVITY")),
        (cov_driver_hours['vehicle_maintenance_subfunction_count'] > 0),
        ((cov_driver_hours['vehicle_maintenance_start'] >= cov_driver_hours['first_pod_to_use']) & (cov_driver_hours['vehicle_maintenance_end'] <= cov_driver_hours['last_pod_to_use']))
    ]
    choices = [
        0,
        0,
        cov_driver_hours['vehicle_maintenance_time']
    ]
    cov_driver_hours['vehicle_maintenance_time_reduction'] = np.select(conditions, choices, default=0)

    conditions = [
        ((cov_driver_hours['total_hours'] == 0) | (cov_driver_hours['first_pod_to_use'].isna()) | (cov_driver_hours['first_pod_to_use'] >= cov_driver_hours['clock_out']) | (cov_driver_hours['clock_in'] >= cov_driver_hours['last_pod_to_use'])),
        (cov_driver_hours['first_pod_to_use'] >= cov_driver_hours['clock_in'])
    ]
    choices = [
        0,
        ((cov_driver_hours.loc[:, ['last_pod_to_use', 'clock_out']].min(axis=1) - cov_driver_hours['first_pod_to_use']) / np.timedelta64(1, 'h'))
    ]
    cov_driver_hours['delivery_time_first_part'] = pd.Series(np.select(conditions, choices, default=((cov_driver_hours['last_pod_to_use'] - cov_driver_hours.loc[:, ['first_pod_to_use', 'clock_in']].max(axis=1)) / np.timedelta64(1, 'h')))).apply(lambda x: round_half_up(x, "0.01"))

    conditions = [
        ((cov_driver_hours['first_lunch_starting_time'].isna()) | (cov_driver_hours['time_of_first_lunch'] == 0) | (cov_driver_hours['first_pod_to_use'].isna()) | (cov_driver_hours['first_pod_to_use'] >= cov_driver_hours['clock_out']) | (cov_driver_hours['clock_in'] >= cov_driver_hours['last_pod_to_use'])),
        ((cov_driver_hours['first_lunch_starting_time'] >= cov_driver_hours['first_pod_to_use']) & (cov_driver_hours['first_lunch_ending_time'] <= cov_driver_hours['last_pod_to_use']) & (cov_driver_hours['time_of_second_lunch'] == 0)),
        ((cov_driver_hours['first_lunch_starting_time'] >= cov_driver_hours['first_pod_to_use']) & (cov_driver_hours['second_lunch_ending_time'] <= cov_driver_hours['last_pod_to_use']) & (cov_driver_hours['time_of_second_lunch'] > 0)),
        ((cov_driver_hours['first_lunch_starting_time'] > cov_driver_hours['first_pod_to_use']) & (cov_driver_hours['first_lunch_starting_time'] < cov_driver_hours['last_pod_to_use']))
    ]
    choices = [
        0,
        cov_driver_hours['time_of_first_lunch'],
        cov_driver_hours['time_of_first_lunch'] + cov_driver_hours['time_of_second_lunch'],
        ((cov_driver_hours.loc[:, ['last_pod_to_use', 'first_lunch_ending_time']].min(axis=1) - cov_driver_hours['first_lunch_starting_time']) / np.timedelta64(1, 'h'))
    ]
    cov_driver_hours['delivery_time_second_part'] = pd.Series(np.select(conditions, choices, default=0)).apply(lambda x: round_half_up(x, "0.01"))

    conditions = [
        ((cov_driver_hours['first_lunch_starting_time'] >= cov_driver_hours['first_pod_to_use']) & (cov_driver_hours['second_lunch_ending_time'] <= cov_driver_hours['last_pod_to_use']) & (cov_driver_hours['time_of_second_lunch'] > 0)),
        ((cov_driver_hours['time_of_second_lunch'] > 0) & (cov_driver_hours['second_lunch_starting_time'] > cov_driver_hours['first_pod_to_use']) & (cov_driver_hours['second_lunch_starting_time'] < cov_driver_hours['last_pod_to_use']))
    ]
    choices = [
        0,
        ((cov_driver_hours.loc[:, ['last_pod_to_use', 'second_lunch_ending_time']].min(axis=1) - cov_driver_hours['second_lunch_starting_time']) / np.timedelta64(1, 'h'))
    ]
    cov_driver_hours['delivery_time_third_part'] = pd.Series(np.select(conditions, choices, default=0)).apply(lambda x: round_half_up(x, "0.01"))

    cov_driver_hours['delivery_time'] = cov_driver_hours['delivery_time_first_part'] - cov_driver_hours['delivery_time_second_part'] - cov_driver_hours['delivery_time_third_part']

    cov_driver_hours['combined_stops'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['final_stop_count'], cov_driver_hours['stop_count'])

    conditions = [
        ((cov_driver_hours['combined_stops'] <= 1) | (cov_driver_hours['total_hours'] == 0) | (cov_driver_hours['last_pod_to_use_wo_seconds'] >= cov_driver_hours['clock_out'])),
        ((cov_driver_hours['last_pod_to_use'].isna()) | (cov_driver_hours['clock_in'] > cov_driver_hours['last_pod_to_use']))
    ]
    choices = [
        0,
        (cov_driver_hours['reg_hours'] + cov_driver_hours['ot_hours'] + cov_driver_hours['dt_hours'])
    ]
    cov_driver_hours['non_productive_time_after_delivery_first_part'] = pd.Series(np.select(conditions, choices, default=((cov_driver_hours['clock_out'] - cov_driver_hours['last_pod_to_use']) / np.timedelta64(1, 'h')))).apply(lambda x: round_half_up(x, "0.01"))

    conditions = [
        (((cov_driver_hours['total_hours'] == 0) & (cov_driver_hours['time_of_first_lunch'] == 0)) | (cov_driver_hours['first_lunch_ending_time'].isna()) | (cov_driver_hours['clock_out'] == cov_driver_hours['last_pod_to_use']) | (cov_driver_hours['clock_in'] > cov_driver_hours['last_pod_to_use']) | (cov_driver_hours['last_pod_to_use'] >= cov_driver_hours['clock_out'])),
        ((cov_driver_hours['first_lunch_starting_time'] >= cov_driver_hours['last_pod_to_use']) & (cov_driver_hours['first_lunch_ending_time'] <= cov_driver_hours['clock_out'])),
        (cov_driver_hours['first_lunch_starting_time'] >= cov_driver_hours['last_pod_to_use']),
        ((cov_driver_hours['first_lunch_ending_time'] > cov_driver_hours['last_pod_to_use']) & (cov_driver_hours['first_lunch_starting_time'] < cov_driver_hours['last_pod_to_use']))
    ]
    choices = [
        0,
        cov_driver_hours['time_of_first_lunch'],
        ((cov_driver_hours.loc[:, ['first_lunch_ending_time', 'clock_out']].min(axis=1) - cov_driver_hours['first_lunch_starting_time']) / np.timedelta64(1, 'h')),
        ((cov_driver_hours.loc[:, ['first_lunch_ending_time', 'clock_out']].min(axis=1) - cov_driver_hours['last_pod_to_use']) / np.timedelta64(1, 'h'))
    ]
    cov_driver_hours['non_productive_time_after_delivery_second_part'] = pd.Series(np.select(conditions, choices, default=0)).apply(lambda x: round_half_up(x, "0.01"))

    cov_driver_hours['non_productive_time_after_delivery_third_part'] = pd.Series(np.where(((cov_driver_hours['time_of_second_lunch'] > 0) & (cov_driver_hours['second_lunch_starting_time'] > cov_driver_hours['last_pod_to_use']) & (cov_driver_hours['second_lunch_ending_time'] < cov_driver_hours['last_pod_to_use'])), ((cov_driver_hours.loc[:, ['second_lunch_ending_time', 'clock_out']].min(axis=1) - cov_driver_hours['last_pod_to_use']) / np.timedelta64(1, 'h')), 0)).apply(lambda x: round_half_up(x, "0.01"))

    cov_driver_hours['non_productive_time_after_delivery'] = cov_driver_hours['non_productive_time_after_delivery_first_part'] - cov_driver_hours['non_productive_time_after_delivery_second_part'] - cov_driver_hours['non_productive_time_after_delivery_third_part']

    cov_driver_hours['non_productive_time_before_delivery'] = (cov_driver_hours['reg_hours'] + cov_driver_hours['ot_hours'] + cov_driver_hours['dt_hours']) - (cov_driver_hours['delivery_time'] + cov_driver_hours['non_productive_time_after_delivery'])

    cov_driver_hours['production_time_converted_to_non_production_time'] = cov_driver_hours['sorting_time_reduction'] + cov_driver_hours['pickup_dropoff_time_reduction'] + cov_driver_hours['line_haul_time_reduction'] + cov_driver_hours['vehicle_maintenance_time_reduction']

    cov_driver_hours['total_delivery_time'] = cov_driver_hours['delivery_time'] - cov_driver_hours['production_time_converted_to_non_production_time']

    cov_driver_hours['total_non_productive_time'] = cov_driver_hours['non_productive_time_after_delivery'] + cov_driver_hours['production_time_converted_to_non_production_time'] + cov_driver_hours['non_productive_time_before_delivery']

    cov_driver_hours['total_delivery_time_wo_breaks'] = cov_driver_hours['total_delivery_time'] - cov_driver_hours['total_break_time_production']

    cov_driver_hours['total_non_productive_time_wo_breaks'] = cov_driver_hours['total_non_productive_time'] - cov_driver_hours['total_break_time_non_production']

    conditions = [
        ((cov_driver_hours['delivery_time'] == 0) | (cov_driver_hours['reg_hours'] == 0) | (cov_driver_hours['non_productive_time_before_delivery'] >= cov_driver_hours['reg_hours'])),
        (cov_driver_hours['delivery_time'] <= (cov_driver_hours['reg_hours'] - cov_driver_hours['non_productive_time_before_delivery']))
    ]
    choices = [
        0,
        cov_driver_hours['delivery_time']
    ]
    cov_driver_hours['regular_production_time'] = pd.Series(np.select(conditions, choices, default=(cov_driver_hours['reg_hours'] - cov_driver_hours['non_productive_time_before_delivery']))).apply(lambda x: round_half_up(x, "0.01"))

    cov_driver_hours['regular_non_productive_time'] = cov_driver_hours['reg_hours'] - cov_driver_hours['regular_production_time']

    daily_stops_per_hour_group = cov_driver_hours.groupby(by=['final_name', 'date'])[['stop_count', 'final_stop_count', 'delivery_time', 'total_delivery_time', 'sdd_signature_time_deduction']].sum()
    daily_stops_per_hour_group.rename(columns={'final_stop_count' : 'daily_final_stop_count', 'total_delivery_time' : 'daily_total_delivery_time'}, inplace=True)

    daily_stops_per_hour_group['stops_per_hour_est_productivity'] = (daily_stops_per_hour_group['stop_count'].apply(lambda x: round_half_up(x, "0.01")) / (daily_stops_per_hour_group['delivery_time'] - daily_stops_per_hour_group['sdd_signature_time_deduction']).apply(lambda x: round_half_up(x, "0.01"))).replace([np.inf, -np.inf, np.nan], 0.0).apply(lambda x: round_half_up(x, "0.01"))

    daily_stops_per_hour_group['final_stops_per_hour'] = (daily_stops_per_hour_group['daily_final_stop_count'] / daily_stops_per_hour_group['daily_total_delivery_time']).replace([np.inf, -np.inf, np.nan], 0).apply(lambda x: round_half_up(x, "0.01"))

    cov_driver_hours = cov_driver_hours.join(daily_stops_per_hour_group.loc[:, ['stops_per_hour_est_productivity', 'final_stops_per_hour']], on=['final_name', 'date'])

    cov_driver_hours.loc[:, ['stops_per_hour_est_productivity', 'final_stops_per_hour']].fillna(0, inplace=True)

    cov_driver_hours = cov_driver_hours.join(employee_directory.loc[:, ['full_name_upper', 'non_production_rate', 'production_base_rate']].set_index(keys=['full_name_upper']), on=['final_name'])

    weekly_sum_groups = cov_driver_hours.groupby(['final_name', 'date'])['total_delivery_time_wo_breaks'].sum()
    weekly_sum_groups.name = "daily_total_delivery_time_wo_breaks"

    cov_driver_hours = cov_driver_hours.join(weekly_sum_groups, on=['final_name', 'date'])

    cov_driver_hours['effective_production_rate_per_day'] = (cov_driver_hours['total_production_pay'].apply(lambda x: round_half_up(x, "0.01")) / cov_driver_hours['daily_total_delivery_time_wo_breaks'].apply(lambda x: round_half_up(x, "0.01"))).replace([np.inf, -np.inf, np.nan], 0).apply(lambda x: round_half_up(x, "0.001"))

    cov_driver_hours['adjustment_for_non_productive_rate'] = np.where(((cov_driver_hours['pay_type'] == "PS") & (cov_driver_hours['effective_production_rate_per_day'].apply(lambda x: round_half_up(x, "0.01")) < cov_driver_hours['non_production_rate'])), (cov_driver_hours['total_delivery_time_wo_breaks'] * (cov_driver_hours['non_production_rate'] - cov_driver_hours['effective_production_rate_per_day'])).apply(lambda x: round_up(x, 2)), 0)

    cov_driver_hours['total_route_compensation'] = (cov_driver_hours['effective_production_rate_per_day'] * cov_driver_hours['total_delivery_time_wo_breaks']).apply(lambda x: round_half_up(x, "0.01")) + cov_driver_hours['adjustment_for_non_productive_rate']

    cov_driver_hours['total_non_production_pay_wo_breaks'] = (cov_driver_hours['non_production_rate'] * cov_driver_hours['total_non_productive_time_wo_breaks']).apply(lambda x: round_half_up(x, "0.01"))

    weekly_sum_groups = cov_driver_hours.groupby(['final_name', 'week_ending'])[['total_applicable_hours', 'total_break_time', 'total_delivery_time_wo_breaks', 'total_route_compensation', 'total_non_production_pay_wo_breaks']].sum()
    weekly_sum_groups.rename(columns={'total_applicable_hours' : 'weekly_total_applicable_hours', 'total_break_time' : 'weekly_total_break_time', 'total_delivery_time_wo_breaks' : 'weekly_total_delivery_time_wo_breaks', 'total_route_compensation' : 'weekly_total_route_compensation', 'total_non_production_pay_wo_breaks' : 'weekly_total_non_production_pay_wo_breaks'}, inplace=True)

    cov_driver_hours = cov_driver_hours.join(weekly_sum_groups, on=['final_name', 'week_ending'])

    cov_driver_hours['effective_production_rate_for_wage_statement'] = (cov_driver_hours['weekly_total_route_compensation'] / cov_driver_hours['weekly_total_delivery_time_wo_breaks']).fillna(0).apply(lambda x: round_up(x, 2))

    cov_driver_hours['regular_rate_calculation'] = ((cov_driver_hours['weekly_total_route_compensation'] + cov_driver_hours['weekly_total_non_production_pay_wo_breaks']) / (cov_driver_hours['weekly_total_applicable_hours'] - cov_driver_hours['weekly_total_break_time'])).fillna(0).apply(lambda x: round_half_up(x, "0.01"))

    cov_driver_hours['gross_earnings'] = pd.Series(np.where(cov_driver_hours['pay_type'] == "PS", (cov_driver_hours['total_route_compensation'] + cov_driver_hours['total_non_production_pay_wo_breaks'] + (cov_driver_hours['regular_rate_calculation'] * cov_driver_hours['total_break_time']) + (cov_driver_hours['ot_hours'] * (cov_driver_hours['regular_rate_calculation'] * 0.5).apply(lambda x: round_half_up(x, "0.01"))) + (cov_driver_hours['dt_hours'] * cov_driver_hours['regular_rate_calculation'])), 0)).apply(lambda x: round_half_up(x, "0.01"))

    cov_driver_hours['package_count'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['package_count'], cov_driver_hours['sdd_packages'])

    cov_driver_hours['amount_charged'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['amount_charged'], cov_driver_hours['sdd_amount_charged'])

    production_rate_lookup = pd.DataFrame({
        'rover_min' : [0, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38],
        'rover_max' : [6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 2000],
        'cov_min' : [0, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40],
        'cov_max' : [8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 2000],
        'increment' : [0, 2, 2, 3, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3],
        'cummulative' : [0, 2, 4, 7, 9, 11, 13, 15, 17, 20, 23, 26, 29, 32, 35, 38, 41, 44],
        'upload_earnings_code' : [101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118],
        'rate_title' : ["Regular Rate 1", "Regular Rate 2", "Regular Rate 3", "Regular Rate 4", "Regular Rate 5", "Regular Rate 6", "Regular Rate 7", "Regular Rate 8", "Regular Rate 9", "Regular Rate 10", "Regular Rate 11", "Regular Rate 12", "Regular Rate 13", "Regular Rate 14", "Regular Rate 15", "Regular Rate 16", "Regular Rate 17", "Regular Rate 18", ]
    })

    cov_rate_list = cov_driver_hours.loc[(cov_driver_hours['dep_no'] == "55"), ['final_name', 'stops_per_hour_est_productivity']].drop_duplicates()
    rover_rate_list = cov_driver_hours.loc[(cov_driver_hours['dep_no'] == "20"), ['final_name', 'stops_per_hour_est_productivity']].drop_duplicates()

    cov_rate_list = cov_rate_list.merge(production_rate_lookup, how='cross')
    cov_rate_list = cov_rate_list.loc[(cov_rate_list['stops_per_hour_est_productivity'] >= cov_rate_list['cov_min']) & (cov_rate_list['stops_per_hour_est_productivity'].round(2) < cov_rate_list['cov_max'])].set_index(keys=['final_name', 'stops_per_hour_est_productivity'])

    rover_rate_list = rover_rate_list.merge(production_rate_lookup, how='cross')
    rover_rate_list = rover_rate_list.loc[(rover_rate_list['stops_per_hour_est_productivity'] >= rover_rate_list['rover_min']) & (rover_rate_list['stops_per_hour_est_productivity'].round(2) < rover_rate_list['rover_max'])].set_index(keys=['final_name', 'stops_per_hour_est_productivity'])

    rate_list = pd.concat([cov_rate_list, rover_rate_list], ignore_index=False, axis=0)

    cov_driver_hours = cov_driver_hours.join(rate_list, on=['final_name', 'stops_per_hour_est_productivity'])
    cov_driver_hours.loc[np.isnan(cov_driver_hours['cummulative']), 'cummulative'] = 0

    cov_driver_hours['production_rate'] = cov_driver_hours['production_base_rate'] + cov_driver_hours['cummulative']

    cov_driver_hours['daily_amount'] = (cov_driver_hours['production_rate'] * cov_driver_hours['total_delivery_time']) + (cov_driver_hours['non_production_rate'] * cov_driver_hours['total_non_productive_time'])

    weekly_sum_groups = cov_driver_hours.groupby(['final_name', 'week_ending'])['daily_amount'].sum()
    weekly_sum_groups.name =  "weekly_total_daily_amount"

    cov_driver_hours = cov_driver_hours.join(weekly_sum_groups, on=['final_name', 'week_ending'])

    cov_driver_hours['weekly_regular_rate_prod'] = (cov_driver_hours['weekly_total_daily_amount'] / cov_driver_hours['weekly_total_applicable_hours']).fillna(0).apply(lambda x: round_half_up(x, "0.01"))

    cov_driver_hours['gross_earnings_prod'] = np.where(cov_driver_hours['pay_type'] == "PRODUCTIVITY", ((cov_driver_hours['regular_non_productive_time'] * cov_driver_hours['non_production_rate']).apply(lambda x: round_half_up(x, "0.01")) + (cov_driver_hours['production_rate'] * cov_driver_hours['regular_production_time']).apply(lambda x: round_half_up(x, "0.01")) + ((cov_driver_hours['weekly_regular_rate_prod'] * 1.5).apply(lambda x: round_half_up(x, "0.01")) * cov_driver_hours['ot_hours']) + (cov_driver_hours['weekly_regular_rate_prod'] * 2.0 * cov_driver_hours['dt_hours']).apply(lambda x: round_half_up(x, "0.01"))), 0)

    cov_driver_hours['combined_gross_earnings'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['gross_earnings'], cov_driver_hours['gross_earnings_prod'])

    cov_driver_hours['earnings_code'] = np.where(cov_driver_hours['pay_type'] == "PRODUCTIVITY", cov_driver_hours['upload_earnings_code'], np.nan)

    cov_driver_hours['weekend_filter'] = np.where(cov_driver_hours['date'].dt.day_of_week >= 5, cov_driver_hours['date'].dt.day_name(), "Weekday")

    cov_driver_hours['driving_days'] = np.where(((cov_driver_hours['stop_count'] > 1) | (cov_driver_hours['full_stop_rate'] > 1)), (1 / cov_driver_hours['multiline']).apply(lambda x: round_half_up(x, "0.01")), 0)

    pay_period_stops_group = cov_driver_hours.loc[cov_driver_hours['weekend_filter'] == "Weekday"].groupby(by=['final_name'])[['combined_stops', 'driving_days']].sum()
    pay_period_stops_group.rename(columns={'combined_stops' : 'pay_period_total_combined_stops', 'driving_days' : 'pay_period_total_driving_days'}, inplace=True)

    pay_period_stops_group['avg_stops_weekday'] = (pay_period_stops_group['pay_period_total_combined_stops'] / pay_period_stops_group['pay_period_total_driving_days']).replace([np.inf, -np.inf, np.nan], 0.0).apply(lambda x: round_half_up(x, "1"))

    cov_driver_hours = cov_driver_hours.join(pay_period_stops_group, on=['final_name'])

    cov_driver_hours['squared_difference_stops'] = np.where(cov_driver_hours['weekend_filter'] == "Weekday", ((cov_driver_hours['combined_stops'] * cov_driver_hours['multiline']) - cov_driver_hours['avg_stops_weekday']).pow(2), 0)

    stops_standard_devation_group = cov_driver_hours.loc[cov_driver_hours['weekend_filter'] == "Weekday"].groupby(by=['final_name'])[['squared_difference_stops', 'driving_days']].sum()

    stops_standard_devation_group['stops_standard_devation'] = (stops_standard_devation_group['squared_difference_stops'] / stops_standard_devation_group['driving_days']).replace([np.inf, -np.inf, np.nan], 0.0).pow(1./2).apply(lambda x: round_half_up(x, "0.01"))

    cov_driver_hours = cov_driver_hours.join(stops_standard_devation_group.loc[:, ['stops_standard_devation']], on=['final_name'])

    avg_productivity_weekday_group = cov_driver_hours.loc[cov_driver_hours['weekend_filter'] == "Weekday"].groupby(by=['final_name'])[['combined_stops', 'total_delivery_time']].sum()

    avg_productivity_weekday_group['avg_productivity_weekday'] = (avg_productivity_weekday_group['combined_stops'] / avg_productivity_weekday_group['total_delivery_time']).replace([np.inf, -np.inf, np.nan], 0.0).apply(lambda x: round_half_up(x, "0.01"))

    cov_driver_hours = cov_driver_hours.join(avg_productivity_weekday_group.loc[:, ['avg_productivity_weekday']], on=['final_name'])

    cov_driver_hours['squared_difference'] = np.where((cov_driver_hours['weekend_filter'] == "Weekday"), (np.where((cov_driver_hours['pay_type'] == "PS"), cov_driver_hours['final_stops_per_hour'], cov_driver_hours['stops_per_hour_est_productivity']) - cov_driver_hours['avg_productivity_weekday']).pow(2), 0)

    productivity_standard_devation_group = cov_driver_hours.loc[cov_driver_hours['weekend_filter'] == "Weekday"].groupby(by=['final_name'])[['squared_difference', 'driving_days']].sum()

    productivity_standard_devation_group['productivity_standard_devation'] = (productivity_standard_devation_group['squared_difference'] / productivity_standard_devation_group['driving_days']).replace([np.inf, -np.inf, np.nan], 0.0).pow(1./2).apply(lambda x: round_half_up(x, "0.01"))

    cov_driver_hours = cov_driver_hours.join(productivity_standard_devation_group.loc[:, ['productivity_standard_devation']], on=['final_name'])

    cov_driver_hours['pay_period_filter'] = ((cov_driver_hours['date'] >= date_range[0]) & (cov_driver_hours['date'] <= date_range[1]))

    cov_driver_hours['include_in_production_summary'] = ((cov_driver_hours['combined_stops'] > 0) & (cov_driver_hours['pay_period_filter']))

    delivery_days_per_week_group = cov_driver_hours.loc[:, ['final_name', 'week_ending', 'date']].drop_duplicates().groupby(['final_name', 'week_ending'])['date'].count()
    delivery_days_per_week_group.name = "delivery_days_per_week"

    cov_driver_hours = cov_driver_hours.join(delivery_days_per_week_group, on=['final_name', 'week_ending'])

    conditions = [
        ((cov_driver_hours['multiline'] > 1 ) & (cov_driver_hours['clock_out'] < cov_driver_hours['first_pod_to_use'])),
        ((cov_driver_hours['multiline'] > 1) & (cov_driver_hours['sorting_time'] > 0) & (cov_driver_hours['sorting_end'] < cov_driver_hours['first_pod_to_use']))
    ]
    choices = [
        cov_driver_hours['total_applicable_hours'],
        cov_driver_hours['sorting_time']
    ]
    cov_driver_hours['morning_warehouse_sort'] = pd.Series(np.select(conditions, choices, default=0)).apply(lambda x: round_half_up(x, "0.01"))

    conditions = [
        ((cov_driver_hours['multiline'] == 1) & (cov_driver_hours['total_delivery_time'] > 0.5)),
        (cov_driver_hours['final_stop_count'] > 1)
    ]
    choices = [
        cov_driver_hours['non_productive_time_before_delivery'],
        (cov_driver_hours['non_productive_time_before_delivery'] - cov_driver_hours['morning_warehouse_sort'])
    ]
    cov_driver_hours['time_before_delivery_driving_only'] = np.select(conditions, choices, default=0)

    cov_driver_hours['time_after_delivery_driving_only'] = np.where((cov_driver_hours['total_delivery_time'] > 0.5), cov_driver_hours['non_productive_time_after_delivery'], 0)

    cov_driver_hours['route_sort'] = pd.Series(np.where(((cov_driver_hours['morning_warehouse_sort'] > 0) | (cov_driver_hours['first_pod_to_use'].isna()) | (cov_driver_hours['total_hours'] == 0) | (cov_driver_hours['clock_in'] > cov_driver_hours['first_pod_to_use'])), 0, ((cov_driver_hours['first_pod_to_use'] - cov_driver_hours['clock_in']) / np.timedelta64(1, 'h')) * 0.5)).apply(lambda x: round_half_up(x, "0.01"))

    cov_driver_hours['additional_morning_warehouse_sort'] = np.where(((cov_driver_hours['multiline'] == 1) & (cov_driver_hours['clock_in'] <= (cov_driver_hours['clock_in'].dt.normalize() + timedelta(0, 0, 0, 0, 30, 4, 0)))), (cov_driver_hours['route_sort'] * 0.5), 0)

    cov_driver_hours['final_additional_morning_warehouse_sort_hours'] = cov_driver_hours['morning_warehouse_sort'] + cov_driver_hours['additional_morning_warehouse_sort']

    cov_driver_hours['final_route_sort_hours'] = cov_driver_hours['route_sort'] - cov_driver_hours['additional_morning_warehouse_sort']

    cov_driver_hours['final_evening_sort_hours'] = pd.Series(np.where(((cov_driver_hours['first_pod_to_use'].notna()) & (cov_driver_hours['sorting_time'] > 0) & (cov_driver_hours['sorting_start'].dt.floor('Min').dt.time >= cov_driver_hours['last_pod_to_use'].dt.floor('Min').dt.time)), cov_driver_hours['sorting_time'], 0)).apply(lambda x: round_half_up(x, "0.01"))

    cov_driver_hours['final_weekend_sort_hours'] = np.where(((cov_driver_hours['first_pod_to_use'].isna()) & (cov_driver_hours['clock_in'].dt.dayofweek.isin([5, 6]))), cov_driver_hours['total_applicable_hours'], 0)

    cov_driver_hours['morning_warehouse_sort_cost'] = cov_driver_hours['final_additional_morning_warehouse_sort_hours'] * cov_driver_hours['non_production_rate']

    cov_driver_hours['route_sort_cost'] = cov_driver_hours['final_route_sort_hours'] * cov_driver_hours['non_production_rate']

    cov_driver_hours['evening_sort_cost'] = cov_driver_hours['final_evening_sort_hours'] * cov_driver_hours['non_production_rate']

    cov_driver_hours['weekend_sort_cost'] = np.where(((cov_driver_hours['pay_type'] == "PS") & (cov_driver_hours['final_weekend_sort_hours'] > 0)), cov_driver_hours['gross_earnings'], np.where((cov_driver_hours['final_weekend_sort_hours'] > 0), cov_driver_hours['daily_amount'], 0))

    conditions = [
        ((cov_driver_hours['non_productive_time_after_delivery'] == 0) | (cov_driver_hours['dt_hours'] == 0)),
        (cov_driver_hours['non_productive_time_after_delivery'] < cov_driver_hours['dt_hours'])
    ]
    choices = [
        0,
        cov_driver_hours['non_productive_time_after_delivery']
    ]
    cov_driver_hours['dt_non_production'] = np.select(conditions, choices, default=cov_driver_hours['dt_hours'])

    conditions = [
        (cov_driver_hours['ot_hours'] == 0),
        ((cov_driver_hours['non_productive_time_after_delivery'] < (cov_driver_hours['ot_hours'] + cov_driver_hours['dt_hours'])) & (cov_driver_hours['non_productive_time_after_delivery'] > cov_driver_hours['dt_non_production'])),
        (cov_driver_hours['non_productive_time_after_delivery'] >= (cov_driver_hours['ot_hours'] + cov_driver_hours['dt_hours'])),
    ]
    choices = [
        0,
        cov_driver_hours['non_productive_time_after_delivery'] - cov_driver_hours['dt_non_production'],
        cov_driver_hours['ot_hours']
    ]
    cov_driver_hours['ot_non_production'] = np.select(conditions, choices, default=0)

    conditions = [
        (cov_driver_hours['ot_hours'] == 0),
        (cov_driver_hours['delivery_time'] > cov_driver_hours['ot_hours'])
    ]
    choices = [
        0,
        cov_driver_hours['ot_hours'] - cov_driver_hours['ot_non_production']
    ]
    cov_driver_hours['ot_production'] = np.select(conditions, choices, default=0)

    cov_driver_hours['dt_production'] = np.where(((cov_driver_hours['ot_non_production'] > 0) | (cov_driver_hours['dt_hours'] == cov_driver_hours['dt_non_production']) | (cov_driver_hours['dt_hours'] == 0)), 0, (cov_driver_hours['dt_hours'] - cov_driver_hours['dt_non_production']))

    cov_driver_hours['ot_cost'] = np.where((cov_driver_hours['pay_type'] == "PS"), ((np.minimum(cov_driver_hours['non_production_rate'], cov_driver_hours['effective_production_rate_per_day']) * cov_driver_hours['ot_production']) + (cov_driver_hours['non_production_rate'] * cov_driver_hours['ot_non_production']) + (cov_driver_hours['ot_hours'] * cov_driver_hours['regular_rate_calculation'] * 0.5)), (cov_driver_hours['ot_hours'] * cov_driver_hours['weekly_regular_rate_prod'] * 1.5))

    cov_driver_hours['dt_cost'] = np.where((cov_driver_hours['pay_type'] == "PS"), ((np.minimum(cov_driver_hours['non_production_rate'], cov_driver_hours['effective_production_rate_per_day']) * cov_driver_hours['dt_production']) + (cov_driver_hours['non_production_rate'] * cov_driver_hours['dt_non_production']) + (cov_driver_hours['dt_hours'] * cov_driver_hours['regular_rate_calculation'] * 1)), (cov_driver_hours['dt_hours'] * cov_driver_hours['weekly_regular_rate_prod'] * 2))

    hub_region = pd.DataFrame({
        'route_code': ['BAY', 'CAJ', 'CEN', 'CNG', 'CST', 'DUB', 'EST', 'FAT', 'ING', 'LAN', 'MOV', 'NSD', 'OAK', 'ONT', 'ONX', 'PMF', 'RIV', 'SFS', 'SJC', 'SND', 'TUS', 'VIS', 'VLY', 'WSC'],
        'new_hub': ['BAY', 'ONT', 'CEN', 'CNG', 'CST', 'DUB', 'EST', 'FAT/VIS', 'ING', 'LAN', 'MOV', 'NSD', 'OAK', 'ONT', 'ONX', 'PMF', 'ONT', 'SFS', 'SJC', 'SND', 'TUS', 'FAT/VIS', 'VLY', 'WSC'],
        'region': ['Norcal', 'OC', 'Norcal', 'LA', 'Central Coast', 'Norcal', 'LA', 'Norcal', 'LA', 'LA', 'SD/MOV', 'SD/MOV', 'Norcal', 'OC', 'LA', 'LA', 'OC', 'OC', 'Norcal', 'SD/MOV', 'OC', 'Norcal', 'LA', 'Norcal']
    }).set_index(keys=['route_code'])

    cov_driver_hours = cov_driver_hours.join(hub_region, on=[cov_driver_hours['driver_code'].str[:3]])
    cov_driver_hours = cov_driver_hours.drop(columns=['key_0'])

    metrics_sum_group = cov_driver_hours.groupby(['final_name', 'date'])[['combined_stops', 'package_count', 'delivery_time', 'gross_earnings', 'gross_earnings_prod']].sum().rename(columns={'combined_stops' : 'spd', 'package_count' : 'ppd', 'delivery_time' : 'daily_delivery_time', 'gross_earnings' : 'daily_gross_earnings', 'gross_earnings_prod' : 'daily_gross_earnings_prod'})

    cov_driver_hours = cov_driver_hours.join(metrics_sum_group, on=['final_name', 'date'])

    cov_driver_hours['sph'] = (cov_driver_hours['spd'] / cov_driver_hours['daily_delivery_time']).replace([np.inf, -np.inf, np.nan], 0.0).apply(lambda x: round_half_up(x, "0.01"))
    cov_driver_hours['cost_per_stop'] = ((cov_driver_hours['daily_gross_earnings'] + cov_driver_hours['daily_gross_earnings_prod']) / (cov_driver_hours['combined_stops'] * cov_driver_hours['multiline'])).replace([np.inf, -np.inf, np.nan], 0.0)

    conditions = [
        ((cov_driver_hours['last_pod_to_use'].notna()) & (cov_driver_hours['last_pod_to_use'] > cov_driver_hours['clock_out'])),
        ((cov_driver_hours['last_pod_to_use'].notna()) & (cov_driver_hours['second_to_last_pod'].notna()) & (((cov_driver_hours['last_pod_to_use'] - cov_driver_hours['second_to_last_pod']) / np.timedelta64(1, 'h')) >= 0.7))
    ]
    choices = [
        0,
        1
    ]
    cov_driver_hours['pod_difference_to_show'] = np.select(conditions, choices, default=0)

    cov_driver_hours['jobs_after_delivery'] = (cov_driver_hours['last_pod_to_use'].notna()) & (cov_driver_hours['clock_in'] > cov_driver_hours['last_pod_to_use'])

    daily_gross_earnings_w_jobs_after_delivery_group = cov_driver_hours.loc[cov_driver_hours['jobs_after_delivery']].groupby(by=['final_name', 'date'])['gross_earnings'].sum()
    daily_gross_earnings_w_jobs_after_delivery_group.name = "daily_gross_earnings_w_jobs_after_delivery"

    cov_driver_hours = cov_driver_hours.join(daily_gross_earnings_w_jobs_after_delivery_group, on=['final_name', 'date'])
    cov_driver_hours['daily_gross_earnings_w_jobs_after_delivery'] = cov_driver_hours['daily_gross_earnings_w_jobs_after_delivery'].fillna(0.0)

    sorting_costs_group = cov_driver_hours.groupby(by=['final_name', 'date'])[['morning_warehouse_sort_cost', 'evening_sort_cost']].sum()
    sorting_costs_group.rename(columns={'morning_warehouse_sort_cost' : 'daily_morning_warehouse_sort_cost', 'evening_sort_cost' : 'daily_evening_sort_cost'}, inplace=True)

    cov_driver_hours = cov_driver_hours.join(sorting_costs_group, on=['final_name', 'date'])

    cov_driver_hours['cost_per_stop_wo_other_jobs'] = ((cov_driver_hours['daily_gross_earnings'] + cov_driver_hours['daily_gross_earnings_prod'] - cov_driver_hours['daily_morning_warehouse_sort_cost'] - cov_driver_hours['daily_evening_sort_cost'] - cov_driver_hours['daily_gross_earnings_w_jobs_after_delivery']) / (cov_driver_hours['combined_stops'] * cov_driver_hours['multiline'])).replace([np.inf, -np.inf, np.nan], 0.0)

    cov_driver_hours['first_pod_1'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['ddd_first_pod_1'], cov_driver_hours['sdd_first_pod_1'])
    cov_driver_hours['first_pod_2'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['ddd_first_pod_2'], cov_driver_hours['sdd_first_pod_2'])
    cov_driver_hours['first_pod_3'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['ddd_first_pod_3'], cov_driver_hours['sdd_first_pod_3'])
    cov_driver_hours['first_pod_4'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['ddd_first_pod_4'], cov_driver_hours['sdd_first_pod_4'])
    cov_driver_hours['first_pod_5'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['ddd_first_pod_5'], cov_driver_hours['sdd_first_pod_5'])
    cov_driver_hours['first_pod_6'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['ddd_first_pod_6'], cov_driver_hours['sdd_first_pod_6'])
    cov_driver_hours['first_pod_7'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['ddd_first_pod_7'], cov_driver_hours['sdd_first_pod_7'])
    cov_driver_hours['first_pod_8'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['ddd_first_pod_8'], cov_driver_hours['sdd_first_pod_8'])
    cov_driver_hours['first_pod_9'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['ddd_first_pod_9'], cov_driver_hours['sdd_first_pod_9'])
    cov_driver_hours['first_pod_10'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['ddd_first_pod_10'], cov_driver_hours['sdd_first_pod_10'])

    cov_driver_hours['last_pod_1'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['ddd_last_pod_1'], cov_driver_hours['sdd_last_pod_1'])
    cov_driver_hours['last_pod_2'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['ddd_last_pod_2'], cov_driver_hours['sdd_last_pod_2'])
    cov_driver_hours['last_pod_3'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['ddd_last_pod_3'], cov_driver_hours['sdd_last_pod_3'])
    cov_driver_hours['last_pod_4'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['ddd_last_pod_4'], cov_driver_hours['sdd_last_pod_4'])
    cov_driver_hours['last_pod_5'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['ddd_last_pod_5'], cov_driver_hours['sdd_last_pod_5'])
    cov_driver_hours['last_pod_6'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['ddd_last_pod_6'], cov_driver_hours['sdd_last_pod_6'])
    cov_driver_hours['last_pod_7'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['ddd_last_pod_7'], cov_driver_hours['sdd_last_pod_7'])
    cov_driver_hours['last_pod_8'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['ddd_last_pod_8'], cov_driver_hours['sdd_last_pod_8'])
    cov_driver_hours['last_pod_9'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['ddd_last_pod_9'], cov_driver_hours['sdd_last_pod_9'])
    cov_driver_hours['last_pod_10'] = np.where(cov_driver_hours['pay_type'] == "PS", cov_driver_hours['ddd_last_pod_10'], cov_driver_hours['sdd_last_pod_10'])

    conditions = [
        (cov_driver_hours['first_pod_1'] is None),
        (cov_driver_hours['first_pod_9'] < cov_driver_hours['first_clock_in']),
        (cov_driver_hours['first_pod_8'] < cov_driver_hours['first_clock_in']),
        (cov_driver_hours['first_pod_7'] < cov_driver_hours['first_clock_in']),
        (cov_driver_hours['first_pod_6'] < cov_driver_hours['first_clock_in']),
        (cov_driver_hours['first_pod_5'] < cov_driver_hours['first_clock_in']),
        (cov_driver_hours['first_pod_4'] < cov_driver_hours['first_clock_in']),
        (cov_driver_hours['first_pod_3'] < cov_driver_hours['first_clock_in']),
        (cov_driver_hours['first_pod_2'] < cov_driver_hours['first_clock_in']),
        (cov_driver_hours['first_pod_1'] < cov_driver_hours['first_clock_in'])
    ]
    choices = [
        0,
        9,
        8,
        7,
        6,
        5,
        4,
        3,
        2,
        1
    ]
    cov_driver_hours['pod_prior_to_clock_in_check'] = pd.Series(np.select(conditions, choices, default=-1)).astype('Int32')

    conditions = [
        (cov_driver_hours['last_pod_1'] is None),
        (cov_driver_hours['last_pod_9'] > cov_driver_hours['last_clock_out']),
        (cov_driver_hours['last_pod_8'] > cov_driver_hours['last_clock_out']),
        (cov_driver_hours['last_pod_7'] > cov_driver_hours['last_clock_out']),
        (cov_driver_hours['last_pod_6'] > cov_driver_hours['last_clock_out']),
        (cov_driver_hours['last_pod_5'] > cov_driver_hours['last_clock_out']),
        (cov_driver_hours['last_pod_4'] > cov_driver_hours['last_clock_out']),
        (cov_driver_hours['last_pod_3'] > cov_driver_hours['last_clock_out']),
        (cov_driver_hours['last_pod_2'] > cov_driver_hours['last_clock_out']),
        (cov_driver_hours['last_pod_1'] >= cov_driver_hours['last_clock_out'])
    ]
    choices = [
        0,
        9,
        8,
        7,
        6,
        5,
        4,
        3,
        2,
        1
    ]
    cov_driver_hours['pod_post_clock_out_check'] = pd.Series(np.select(conditions, choices, default=-1)).astype('Int32')

    max_other_deliver_time = other_delivery_data.groupby(by=['final_name', 'date'])['delivery_time'].max()
    max_other_deliver_time.name = "pickup_datetime"

    cov_driver_hours = cov_driver_hours.join(max_other_deliver_time, on=['final_name', 'date'])

    cov_driver_hours['max_subfunction_timestamp'] = cov_driver_hours.loc[:, ['last_pod_1', 'lunch_end', 'sorting_end', 'pickup_dropoff_end', 'line_haul_end', 'vehicle_maintenance_end']].max(axis=1, numeric_only=False)

    cov_driver_hours = cov_driver_hours.join(daily_error_exceptions, on=['final_name', 'date'])

    conditions = [
        ((cov_driver_hours['dep_no'] != "55") | (cov_driver_hours['status'] == "T") | (cov_driver_hours['hub'] == "TERM") | (cov_driver_hours['stop_count'] <= 10) | (cov_driver_hours['weekend_filter'] != "Weekday") | (cov_driver_hours['clock_out'] <= cov_driver_hours['clock_in']) | (cov_driver_hours['pickup_datetime'] >= cov_driver_hours['last_pod_1'])),
        ((cov_driver_hours['date'].dt.weekday == 5.0) & (((cov_driver_hours['clock_out'] - cov_driver_hours['max_subfunction_timestamp']) / np.timedelta64(1, 'h')) > (3/4))),
        (((cov_driver_hours['clock_out'] - cov_driver_hours['max_subfunction_timestamp']) / np.timedelta64(1, 'h')) > (1/12))
    ]
    choices = [
        0,
        ((cov_driver_hours['clock_out'] - cov_driver_hours['max_subfunction_timestamp']) / np.timedelta64(1, 'h')),
        (((cov_driver_hours['clock_out'] - cov_driver_hours['max_subfunction_timestamp']) / np.timedelta64(1, 'h')) - (1/12))
    ]
    cov_driver_hours['end_of_shift_difference'] = np.select(conditions, choices, default=0)

    cov_driver_hours['new_clock_out_time'] = pd.to_datetime(np.where((cov_driver_hours['end_of_shift_difference'] >= 0.1), (cov_driver_hours['clock_out'] - pd.to_timedelta(cov_driver_hours['end_of_shift_difference'], 'hours')), pd.NaT))

    conditions = [
        ((cov_driver_hours['end_of_shift_difference'] > 1.0) & (~cov_driver_hours['final_name'].isin(end_of_shift_protocol_exceptions)) & (pd.isna(cov_driver_hours['End of Shift Protocol']))),
        ((pd.notna(cov_driver_hours['new_clock_out_time'])) & (~cov_driver_hours['final_name'].isin(end_of_shift_protocol_exceptions)) & (pd.isna(cov_driver_hours['End of Shift Protocol'])))
    ]
    choices = [
        ("New Clock-Out Time: " + cov_driver_hours['new_clock_out_time'].dt.strftime("%m/%d/%Y %H:%M") + " - Must Fix"),
        ("New Clock-Out Time: " + cov_driver_hours['new_clock_out_time'].dt.strftime("%m/%d/%Y %H:%M"))
    ]
    cov_driver_hours['end_of_shift_protocol'] = np.select(conditions, choices, default=None)

    cov_driver_hours['clock_in_wo_seconds'] = cov_driver_hours['clock_in'].dt.floor('Min')
    cov_driver_hours['clock_out_wo_seconds'] = cov_driver_hours['clock_out'].dt.floor('Min')

    cov_driver_hours['prev_final_name'] = cov_driver_hours['final_name'].shift(1)
    cov_driver_hours['prev_clock_in'] = cov_driver_hours['clock_in_wo_seconds'].shift(1)
    cov_driver_hours['prev_clock_out'] = cov_driver_hours['clock_out_wo_seconds'].shift(1)
    cov_driver_hours['prev_total_hours'] = cov_driver_hours['total_hours'].shift(1)

    cov_driver_hours['post_final_name'] = cov_driver_hours['final_name'].shift(-1)
    cov_driver_hours['post_clock_in'] = cov_driver_hours['clock_in_wo_seconds'].shift(-1)
    cov_driver_hours['post_clock_out'] = cov_driver_hours['clock_out_wo_seconds'].shift(-1)
    cov_driver_hours['post_total_hours'] = cov_driver_hours['total_hours'].shift(-1)

    cov_driver_hours['clock_in_clock_out_errors'] = cov_driver_hours.apply(lambda x: 
        "Still Clocked-In - Must Fix" if (pd.isna(x['clock_out'])) else (
            "Overlapping Shifts - Must Fix" if (
            (
                (
                    (
                        ((x['prev_clock_in'] <= x['clock_in_wo_seconds']) & (x['prev_clock_out'] > x['clock_in_wo_seconds']))
                        |
                        ((x['prev_clock_in'] <= x['clock_out_wo_seconds']) & (x['prev_clock_out'] >= x['clock_out_wo_seconds']))
                        |
                        ((x['prev_clock_in'] >= x['clock_in_wo_seconds']) & (x['prev_clock_out'] <= x['clock_out_wo_seconds']))
                        |
                        ((x['prev_clock_in'] <= x['clock_in_wo_seconds']) & (x['prev_clock_out'] >= x['clock_out_wo_seconds']))
                    )
                    &
                    ((x['prev_final_name'] == x['final_name']) & (x['prev_total_hours'] > 0.0))
                ) | (
                    (
                        ((x['post_clock_in'] <= x['clock_in_wo_seconds']) & (x['post_clock_out'] >= x['clock_in_wo_seconds']))
                        |
                        ((x['post_clock_in'] < x['clock_out_wo_seconds']) & (x['post_clock_out'] >= x['clock_out_wo_seconds']))
                        |
                        ((x['post_clock_in'] >= x['clock_in_wo_seconds']) & (x['post_clock_out'] <= x['clock_out_wo_seconds']))
                        |
                        ((x['post_clock_in'] <= x['clock_in_wo_seconds']) & (x['post_clock_out'] >= x['clock_out_wo_seconds']))
                    )
                    &
                    ((x['post_final_name'] == x['final_name']) & (x['post_total_hours'] > 0.0))
                )     
            ) & (x['total_hours'] > 0.0)) else (
                "Clock-In After Clock-Out - Must Fix" if (x['clock_in'] > x['clock_out']) else (
                    "Excessive Hours: {} - Must Fix".format(round((x['reg_hours_sum'] + x['ot_hours_sum'] + x['dt_hours_sum']), 2)) if (((x['reg_hours_sum'] + x['ot_hours_sum'] + x['dt_hours_sum']) > 20.0) & (pd.isna(x['Excessive Hours']))) else (
                        "High Hours: {} - Please Confirm".format(round((x['reg_hours_sum'] + x['ot_hours_sum'] + x['dt_hours_sum']), 2)) if (((x['reg_hours_sum'] + x['ot_hours_sum'] + x['dt_hours_sum']) > excessive_hour_threshold) & (pd.isna(x['Excessive Hours']))) else (
                            np.nan
                        )
                    )
                )
            )
        )
    , axis=1)

    cov_driver_hours.drop(columns=['clock_in_wo_seconds', 'clock_out_wo_seconds', 'prev_final_name', 'prev_clock_in', 'prev_clock_out', 'prev_total_hours', 'post_final_name', 'post_clock_in', 'post_clock_out', 'post_total_hours'], inplace=True)

    conditions = [
        ((cov_driver_hours['total_hours_sum'] > 6) & (cov_driver_hours['lunch_start_count'] < 1)),
        ((cov_driver_hours['total_hours_sum'] > 6) & (cov_driver_hours['total_hours_sum'] - (cov_driver_hours['reg_hours_sum'] + cov_driver_hours['ot_hours_sum'] + cov_driver_hours['dt_hours_sum']) > 4.0) & (pd.isna(cov_driver_hours['Excessive Lunch Break']))),
        ((cov_driver_hours['total_hours_sum'] > 6) & (cov_driver_hours['total_hours_sum'] - (cov_driver_hours['reg_hours_sum'] + cov_driver_hours['ot_hours_sum'] + cov_driver_hours['dt_hours_sum']) > 1.0) & (pd.isna(cov_driver_hours['Excessive Lunch Break']))),
        ((cov_driver_hours['total_hours_sum'] > 6) & (round(cov_driver_hours['total_hours_sum'] - (cov_driver_hours['reg_hours_sum'] + cov_driver_hours['ot_hours_sum'] + cov_driver_hours['dt_hours_sum']), 2) < 0.5)),
        (cov_driver_hours['lunch_pod_count'] > 0)
    ]
    choices = [
        "Missing Lunch Subfunction",
        "Excessive Lunch Break: " + (cov_driver_hours['total_hours_sum'] - (cov_driver_hours['reg_hours_sum'] + cov_driver_hours['ot_hours_sum'] + cov_driver_hours['dt_hours_sum'])).round(2).astype('str') + " hours - Must Fix",
        "High Lunch Break: " + (cov_driver_hours['total_hours_sum'] - (cov_driver_hours['reg_hours_sum'] + cov_driver_hours['ot_hours_sum'] + cov_driver_hours['dt_hours_sum'])).round(2).astype('str') + " hours - Please Confirm",
        "Short Lunch Break: " + (cov_driver_hours['total_hours_sum'] - (cov_driver_hours['reg_hours_sum'] + cov_driver_hours['ot_hours_sum'] + cov_driver_hours['dt_hours_sum'])).round(2).astype('str') + " hours",
        "PODs During Lunch: " + cov_driver_hours['lunch_pod_count'].astype('int64').astype('str')
    ]
    cov_driver_hours['lunch_errors'] = np.select(conditions, choices, default=None)

    conditions = [
        (((cov_driver_hours['dep_no'] == "55") | ((cov_driver_hours['dep_no'] == "20") & (cov_driver_hours['title'].str.contains(pat="relief", case=False, regex=False)))) & (cov_driver_hours['first_clock_in'] > cov_driver_hours['first_pod_10'])),
        (((cov_driver_hours['dep_no'] == "55") | ((cov_driver_hours['dep_no'] == "20") & (cov_driver_hours['title'].str.contains(pat="relief", case=False, regex=False)))) & (cov_driver_hours['last_clock_out'] < cov_driver_hours['last_pod_10']))
    ]
    choices = [
        "Deliveries Before Clock-In",
        "Deliveries After Clock-Out"
    ]
    cov_driver_hours['pod_errors'] = np.select(conditions, choices, default=None)

    cov_driver_hours['error_checks'] = (cov_driver_hours['clock_in_clock_out_errors'].notna()) | (cov_driver_hours['lunch_errors'].notna()) | (cov_driver_hours['pod_errors'].notna()) | (cov_driver_hours['end_of_shift_protocol'].notna())

    cov_driver_hours.rename(columns={'final_hub' : 'hub', 'final_name' : 'name'}, inplace=True)
    
    calculated_driver_data = cov_driver_hours.loc[:, [
        'dep_no',
        'status',
        'name',
        'hub', 
        'clock_in',
        'clock_out',
        'lunch_start',
        'lunch_end',
        'adj_lunch_start',
        'adj_lunch_end',
        'sorting_start',
        'sorting_end',
        'pickup_dropoff_start',
        'pickup_dropoff_end',
        'line_haul_start',
        'line_haul_end',
        'vehicle_maintenance_start',
        'vehicle_maintenance_end',
        'total_hours',
        'reg_hours',
        'ot_hours',
        'dt_hours',
        'final_lunch_time',
        'sorting_time',
        'pickup_dropoff_time',
        'line_haul_time',
        'vehicle_maintenance_time',
        'total_applicable_hours',
        'min_starting_lunch_time',
        'min_ending_lunch_time',
        'first_lunch_starting_time',
        'first_lunch_ending_time',
        'time_of_first_lunch',
        'second_lunch_starting_time',
        'second_lunch_ending_time',
        'time_of_second_lunch',
        'running_break_time',
        'total_breaks_for_day',
        'total_break_time',
        'total_break_time_non_production',
        'total_break_time_production',
        'date',
        'week_ending',
        'driver_code',
        'route',
        'first_clock_in',
        'last_clock_out',
        'on_time_percent',
        'first_pod_1',
        'first_pod_2',
        'first_pod_3',
        'first_pod_4',
        'first_pod_5',
        'first_pod_6',
        'first_pod_7',
        'first_pod_8',
        'first_pod_9',
        'first_pod_10',
        'last_pod_10',
        'last_pod_9',
        'last_pod_8',
        'last_pod_7',
        'last_pod_6',
        'last_pod_5',
        'last_pod_4',
        'last_pod_3',
        'last_pod_2',
        'last_pod_1',
        'first_pod_name',
        'first_pod_to_use',
        'last_pod_to_use',
        'second_to_last_pod',
        'last_pod',
        'non_productive_time_before_delivery',
        'delivery_time',
        'non_productive_time_after_delivery',
        'production_time_converted_to_non_production_time',
        'total_delivery_time',
        'total_non_productive_time',
        'total_delivery_time_wo_breaks',
        'total_non_productive_time_wo_breaks',
        'regular_non_productive_time',
        'regular_production_time',
        'stop_count',
        'sdd_signature_time_deduction',
        'final_stop_count',
        'stop_count_old_model',
        'full_stop_rate',
        'vape_stops',
        'discounted_stops',
        'stops_per_hour_est_productivity',
        'final_stops_per_hour',
        'non_production_rate',
        'route_supplemental_pay',
        'total_production_pay',
        'effective_production_rate_per_day',
        'adjustment_for_non_productive_rate',
        'total_route_compensation',
        'total_non_production_pay_wo_breaks',
        'effective_production_rate_for_wage_statement',
        'regular_rate_calculation',
        'gross_earnings',
        'package_count',
        'amount_charged',
        'production_base_rate',
        'production_rate',
        'daily_amount',
        'weekly_regular_rate_prod',
        'gross_earnings_prod',
        'combined_gross_earnings',
        'earnings_code',
        'combined_stops',
        'avg_stops_weekday',
        'squared_difference_stops',
        'stops_standard_devation',
        'avg_productivity_weekday',
        'squared_difference',
        'productivity_standard_devation',
        'multiline',
        'include_in_production_summary',
        'pay_period_filter',
        'driving_days',
        'time_before_delivery_driving_only',
        'time_after_delivery_driving_only',
        'morning_warehouse_sort',
        'additional_morning_warehouse_sort',
        'final_additional_morning_warehouse_sort_hours',
        'route_sort',
        'final_route_sort_hours',
        'final_evening_sort_hours',
        'final_weekend_sort_hours',
        'morning_warehouse_sort_cost',
        'route_sort_cost',
        'evening_sort_cost',
        'weekend_sort_cost',
        'ot_production',
        'ot_non_production',
        'dt_production',
        'dt_non_production',
        'ot_cost',
        'dt_cost',
        'weekend_filter',
        'new_hub',
        'region',
        'spd',
        'ppd',
        'sph',
        'pod_difference_to_show',
        'cost_per_stop',
        'cost_per_stop_wo_other_jobs',
        'jobs_after_delivery',
        'hire_date']]

    calculated_driver_data.insert(0, 'report_id', report_id)

    delete_prev_report_id_query = text('''
        DELETE
        FROM data_analytics_reporting.calculated_driver_data
        WHERE report_id LIKE '{}%%'
    '''.format(report_id[:5]))

    with dbEngine.connect() as con:
        con.execute(delete_prev_report_id_query)

    insert_calculated_driver_data_with_progress(calculated_driver_data)


    print("Updating driver errors...")

    if not secondary_driver_hours:
        with dbEngine.connect() as con:
            con.execute(text("DELETE FROM data_analytics_reporting.current_driver_errors"))

    main_errors = cov_driver_hours.loc[cov_driver_hours['status'] == "A"]

    main_errors = pd.concat([main_errors, stops_no_hours_list], ignore_index=True)

    if full_errors:
        main_errors = main_errors.loc[(main_errors['clock_in_clock_out_errors'].notnull()) | (main_errors['lunch_errors'].notnull()) | (main_errors['pod_errors'].notnull()) | (main_errors['end_of_shift_protocol'].notnull())]
        main_errors.loc[main_errors['lunch_errors'].isna(), ['lunch_start', 'lunch_end']] = pd.NaT
        main_errors.loc[(main_errors['pod_errors'].isna()) & (main_errors['end_of_shift_protocol'].isna()), ['first_pod_1', 'first_pod_2', 'first_pod_10', 'last_pod_10', 'last_pod_2', 'last_pod_1']] = pd.NaT
        
    else:
        main_errors = main_errors.loc[(main_errors['clock_in_clock_out_errors'].notna()) | (main_errors['lunch_errors'].str.contains(pat="^[Excessive|High]", regex=True, na=False)) | (main_errors['pod_errors'].notna()) | (main_errors['end_of_shift_protocol'].str.contains(pat="Fix", regex=True, na=False))]
        main_errors.loc[~main_errors['lunch_errors'].str.contains(pat="^[Excessive|High]", regex=True, na=False), ['lunch_start', 'lunch_end']] = pd.NaT
        main_errors.loc[~main_errors['lunch_errors'].str.contains(pat="^[Excessive|High]", regex=True, na=False), 'lunch_errors'] = np.nan
        main_errors.loc[~main_errors['end_of_shift_protocol'].str.contains(pat="Fix", regex=True, na=False), 'end_of_shift_protocol'] = np.nan
        main_errors.loc[(main_errors['pod_errors'].isna() & (main_errors['end_of_shift_protocol'].isna())), ['first_pod_1', 'first_pod_2', 'first_pod_10', 'last_pod_10', 'last_pod_2', 'last_pod_1']] = pd.NaT

    main_errors = main_errors.loc[:, ['name', 'hub', 'route', 'date', 'clock_in', 'clock_out', 'reg_hours', 'ot_hours', 'dt_hours', 'clock_in_clock_out_errors', 'lunch_start', 'lunch_end', 'lunch_errors', 'first_pod_1', 'first_pod_2', 'first_pod_10', 'last_pod_10', 'last_pod_2', 'last_pod_1', 'pod_errors', 'end_of_shift_protocol']]
    main_errors.rename(columns={'first_pod_1' : 'first_pod', 'first_pod_2' : 'second_pod', 'first_pod_10' : 'tenth_pod', 'last_pod_10' : 'tenth_last_pod', 'last_pod_2' : 'second_last_pod', 'last_pod_1': 'last_pod'}, inplace=True)
    main_errors.sort_values(by=['hub', 'name', 'date', 'clock_in', 'clock_out', 'reg_hours', 'ot_hours', 'dt_hours'], inplace=True)

    main_errors.rename(columns={'name' : 'Name', 'hub' : 'Hub', 'route' : 'Route', 'date' : 'Date', 'clock_in' : 'Clock-In', 'clock_out' : 'Clock-Out', 'reg_hours' : 'Reg Hours', 'ot_hours' : 'OT Hours', 'dt_hours' : 'DT Hours', 'lunch_start' : 'Lunch Start', 'lunch_end' : 'Lunch End', 'first_pod' : '1st POD', 'second_pod' : '2nd POD', 'tenth_pod' : '10th POD', 'tenth_last_pod' : '10th Last POD', 'second_last_pod' : '2nd Last POD', 'last_pod' : 'Last POD', 'clock_in_clock_out_errors' : 'Clock-In / Clock-Out Errors', 'lunch_errors' : 'Lunch Errors', 'pod_errors' : 'POD Errors', 'end_of_shift_protocol' : 'End of Shift Protocol'}, inplace=True)

    existing_data = pd.DataFrame()

    smart = Smartsheet(access_token=os.environ.get('SMARTSHEET_ACCESS_TOKEN'))

    def sheet_to_dataframe(sheet):
        col_names = [col.title for col in sheet.columns]
        rows = []
        for row in sheet.rows:
            cells = []
            for cell in row.cells:
                cells.append(cell.value)
            rows.append(cells)
        data_frame = pd.DataFrame(rows, columns=col_names)
        return data_frame

    def save_existing_data(sheet):
        
        nonlocal existing_data
        existing_hub_data = sheet_to_dataframe(sheet)

        existing_hub_data['Approved'].replace({"True" : True, np.nan : False}, inplace=True)
        existing_hub_data['Name'] = existing_hub_data['Name'].str.upper()
        existing_hub_data['Date'] = pd.to_datetime(existing_hub_data['Date'], format='%Y-%m-%d', errors='coerce')
        existing_hub_data['Clock-In'] = pd.to_datetime(existing_hub_data['Clock-In'], format='%m/%d/%Y %H:%M')
        existing_hub_data['Clock-Out'] = pd.to_datetime(existing_hub_data['Clock-Out'], format='%m/%d/%Y %H:%M')
        existing_hub_data['Lunch Start'] = pd.to_datetime(existing_hub_data['Lunch Start'], format='%m/%d/%Y %H:%M')
        existing_hub_data['Lunch End'] = pd.to_datetime(existing_hub_data['Lunch End'], format='%m/%d/%Y %H:%M')
        existing_hub_data['1st POD'] = pd.to_datetime(existing_hub_data['1st POD'], format='%H:%M')
        existing_hub_data['1st POD'] = existing_hub_data.apply(lambda x: datetime(x['Date'].year, x['Date'].month, x['Date'].day, int(x['1st POD'].hour), int(x['1st POD'].minute)) if pd.notna(x['1st POD']) else np.nan, axis=1)
        existing_hub_data['2nd POD'] = pd.to_datetime(existing_hub_data['2nd POD'], format='%H:%M')
        existing_hub_data['2nd POD'] = existing_hub_data.apply(lambda x: datetime(x['Date'].year, x['Date'].month, x['Date'].day, int(x['2nd POD'].hour), int(x['2nd POD'].minute)) if pd.notna(x['2nd POD']) else np.nan, axis=1)
        existing_hub_data['10th POD'] = pd.to_datetime(existing_hub_data['10th POD'], format='%H:%M')
        existing_hub_data['10th POD'] = existing_hub_data.apply(lambda x: datetime(x['Date'].year, x['Date'].month, x['Date'].day, int(x['10th POD'].hour), int(x['10th POD'].minute)) if pd.notna(x['10th POD']) else np.nan, axis=1)
        existing_hub_data['10th Last POD'] = pd.to_datetime(existing_hub_data['10th Last POD'], format='%H:%M')
        existing_hub_data['10th Last POD'] = existing_hub_data.apply(lambda x: datetime(x['Date'].year, x['Date'].month, x['Date'].day, int(x['10th Last POD'].hour), int(x['10th Last POD'].minute)) if pd.notna(x['10th Last POD']) else np.nan, axis=1)
        existing_hub_data['2nd Last POD'] = pd.to_datetime(existing_hub_data['2nd Last POD'], format='%H:%M')
        existing_hub_data['2nd Last POD'] = existing_hub_data.apply(lambda x: datetime(x['Date'].year, x['Date'].month, x['Date'].day, int(x['2nd Last POD'].hour), int(x['2nd Last POD'].minute)) if pd.notna(x['2nd Last POD']) else np.nan, axis=1)
        existing_hub_data['Last POD'] = pd.to_datetime(existing_hub_data['Last POD'], format='%H:%M')
        existing_hub_data['Last POD'] = existing_hub_data.apply(lambda x: datetime(x['Date'].year, x['Date'].month, x['Date'].day, int(x['Last POD'].hour), int(x['Last POD'].minute)) if pd.notna(x['Last POD']) else np.nan, axis=1)

        existing_hub_data = existing_hub_data.loc[existing_hub_data['Approved']]

        existing_data = pd.concat([existing_data, existing_hub_data], ignore_index=True)

    def delete_existing_data(sheet, chunk_interval=300):
        rows_to_delete = [row.id for row in sheet.rows]
        for x in range(0, len(rows_to_delete), chunk_interval):
            smart.Sheets.delete_rows(sheet.id, rows_to_delete[x:x + chunk_interval])

    smartsheet_ids = {
        '847949659590532' : ['BAY'],
        '3380674694172548' : ['CAJ', 'ONT', 'ONT/CAJ', 'RIV'],
        '8447224274964356' : ['CEN'],
        '4787328023220100' : ['CNG', 'LAN', 'ONX', 'VLY'],
        '706722544963460' : ['CST'],
        '3521884628930436' : ['DUB', 'NOR'],
        '7636244255166340' : ['EST'],
        '6195699339186052' : ['FAT', 'VIS'],
        '2044381519368068' : ['ING'],
        '2817999618658180' : ['MOV'],
        '7320628583419780' : ['NSD', 'SND'],
        '846884507701124' : ['PMF'],
        '3662252750098308' : ['SFS'],
        '848147228086148' : ['SJC'],
        '1973987005386628' : ['TUS'],
        '7040287478075268' : ['WSC']
    }

    for sheet_id in smartsheet_ids.keys():
        
        sheet = smart.Sheets.get_sheet(sheet_id, page_size=5000)

        save_existing_data(sheet)

        if not secondary_driver_hours:
            delete_existing_data(sheet)

    existing_data_report_id = datetime.now()
    existing_data.insert(0, 'report_id', existing_data_report_id)

    existing_data.to_sql('smartsheet_error_approvals', dbEngine, schema='data_analytics_reporting', if_exists='append', index=False)
    smartsheet_errors_approvals = pd.read_sql("SELECT * FROM data_analytics_reporting.smartsheet_error_approvals", dbEngine)

    smartsheet_errors_approvals.drop_duplicates(subset=['Name', 'Route', 'Clock-In', 'Clock-Out', 'Lunch Start', 'Lunch End'], keep='last', inplace=True)

    smartsheet_errors_approvals['Lunch Start'].fillna(value=datetime(1900, 1, 1, 0, 0, 0, 0), inplace=True)
    smartsheet_errors_approvals['Lunch End'].fillna(value=datetime(1900, 1, 1, 0, 0, 0, 0), inplace=True)
    smartsheet_errors_approvals['Approved'] = smartsheet_errors_approvals['Approved'].astype(bool)

    smartsheet_errors_approvals = smartsheet_errors_approvals.set_index(keys=['Name', 'Route', 'Clock-In', 'Clock-Out', 'Lunch Start', 'Lunch End'], verify_integrity=True)

    main_errors['Clock-In'] = main_errors['Clock-In'].dt.floor('Min')
    main_errors['Clock-Out'] = main_errors['Clock-Out'].dt.floor('Min')
    main_errors['Lunch Start'].fillna(value=datetime(1900, 1, 1, 0, 0, 0, 0), inplace=True)
    main_errors['Lunch End'].fillna(value=datetime(1900, 1, 1, 0, 0, 0, 0), inplace=True)

    main_errors = main_errors.join(smartsheet_errors_approvals['Approved'], on=['Name', 'Route', 'Clock-In', 'Clock-Out', 'Lunch Start', 'Lunch End'])

    main_errors['Approved'].fillna(value=False, inplace=True)
    main_errors['Lunch Start'].replace({datetime(1900, 1, 1, 0, 0, 0, 0) : pd.NaT}, inplace=True)
    main_errors['Lunch End'].replace({datetime(1900, 1, 1, 0, 0, 0, 0) : pd.NaT}, inplace=True)
    main_errors = main_errors.loc[~(main_errors['Approved']) | main_errors['Clock-In / Clock-Out Errors'].astype('object').str.contains(pat="fix", case=False, na=False) | main_errors['Lunch Errors'].astype('object').str.contains(pat="fix", case=False, na=False) | main_errors['End of Shift Protocol'].astype('object').str.contains(pat="fix", case=False, na=False)]

    insert_driver_errors_with_progress(main_errors)
    
    main_errors['Approved'] = False
    main_errors['Name'] = main_errors['Name'].str.title()
    main_errors['Date'] = main_errors['Date'].dt.strftime("%Y-%m-%d")
    main_errors['Clock-In'] = main_errors['Clock-In'].dt.strftime("%m/%d/%Y %H:%M")
    main_errors['Clock-Out'] = main_errors['Clock-Out'].dt.strftime("%m/%d/%Y %H:%M")
    main_errors['Lunch Start'] = main_errors['Lunch Start'].dt.strftime("%m/%d/%Y %H:%M")
    main_errors['Lunch End'] = main_errors['Lunch End'].dt.strftime("%m/%d/%Y %H:%M")
    main_errors['1st POD'] = main_errors['1st POD'].dt.strftime("%H:%M")
    main_errors['2nd POD'] = main_errors['2nd POD'].dt.strftime("%H:%M")
    main_errors['10th POD'] = main_errors['10th POD'].dt.strftime("%H:%M")
    main_errors['10th Last POD'] = main_errors['10th Last POD'].dt.strftime("%H:%M")
    main_errors['2nd Last POD'] = main_errors['2nd Last POD'].dt.strftime("%H:%M")
    main_errors['Last POD'] = main_errors['Last POD'].dt.strftime("%H:%M")

    for sheet_id, hubs in smartsheet_ids.items():
        
        sheet = smart.Sheets.get_sheet(sheet_id, page_size=5000)

        data_dict = main_errors.loc[main_errors['Hub'].isin(hubs)].fillna('').to_dict('index')

        column_map = {}

        for column in sheet.columns:
            column_map[column.title] = column.id

        rowsToAdd = []

        # goal is to create a row for each object in data_dict
        for i, i in data_dict.items():

            # create a new row object
            new_row = smart.models.Row()
            new_row.to_top = True

            # for each key value pair, create & add a cell to the row object
            for k, v in i.items():

                # create the cell object and populate with value
                new_cell = smart.models.Cell()
                new_cell.column_id = column_map[k]
                new_cell.value = v

                # add the cell object to the row object
                new_row.cells.append(new_cell)

            # add the row object to the collection of rows
            rowsToAdd.append(new_row)

        # add the collection of rows to the sheet in Smartsheet
        result = smart.Sheets.add_rows(sheet_id, rowsToAdd)

    dbEngine.dispose()