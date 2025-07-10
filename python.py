#!/usr/bin/env python3

import numpy as np
import pandas as pd
import psycopg2
import pyodbc
from datetime import datetime, timedelta, time
import re
from sqlalchemy import text
from tqdm import tqdm

import get_db_connection
from update_daily_delivery_data_logging_config import logger

def update_delivery_data(date_range, udpate_payroll_otp_data=True):
    
    dbEngine = get_db_connection.get_mysql_engine()

    # https://stackoverflow.com/questions/39494056/progress-bar-for-pandas-dataframe-to-sql
    def chunker(seq, size):
        # from http://stackoverflow.com/a/434328
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    def insert_payroll_with_progress(df):
        chunksize = int(len(df) / min(len(df), 20)) # 5%
        with tqdm(total=len(df)) as pbar:
            for i, cdf in enumerate(chunker(df, chunksize)):
                replace = "replace" if i == 0 else "append"
                cdf.to_sql('payroll_delivery_data', dbEngine, schema='data_analytics_reporting', if_exists='append', index=False)
                pbar.update(chunksize)
    
    def insert_otp_with_progress(df):
        chunksize = int(len(df) / min(len(df), 20)) # 5%
        with tqdm(total=len(df)) as pbar:
            for i, cdf in enumerate(chunker(df, chunksize)):
                replace = "replace" if i == 0 else "append"
                cdf.to_sql('otp_data', dbEngine, schema='data_analytics_reporting', if_exists='append', index=False)
                pbar.update(chunksize)
    
    def insert_cov_with_progress(df):
        chunksize = int(len(df) / min(len(df), 20)) # 5%
        with tqdm(total=len(df)) as pbar:
            for i, cdf in enumerate(chunker(df, chunksize)):
                replace = "replace" if i == 0 else "append"
                cdf.to_sql('summarized_delivery_data', dbEngine, schema='data_analytics_reporting', if_exists='append', index=False)
                pbar.update(chunksize)
    
    def insert_other_with_progress(df):
        chunksize = int(len(df) / min(len(df), 20)) # 5%
        with tqdm(total=len(df)) as pbar:
            for i, cdf in enumerate(chunker(df, chunksize)):
                replace = "replace" if i == 0 else "append"
                cdf.to_sql('other_delivery_data', dbEngine, schema='data_analytics_reporting', if_exists='append', index=False)
                pbar.update(chunksize)
    
    def insert_detailed_with_progress(df):
        chunksize = int(len(df) / min(len(df), 20)) # 5%
        with tqdm(total=len(df)) as pbar:
            for i, cdf in enumerate(chunker(df, chunksize)):
                replace = "replace" if i == 0 else "append"
                cdf.to_sql('detailed_delivery_data', dbEngine, schema='data_analytics_reporting', if_exists='append', index=False)
                pbar.update(chunksize)
    

    service_type_due_times_dict = {
        "09A":time(9, 0, 0), "10A":time(10, 0, 0), "12E":time(12, 0, 0), "12P":time(12, 0, 0), "13P":time(13, 0, 0), "15E":time(15, 0, 0), "15P":time(15, 0, 0), "AFTERNOON":time(15, 30, 0), "CUSOTM1:30":time(13, 30, 0), "CUSTOM1:30":time(13, 30, 0), "CUSTOM1030":time(10, 30, 0), "CUSTOM11AM":time(11, 0, 0), "CUSTOM1PM":time(13, 0, 0), "CUSTOM2PM":time(14, 0, 0), "CUSTOM5:00":time(17, 0, 0), "CUSTOM8:30":time(8, 30, 0), "CUSTOM8PM":time(20, 0, 0), "DELIVER IT 08:30":time(8, 30, 0), "DELIVER IT 09:30":time(9, 30, 0), "DELIVER IT 10:30":time(10, 30, 0), "DELIVER IT 12:00":time(12, 0, 0), "DELIVER IT 12P EXP":time(12, 0, 0), "DELIVER IT 13:00":time(13, 0, 0), "DELIVER IT 15:30":time(15, 30, 0), "DELIVER IT 15P EXP":time(12, 0, 0), "EXPRESS OVERNITE 12PM":time(12, 0, 0), "EXPRESS OVERNITE 5PM":time(17, 0, 0), "NOON-IT!":time(12, 0, 0)
    }

    holiday_list = [
        datetime(2025, 1, 1), datetime(2025, 5, 26), datetime(2025, 7, 4), datetime(2025, 9, 1), datetime(2025, 11, 27), datetime(2025, 12, 25)
    ]
    
    pickup_internal_codes = [
        '10000', '10049', '10051', '10053', '10055', '79000', '100100', '100463', '100935', '101999', '102292', '102378', '102425', '102454', '102458', '102487', '102491', '102494', '102495', '102629', '102630', '102676', '102677', '102708', '102709', '102761', '102762', '102830', '102836', '103009', '103010', '103026', '103326', '103434', '103463', '103491', '103522', '989898998', '989898999'
    ]

    ace_codes_to_remove = [
        '99004', '44145', '99011', '44144', '99442', '99001', '99280', '44132', '99173', '99008', '99000', '99445', '99608', '99015', '99017', '99007', '92272', '99583', '99013', '99010', '99005', '99006', '99635', '99014', '99002', '99016', '99009', '99018', '99174'
    ]

    transit_linehaul_revenue_codes = [
        '103701', '103699'
    ]

    monthly_billing_codes = [
        '102664', '103645', '103262'
    ]

    adp_paychex_codes = [
        '102214', '102215', '102252', '102253', '102289', '102392', '102392W2', '102394', '102449', '102499', '102501', '102501W2', '102511', '102515', '102738', '103349', '6466', 'PA0080', 'PA0087', 'PA0580', 'PA0580'
    ]
    

    if udpate_payroll_otp_data:
    
        ## Delivery Data
        print("Updating Delivery Data...")
        logger.info("Delivery Data")
        
        delivery_data_query = '''
            SELECT
                id,
                order_id,
                order_number,
                assignee_name,
                pod_datetime,
                event_name,
                event

            FROM
                integrity.bean_pods
            
            WHERE
                pod_datetime >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
        '''

        delivery_data = pd.read_sql(delivery_data_query, dbEngine)

        delivery_data['delivery_time'] = delivery_data['pod_datetime'].dt.tz_localize('UTC').dt.tz_convert('US/Pacific').dt.tz_localize(None)
        delivery_data['delivery_date'] = delivery_data['delivery_time'].dt.normalize()

        bpaa = delivery_data.loc[delivery_data['event'] == "Attempt"].groupby(['order_number', 'assignee_name', 'delivery_date'])[['id', 'delivery_time']].min().reset_index()
        bpad = delivery_data.loc[delivery_data['event'] == "Delivered"].groupby(['order_number'])[['id', 'delivery_time']].min().reset_index()
        bpa = bpaa.join(bpad.set_index(keys=['order_number']), on=['order_number'], rsuffix="_bpad")
        bpa = bpa.loc[((bpa['delivery_time'] < bpa['delivery_time_bpad']) | (bpa['delivery_time_bpad'].isna())), ['order_number', 'id']]
        bp = pd.concat([bpa, bpad.loc[:, ['order_number', 'id']]], axis=0)
        bp_id_list = bp['id'].to_list()

        delivery_data = delivery_data.loc[delivery_data['id'].isin(bp_id_list)]

        bean_pods_order_id_list = str(tuple(delivery_data.loc[delivery_data['order_id'].notna()]['order_id'].drop_duplicates().astype('str').str[:-2].to_list())).replace("'","")

        logger.info("doj")
        doj_query = '''
            SELECT
                order_id,
                name AS 'delivery_name',
                address_one AS 'delivery_address_one',
                address_two AS 'delivery_address_two',
                city AS 'delivery_city',
                state AS 'delivery_state',
                zip AS 'delivery_zip'

            FROM
                integrity.order_jobs
            
            WHERE
                    job_type = 'D'
                AND order_id IN {}
        '''.format(bean_pods_order_id_list)

        doj = pd.read_sql(doj_query, dbEngine).set_index(keys=['order_id'])

        logger.info("poj")
        poj_query = '''
            SELECT
                order_id AS 'poj_order_id',
                name AS 'pickup_name',
                address_one AS 'pickup_address_one',
                address_two AS 'pickup_address_two',
                city AS 'pickup_city',
                state AS 'pickup_state',
                zip AS 'pickup_zip'

            FROM
                integrity.order_jobs
            
            WHERE
                    job_type = 'P'
                AND order_id IN {}
        '''.format(bean_pods_order_id_list)

        poj = pd.read_sql(poj_query, dbEngine).set_index(keys=['poj_order_id'])

        logger.info("orders")
        orders_query = '''
            SELECT
                id,
                order_date,
                reference_one,
                reference_two,
                notes,
                pod_name,
                customer_id,
                service_id
            
            FROM
                integrity.orders
            
            WHERE
                id IN {}
        '''.format(bean_pods_order_id_list)

        orders = pd.read_sql(orders_query, dbEngine).set_index(keys=['id'])

        orders_customer_id_list = tuple(orders['customer_id'].drop_duplicates().astype('str').to_list())
        orders_service_id_list = tuple(orders['service_id'].drop_duplicates().astype('str').to_list())

        logger.info("customers")
        customers_query = '''
            SELECT
                id,
                account_number AS 'customer_code',
                customer_name
            
            FROM
                integrity.customers
            
            WHERE
                id IN {}
        '''.format(orders_customer_id_list)

        customers = pd.read_sql(customers_query, dbEngine).set_index(keys=['id'])

        logger.info("op")
        op_query = '''
            SELECT
                order_id,
                weight

            FROM
                integrity.order_pieces
            
            WHERE
                order_id IN {}
        '''.format(bean_pods_order_id_list)

        op = pd.read_sql(op_query, dbEngine).set_index(keys=['order_id'])

        logger.info("services")
        services_query = '''
            SELECT
                id,
                name AS 'service_type'
            
            FROM
                integrity.services
            
            WHERE
                id IN {}
        '''.format(orders_service_id_list)

        services = pd.read_sql(services_query, dbEngine).set_index(keys=['id'])

        logger.info("zips")
        zips_query = '''
            SELECT
                zip,
                terminal_id
            
            FROM integrity.zip_codes zc1
            
            WHERE
                zc1.id = (
                    SELECT zc2.id
                    FROM integrity.zip_codes zc2
                    WHERE
                        zc1.zip = zc2.zip
                    ORDER BY zc2.updated_at DESC
                    LIMIT 1
                )
        '''

        zips = pd.read_sql(zips_query, dbEngine).set_index(keys=['zip'])

        zips_terminal_id_list = tuple(zips['terminal_id'].drop_duplicates().astype('str').to_list())

        logger.info("terminals")
        terminals_query = '''
            SELECT
                id,
                name AS 'terminal'
            
            FROM
                integrity.terminals
            
            WHERE
                id IN {}
        '''.format(zips_terminal_id_list)

        terminals = pd.read_sql(terminals_query, dbEngine).set_index(keys=['id'])

        logger.info("amount_charged")
        amount_charged_query = '''
            SELECT
                order_id,
                amount AS 'amount_charged'
            
            FROM integrity.order_fees

            WHERE
                    order_id IN {}
                AND name = 'AmountCharged'
            
            ORDER BY
                created_at
        '''.format(bean_pods_order_id_list)

        amount_charged = pd.read_sql(amount_charged_query, dbEngine)
        amount_charged = amount_charged.drop_duplicates(subset=['order_id'], keep='last').set_index(keys=['order_id'])

        logger.info("service_amount")
        service_amount_query = '''
            SELECT
                order_id,
                amount AS 'service_amount'
            
            FROM integrity.order_fees

            WHERE
                    order_id IN {}
                AND name = 'Service'
            
            ORDER BY
                created_at
        '''.format(bean_pods_order_id_list)

        service_amount = pd.read_sql(service_amount_query, dbEngine)
        service_amount = service_amount.drop_duplicates(subset=['order_id'], keep='last').set_index(keys=['order_id'])

        logger.info("fuel_fee")
        fuel_fee_query = '''
            SELECT
                order_id,
                amount AS 'fuel_fee'
            
            FROM integrity.order_fees

            WHERE
                    order_id IN {}
                AND name = 'Fuel'
            
            ORDER BY
                created_at
        '''.format(bean_pods_order_id_list)

        fuel_fee = pd.read_sql(fuel_fee_query, dbEngine)
        fuel_fee = fuel_fee.drop_duplicates(subset=['order_id'], keep='last').set_index(keys=['order_id'])

        logger.info("sort_event")
        sort_event_query = '''
            SELECT
                order_id,
                event_timestamp AS 'sortscan_time',
                event_note AS 'sortscan_note'
            
            FROM integrity.order_events

            WHERE
                    order_id IN {}
                AND event_id = 6
            
            ORDER BY
                order_id,
                created_at
        '''.format(bean_pods_order_id_list)

        sort_event = pd.read_sql(sort_event_query, dbEngine)
        sort_event = sort_event.drop_duplicates(subset=['order_id'], keep='first').set_index(keys=['order_id'])

        logger.info("arrive_hub_event")
        arrive_hub_event_query = '''
            SELECT
                order_id,
                event_timestamp AS 'hub_arrival_time',
                event_note AS 'hub_arrival_note'
            
            FROM integrity.order_events

            WHERE
                    order_id IN {}
                AND event_id = 8
            
            ORDER BY
                order_id,
                created_at
        '''.format(bean_pods_order_id_list)

        arrive_hub_event = pd.read_sql(arrive_hub_event_query, dbEngine)
        arrive_hub_event = arrive_hub_event.drop_duplicates(subset=['order_id'], keep='first').set_index(keys=['order_id'])

        logger.info("out_for_delivery_event")
        out_for_delivery_event_query = '''
            SELECT
                order_id,
                event_timestamp AS 'out_for_delivery_time',
                event_note AS 'out_for_delivery_note'
            
            FROM integrity.order_events

            WHERE
                    order_id IN {}
                AND event_id = 10
            
            ORDER BY
                order_id,
                created_at
        '''.format(bean_pods_order_id_list)

        out_for_delivery_event = pd.read_sql(out_for_delivery_event_query, dbEngine)
        out_for_delivery_event = out_for_delivery_event.drop_duplicates(subset=['order_id'], keep='first').set_index(keys=['order_id'])

        logger.info("attempted_event")
        attempted_event_query = '''
            SELECT
                order_id,
                event_timestamp AS 'attempted_time',
                event_note AS 'attempted_note'
            
            FROM integrity.order_events

            WHERE
                    order_id IN {}
                AND event_id = 12
            
            ORDER BY
                order_id,
                created_at
        '''.format(bean_pods_order_id_list)

        attempted_event = pd.read_sql(attempted_event_query, dbEngine)
        attempted_event = attempted_event.drop_duplicates(subset=['order_id'], keep='first').set_index(keys=['order_id'])

        logger.info("joins")
        delivery_data = delivery_data.join(doj, on=['order_id'])
        delivery_data = delivery_data.join(poj, on=['order_id'])
        delivery_data = delivery_data.join(orders, on=['order_id'])
        delivery_data = delivery_data.join(customers, on=['customer_id'])
        delivery_data = delivery_data.join(op, on=['order_id'])
        delivery_data = delivery_data.join(services, on=['service_id'])
        delivery_data = delivery_data.join(zips, on=[delivery_data['delivery_zip'].str[:5]])
        delivery_data = delivery_data.join(terminals, on=['terminal_id'])
        delivery_data = delivery_data.join(amount_charged, on=['order_id'])
        delivery_data = delivery_data.join(service_amount, on=['order_id'])
        delivery_data = delivery_data.join(fuel_fee, on=['order_id'])
        delivery_data = delivery_data.join(sort_event, on=['order_id'])
        delivery_data = delivery_data.join(arrive_hub_event, on=['order_id'])
        delivery_data = delivery_data.join(out_for_delivery_event, on=['order_id'])
        delivery_data = delivery_data.join(attempted_event, on=['order_id'])

        delivery_data['amount_charged'] = delivery_data['amount_charged'].replace({"":np.nan}).astype('float')
        delivery_data['service_amount'] = delivery_data['service_amount'].replace({"":np.nan}).astype('float')
        delivery_data['fuel_fee'] = delivery_data['fuel_fee'].replace({"":np.nan}).astype('float')
        
        logger.info("Convert times")
        conditions = [
            (delivery_data['delivery_zip'].str[:5].isin(['85901', '85902', '85911', '85912', '85923', '85926', '85928', '85929', '85930', '85931', '85933', '85934', '85935', '85937', '85939', '85941', '85942', '86025', '86029', '86030', '86031', '86032', '86033', '86034', '86039', '86042', '86043', '86047', '86054', '86510', '86520'])),
            (delivery_data['delivery_state'].str.upper() == "AZ")
        ]
        choices = [
            "US/Mountain",
            "America/Phoenix"
        ]
        delivery_data['time_zone'] = np.select(conditions, choices, default="US/Pacific")

        delivery_data['order_date'] = [dt.tz_localize("UTC").tz_convert(tz).tz_localize(None) for dt,tz in zip(delivery_data['order_date'], delivery_data['time_zone'])]
        delivery_data['delivery_time'] = [dt.tz_localize("UTC").tz_convert(tz).tz_localize(None) for dt,tz in zip(delivery_data['pod_datetime'], delivery_data['time_zone'])]
        delivery_data['sortscan_time'] = [dt.tz_localize("UTC").tz_convert(tz).tz_localize(None) for dt,tz in zip(delivery_data['sortscan_time'], delivery_data['time_zone'])]
        delivery_data['hub_arrival_time'] = [dt.tz_localize("UTC").tz_convert(tz).tz_localize(None) for dt,tz in zip(delivery_data['hub_arrival_time'], delivery_data['time_zone'])]
        delivery_data['out_for_delivery_time'] = [dt.tz_localize("UTC").tz_convert(tz).tz_localize(None) for dt,tz in zip(delivery_data['out_for_delivery_time'], delivery_data['time_zone'])]
        delivery_data['attempted_time'] = [dt.tz_localize("UTC").tz_convert(tz).tz_localize(None) for dt,tz in zip(delivery_data['attempted_time'], delivery_data['time_zone'])]

        logger.info("On-time")
        conditions = [
            delivery_data['sortscan_note'].str.contains(pat="central coast", regex=False, na=False, case=False),
            delivery_data['sortscan_note'].str.contains(pat="cerritos", regex=False, na=False, case=False),
            delivery_data['sortscan_note'].str.contains(pat="manteca", regex=False, na=False, case=False),
            delivery_data['sortscan_note'].str.contains(pat="NOT_FOUND", regex=False, na=False, case=False),
            delivery_data['sortscan_note'].str.contains(pat="san diego", regex=False, na=False, case=False)
        ]
        choices = [
            "CST",
            "CER",
            "CEN",
            "Not Found",
            "SND"
        ]
        delivery_data['sort_hub'] = np.select(conditions, choices, default=delivery_data['sortscan_note'].str[:3].str.upper())

        conditions = [
            delivery_data['customer_code'].isin(pickup_internal_codes),
            delivery_data['customer_code'].isin(ace_codes_to_remove),
            (delivery_data['customer_code'] == "102705"),      # Mistake
            delivery_data['customer_code'].isin(transit_linehaul_revenue_codes),
            delivery_data['customer_code'].isin(monthly_billing_codes),
            (
                    ((delivery_data['amount_charged'].isna())
                |   (delivery_data['amount_charged'] < 1))
                &   (~delivery_data['customer_code'].isin(adp_paychex_codes))
            )
        ]
        choices = [
            -1,
            -2,
            -2,
            0,
            0,
            0
        ]
        delivery_data['package_operational'] = np.select(conditions, choices, default=1)

        conditions = [
            delivery_data['customer_code'].isin(pickup_internal_codes),
            delivery_data['customer_code'].isin(ace_codes_to_remove),
            (delivery_data['customer_code'] == "102705"),      # Mistake
            delivery_data['customer_code'].isin(transit_linehaul_revenue_codes),
            delivery_data['customer_code'].isin(monthly_billing_codes),
            (
                    (delivery_data['amount_charged'].isna())
                |   (delivery_data['amount_charged'] < 1)
            )
        ]
        choices = [
            -1,
            -2,
            -2,
            0,
            0,
            0
        ]
        delivery_data['package_financial'] = np.select(conditions, choices, default=1)

        conditions = [
            (delivery_data['customer_code'].isin(["103690", "103349", "102252"])),
            ((delivery_data['sortscan_time'].isna()) & (delivery_data['hub_arrival_time'].isna()) & (delivery_data['out_for_delivery_time'].isna())),
            ((delivery_data['terminal'].isin(["PDX", "SEA"])) & (delivery_data['customer_name'].str.contains(pat="NESPRESSO", case=False)) & (delivery_data['hub_arrival_time'].notna()) & (delivery_data['hub_arrival_time'].dt.time >= time(17, 0, 0)) & (delivery_data['hub_arrival_time'].dt.time <= time(23, 59, 59))),
            ((delivery_data['sortscan_time'].notna()) & (delivery_data['sortscan_time'].dt.time < time(4, 0, 0))),
            (delivery_data['sortscan_time'].notna()),
            ((delivery_data['terminal'] == "SFS") & (delivery_data['sortscan_time'].isna()) & (delivery_data['hub_arrival_time'].notna()) & (delivery_data['hub_arrival_time'].dt.time >= time(17, 0, 0)) & (delivery_data['hub_arrival_time'].dt.time <= time(23, 59, 0))),
            ((delivery_data['sortscan_time'].isna()) & (delivery_data['hub_arrival_time'].notna())),
            ((delivery_data['sortscan_time'].isna()) & (delivery_data['hub_arrival_time'].isna()) & (delivery_data['out_for_delivery_time'].notna()))
        ]
        choices = [
            delivery_data['order_date'],
            pd.NaT,
            delivery_data['hub_arrival_time'].dt.date,
            (delivery_data['sortscan_time'].dt.date - timedelta(days=1)),
            delivery_data['sortscan_time'].dt.date,
            delivery_data['hub_arrival_time'].dt.date,
            (delivery_data['hub_arrival_time'].dt.date - timedelta(days=1)),
            (delivery_data['out_for_delivery_time'].dt.date - timedelta(days=1))
        ]
        delivery_data['possession_date'] = pd.to_datetime(np.select(conditions, choices, default=pd.NaT))

        delivery_data = delivery_data.sort_values(by=['delivery_time'])

        delivery_data['attempt_number'] = delivery_data.groupby(['order_number']).cumcount() + 1

        conditions = [
            (delivery_data['customer_code'] == "103537"),
            (((delivery_data['terminal'].isin(["PDX", "SEA"])) & (delivery_data['customer_name'].str.contains(pat="NESPRESSO", case=False)) & (delivery_data['delivery_zip'].str[:3] == "974")) | ((delivery_data['sortscan_time'].notna()) & ((delivery_data['sortscan_time'].dt.time >= time(21, 0, 0)) | (delivery_data['sortscan_time'].dt.time < time(4, 0, 0))) & (delivery_data['terminal'].isin(['BAY', 'CEN', 'DUB', 'NOT', 'OAK', 'SJC', 'WSC'])) & (~delivery_data['customer_name'].str.contains(pat="ADP", case=False)) & (~delivery_data['customer_name'].str.contains(pat="PAYCHEX", case=False))))
        ]
        choices = [
            (delivery_data['attempt_number'] + 2),
            (delivery_data['attempt_number'] + 1)
        ]
        delivery_data['tnt'] = np.select(conditions, choices, default=delivery_data['attempt_number'])

        delivery_data['service_type_due_time'] = delivery_data['service_type'].str.upper().replace(service_type_due_times_dict)
        delivery_data.loc[delivery_data['service_type'].str.upper() == delivery_data['service_type_due_time'], 'service_type_due_time'] = None
        
        conditions = [
            ((delivery_data['customer_name'].str.contains(pat="ADP", case=False)) & (delivery_data['service_type'] == "CUSTOM1:30")),
            ((delivery_data['customer_name'].str.contains(pat="PAYCHEX", case=False)) & (delivery_data['service_type'].isin(["DELIVER IT 12:00", "DELIVER IT 12P EXP", "12P"]))),
            ((delivery_data['customer_name'].str.contains(pat="PAYCHEX", case=False)) & (delivery_data['service_type'].isin(["DELIVER IT 15:30", "DELIVER IT 15P EXP", "15E", "15P"]))),
            (delivery_data['customer_name'].str.contains(pat="ALBERTSON", case=False)),
            (delivery_data['service_type_due_time'].notna())
        ]
        choices = [
            time(15, 30, 0),
            time(12, 30, 0),
            time(15, 30, 0),
            time(15, 30, 0),
            delivery_data['service_type_due_time']
        ]
        delivery_data['due_time'] = np.select(conditions, choices, default=time(23, 59, 59))

        for i in range(1, 18):
            delivery_data['due_date_to_check_{}'.format(i)] = delivery_data['possession_date'] + timedelta(days=i)

        delivery_data['running_attempt_count_1'] = np.where(((~delivery_data['due_date_to_check_1'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_1'].isin(holiday_list))), 1, 0)
        delivery_data['running_attempt_count_2'] = delivery_data['running_attempt_count_1'] + np.where(((~delivery_data['due_date_to_check_2'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_2'].isin(holiday_list))), 1, 0)
        delivery_data['running_attempt_count_3'] = delivery_data['running_attempt_count_2'] + np.where(((~delivery_data['due_date_to_check_3'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_3'].isin(holiday_list))), 1, 0)
        delivery_data['running_attempt_count_4'] = delivery_data['running_attempt_count_3'] + np.where(((~delivery_data['due_date_to_check_4'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_4'].isin(holiday_list))), 1, 0)
        delivery_data['running_attempt_count_5'] = delivery_data['running_attempt_count_4'] + np.where(((~delivery_data['due_date_to_check_5'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_5'].isin(holiday_list))), 1, 0)
        delivery_data['running_attempt_count_6'] = delivery_data['running_attempt_count_5'] + np.where(((~delivery_data['due_date_to_check_6'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_6'].isin(holiday_list))), 1, 0)
        delivery_data['running_attempt_count_7'] = delivery_data['running_attempt_count_6'] + np.where(((~delivery_data['due_date_to_check_7'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_7'].isin(holiday_list))), 1, 0)
        delivery_data['running_attempt_count_8'] = delivery_data['running_attempt_count_7'] + np.where(((~delivery_data['due_date_to_check_8'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_8'].isin(holiday_list))), 1, 0)
        delivery_data['running_attempt_count_9'] = delivery_data['running_attempt_count_8'] + np.where(((~delivery_data['due_date_to_check_9'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_9'].isin(holiday_list))), 1, 0)
        delivery_data['running_attempt_count_10'] = delivery_data['running_attempt_count_9'] + np.where(((~delivery_data['due_date_to_check_10'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_10'].isin(holiday_list))), 1, 0)
        delivery_data['running_attempt_count_11'] = delivery_data['running_attempt_count_10'] + np.where(((~delivery_data['due_date_to_check_11'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_11'].isin(holiday_list))), 1, 0)
        delivery_data['running_attempt_count_12'] = delivery_data['running_attempt_count_11'] + np.where(((~delivery_data['due_date_to_check_12'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_12'].isin(holiday_list))), 1, 0)
        delivery_data['running_attempt_count_13'] = delivery_data['running_attempt_count_12'] + np.where(((~delivery_data['due_date_to_check_13'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_13'].isin(holiday_list))), 1, 0)
        delivery_data['running_attempt_count_14'] = delivery_data['running_attempt_count_13'] + np.where(((~delivery_data['due_date_to_check_14'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_14'].isin(holiday_list))), 1, 0)
        delivery_data['running_attempt_count_15'] = delivery_data['running_attempt_count_14'] + np.where(((~delivery_data['due_date_to_check_15'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_15'].isin(holiday_list))), 1, 0)
        delivery_data['running_attempt_count_16'] = delivery_data['running_attempt_count_15'] + np.where(((~delivery_data['due_date_to_check_16'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_16'].isin(holiday_list))), 1, 0)
        delivery_data['running_attempt_count_17'] = delivery_data['running_attempt_count_16'] + np.where(((~delivery_data['due_date_to_check_17'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_17'].isin(holiday_list))), 1, 0)

        conditions = [
            ((~delivery_data['due_date_to_check_1'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_1'].isin(holiday_list)) & (delivery_data['running_attempt_count_1'] >= delivery_data['tnt'])),
            ((~delivery_data['due_date_to_check_2'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_2'].isin(holiday_list)) & (delivery_data['running_attempt_count_2'] >= delivery_data['tnt'])),
            ((~delivery_data['due_date_to_check_3'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_3'].isin(holiday_list)) & (delivery_data['running_attempt_count_3'] >= delivery_data['tnt'])),
            ((~delivery_data['due_date_to_check_4'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_4'].isin(holiday_list)) & (delivery_data['running_attempt_count_4'] >= delivery_data['tnt'])),
            ((~delivery_data['due_date_to_check_5'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_5'].isin(holiday_list)) & (delivery_data['running_attempt_count_5'] >= delivery_data['tnt'])),
            ((~delivery_data['due_date_to_check_6'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_6'].isin(holiday_list)) & (delivery_data['running_attempt_count_6'] >= delivery_data['tnt'])),
            ((~delivery_data['due_date_to_check_7'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_7'].isin(holiday_list)) & (delivery_data['running_attempt_count_7'] >= delivery_data['tnt'])),
            ((~delivery_data['due_date_to_check_8'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_8'].isin(holiday_list)) & (delivery_data['running_attempt_count_8'] >= delivery_data['tnt'])),
            ((~delivery_data['due_date_to_check_9'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_9'].isin(holiday_list)) & (delivery_data['running_attempt_count_9'] >= delivery_data['tnt'])),
            ((~delivery_data['due_date_to_check_10'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_10'].isin(holiday_list)) & (delivery_data['running_attempt_count_10'] >= delivery_data['tnt'])),
            ((~delivery_data['due_date_to_check_11'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_11'].isin(holiday_list)) & (delivery_data['running_attempt_count_11'] >= delivery_data['tnt'])),
            ((~delivery_data['due_date_to_check_12'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_12'].isin(holiday_list)) & (delivery_data['running_attempt_count_12'] >= delivery_data['tnt'])),
            ((~delivery_data['due_date_to_check_13'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_13'].isin(holiday_list)) & (delivery_data['running_attempt_count_13'] >= delivery_data['tnt'])),
            ((~delivery_data['due_date_to_check_14'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_14'].isin(holiday_list)) & (delivery_data['running_attempt_count_14'] >= delivery_data['tnt'])),
            ((~delivery_data['due_date_to_check_15'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_15'].isin(holiday_list)) & (delivery_data['running_attempt_count_15'] >= delivery_data['tnt'])),
            ((~delivery_data['due_date_to_check_16'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_16'].isin(holiday_list)) & (delivery_data['running_attempt_count_16'] >= delivery_data['tnt'])),
            ((~delivery_data['due_date_to_check_17'].dt.weekday.isin([5, 6])) & (~delivery_data['due_date_to_check_17'].isin(holiday_list)) & (delivery_data['running_attempt_count_17'] >= delivery_data['tnt']))
        ]
        choices = [
            delivery_data['due_date_to_check_1'],
            delivery_data['due_date_to_check_2'],
            delivery_data['due_date_to_check_3'],
            delivery_data['due_date_to_check_4'],
            delivery_data['due_date_to_check_5'],
            delivery_data['due_date_to_check_6'],
            delivery_data['due_date_to_check_7'],
            delivery_data['due_date_to_check_8'],
            delivery_data['due_date_to_check_9'],
            delivery_data['due_date_to_check_10'],
            delivery_data['due_date_to_check_11'],
            delivery_data['due_date_to_check_12'],
            delivery_data['due_date_to_check_13'],
            delivery_data['due_date_to_check_14'],
            delivery_data['due_date_to_check_15'],
            delivery_data['due_date_to_check_16'],
            delivery_data['due_date_to_check_17']
        ]
        delivery_data['due_date'] = pd.to_datetime(np.select(conditions, choices, default=pd.NaT))

        delivery_data['due_datetime'] = pd.to_datetime((delivery_data['due_date'].astype(str) + " " + delivery_data['due_time'].astype(str)), errors='coerce')

        delivery_data['on_time'] = np.where((delivery_data['possession_date'].isna()), np.nan, (delivery_data['due_datetime'] >= delivery_data['delivery_time']))


        ## Payroll Delivery Data
        print("Updating Payroll Delivery Data...")

        logger.info("payroll_delivery_data")
        payroll_delivery_data = delivery_data.loc[~delivery_data['customer_code'].isin(
                pickup_internal_codes + ace_codes_to_remove + transit_linehaul_revenue_codes + monthly_billing_codes
            ),
            [
                'customer_code', 'customer_name', 'order_number', 'reference_one', 'reference_two', 'terminal', 'assignee_name', 'pickup_name', 'pickup_address_one', 'pickup_address_two', 'delivery_name', 'delivery_address_one', 'delivery_address_two', 'delivery_city', 'delivery_state', 'delivery_zip', 'weight', 'service_type', 'notes', 'pod_name', 'event_name', 'event', 'service_amount', 'amount_charged', 'fuel_fee', 'order_date', 'delivery_time', 'on_time', 'package_operational', 'package_financial'
            ]
        ]

        with dbEngine.connect() as con:
            con.execute(text("DELETE FROM data_analytics_reporting.payroll_delivery_data"))
        insert_payroll_with_progress(payroll_delivery_data)


        ## OTP Data
        print("Updating OTP Data...")

        logger.info("otp_delivery_data")
        otp_data = delivery_data.loc[~delivery_data['customer_code'].isin(
            [
                '10000', '10049', '10051', '10053', '10055', '79000', '100100', '100463', '100935', '101999', '102292', '102378', '102425', '102454', '102458', '102487', '102491', '102494', '102495', '102629', '102630', '102676', '102677', '102705', '102708', '102709', '102761', '102762', '102830', '102836', '103009', '103010', '103026', '103326', '103434', '103463', '103491', '103522', '989898998', '989898999', '99004', '44145', '99011', '44144', '99442', '99001', '99280', '44132', '99173', '99008', '99000', '99445', '99608', '99015', '99017', '99007', '92272', '99583', '99013', '99010', '99005', '99006', '99635', '99014', '99002', '99016', '99009', '99018', '99174', '102705'
            ]),
            [
                'customer_code', 'customer_name', 'order_number', 'terminal', 'delivery_zip', 'event', 'assignee_name', 'service_type', 'notes', 'amount_charged', 'order_date', 'delivery_time', 'sortscan_time', 'sortscan_note', 'hub_arrival_time', 'hub_arrival_note', 'out_for_delivery_time', 'out_for_delivery_note', 'sort_hub', 'package_operational', 'package_financial', 'possession_date', 'attempt_number', 'due_datetime', 'on_time'
            ]
        ]

        with dbEngine.connect() as con:
            con.execute(text("DELETE FROM data_analytics_reporting.otp_data"))
        insert_otp_with_progress(otp_data)


    # Payroll Delivery Data
    logger.info("Payroll Delivery Data")

    delivery_data_query = '''
        SELECT *

        FROM
            data_analytics_reporting.payroll_delivery_data

        WHERE 
            delivery_time >= '{}' AND delivery_time <= '{}'
    '''.format(date_range[0], date_range[1])

    delivery_data = pd.read_sql(delivery_data_query, dbEngine)

    delivery_data['assignee_name'] = delivery_data['assignee_name'].str.upper().str.strip()
    delivery_data['service_type'] = delivery_data['service_type'].str.upper().str.strip()
    delivery_data['service_amount'] = delivery_data['service_amount'].replace({"None": np.nan, "": np.nan}).astype('float')
    delivery_data['amount_charged'] = delivery_data['amount_charged'].replace({"None": np.nan, "": np.nan}).astype('float')
    delivery_data['fuel_fee'] = delivery_data['fuel_fee'].replace({"None": np.nan, "": np.nan}).astype('float')
    delivery_data = delivery_data.rename(columns={'pickup_address_one':'pickup_address', 'delivery_address_one':'delivery_address'})


    ## Clean Driver Names

    clean_driver_names = pd.read_sql("SELECT * FROM data_analytics_reporting.clean_driver_names", dbEngine).set_index(keys=['assignee_name'])

    delivery_data = delivery_data.join(clean_driver_names, on='assignee_name')

    delivery_data = delivery_data.loc[~delivery_data['sage_name'].isna()]


    ## Employee Directory

    employee_directory_query = '''
        SELECT
            last_name,
            first_name,
            department_no,
            location AS driver_code

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

    employee_directory['sage_name'] = (employee_directory['last_name'] + ", " + employee_directory['first_name']).str.upper()
    employee_directory = employee_directory.loc[:, ['sage_name', 'department_no', 'driver_code']]
    employee_directory.set_index(keys=['sage_name'], inplace=True)

    delivery_data = delivery_data.join(employee_directory, on='sage_name')

    delivery_data = delivery_data.loc[delivery_data['department_no'].isin([10, 20, 55])]
    delivery_data = delivery_data.rename(columns={'sage_name':'driver_name'})

    delivery_data['address_clean'] = delivery_data['delivery_address'].str.replace(" ", "", regex=False).str.upper()
    delivery_data['delivery_date'] = delivery_data['delivery_time'].dt.normalize()
    delivery_data['delivery_time_hour'] = delivery_data['delivery_time'].dt.hour

    delivery_data['vape_customer'] = (
        (
            (delivery_data['event'] == "Delivered")
            & delivery_data['customer_code'].isin(['103595', '103593', '103633', '103634'])
        )
    )

    delivery_data.sort_values(by=['driver_name', 'delivery_time', 'customer_code', 'address_clean', 'order_number'], inplace=True)
    delivery_data['prev_delivery_time'] = delivery_data['delivery_time'].shift(1)
    delivery_data['post_delivery_time'] = delivery_data['delivery_time'].shift(-1)
    delivery_data['prev_delivery_date'] = delivery_data['delivery_date'].shift(1)
    delivery_data['post_delivery_date'] = delivery_data['delivery_date'].shift(-1)
    delivery_data['post_pod_name'] = delivery_data['pod_name'].shift(-1)
    delivery_data['post_vape_customer'] = delivery_data['vape_customer'].shift(-1)
    delivery_data['post_address_clean'] = delivery_data['address_clean'].shift(-1)
    delivery_data['post_event'] = delivery_data['event'].shift(-1)

    delivery_data['signature_time_deduction'] = delivery_data.apply(lambda x: (x['delivery_time'] - x['prev_delivery_time']) / np.timedelta64(1, 'h')
        if (
            (x['event'] == "Delivered")
            and (x['vape_customer'])
            and (x['delivery_date'] == x['prev_delivery_date'])
            and (abs((x['delivery_time'] - x['prev_delivery_time']) / np.timedelta64(1, 'h')) < 1.5)
        ) or (
            (x['post_event'] == "Delivered")
            and (x['event'] != "Delivered")
            and (x['post_vape_customer'])
            and (x['vape_customer'])
            and (x['address_clean'] == x['post_address_clean'])
            and (x['delivery_date'] == x['post_delivery_date'])
            and (abs((x['delivery_time'] - x['prev_delivery_time']) / np.timedelta64(1, 'h')) < 1.5)
        )
        else 0.0, 
    axis=1).round(decimals=2)

    delivery_data['signature_time_deduction_taken'] = np.where((delivery_data['signature_time_deduction'] <= 0.0), 0, 1)

    delivery_data['packages'] = 1

    delivery_data.sort_values(by=['driver_name', 'customer_code', 'address_clean', 'delivery_time', 'order_number'], inplace=True, ignore_index=True)

    delivery_data['prev_customer_code'] = delivery_data['customer_code'].shift(1)
    delivery_data['prev_2_customer_code'] = delivery_data['customer_code'].shift(2)
    delivery_data['prev_delivery_time'] = delivery_data['delivery_time'].shift(1)
    delivery_data['prev_2_delivery_time'] = delivery_data['delivery_time'].shift(2)
    delivery_data['prev_address_clean'] = delivery_data['address_clean'].shift(1)
    delivery_data['prev_2_address_clean'] = delivery_data['address_clean'].shift(2)
    delivery_data['ind'] = delivery_data.index

    delivery_data['stops'] = delivery_data.apply(lambda x: 
        1 if (
            (re.search("ALBERTSONS", str(x['customer_name']), re.IGNORECASE) is not None)
            or (re.search("SCHRADERS", str(x['customer_name']), re.IGNORECASE) is not None)
            or (x['ind'] < 2)
        )
        else 0 if (
            (x['service_type'] == "STEM")
            or (
                (abs((x['delivery_time'] - x['prev_delivery_time'])  / np.timedelta64(1, 'm')) <= 20)
                and (x['customer_code'] == x['prev_customer_code'])
                and (x['address_clean'] == x['prev_address_clean'])
            )
            or (
                (abs((x['delivery_time'] - x['prev_2_delivery_time'])  / np.timedelta64(1, 'm')) <= 20)
                and (x['customer_code'] == x['prev_2_customer_code'])
                and (x['address_clean'] == x['prev_2_address_clean'])
            )
            or (
                x['customer_code'] == "100935"
            )
        )
        else 1,
    axis=1)


    # Summarized Delivery Data
    print("Updating Summarized Delivery Data...")
    logger.info("Summarized Delivery Data")

    cov_delivery_data = delivery_data.loc[delivery_data['department_no'].isin([10, 20, 55])]

    cov_delivery_data = cov_delivery_data.loc[~cov_delivery_data['customer_code'].isin(['100463', '79000'])]
    cov_delivery_data = cov_delivery_data.loc[~cov_delivery_data['service_type'].isin(['FLAT', 'PICK-UP', 'PICKUP', 'SORT', 'STEM', 'SUBSIDY'])]
    cov_delivery_data = cov_delivery_data.loc[(~cov_delivery_data['service_type'].isin(['RETURN-IT', 'RTS'])) & (cov_delivery_data['amount_charged'] > 1)]

    pod_list = cov_delivery_data.loc[:, ['driver_name', 'delivery_date']].drop_duplicates()
    comm_list = cov_delivery_data.loc[:, ['driver_name', 'delivery_date', 'delivery_time']]
    comm_list['delivery_time_hour'] = comm_list['delivery_time'].dt.hour
    comm_list = comm_list.loc[comm_list['delivery_time_hour'] > 4]

    comm_list['delivery_time'] = comm_list['delivery_time'].dt.floor('Min')
    comm_list = comm_list.drop_duplicates().groupby(by=['driver_name', 'delivery_date'])['delivery_time']
    first_pod_1 = comm_list.nsmallest(n=1).groupby(level=['driver_name', 'delivery_date']).last().rename("first_pod_1")
    pod_list = pod_list.join(first_pod_1, on=['driver_name', 'delivery_date'])
    last_pod_1 = comm_list.nlargest(n=1).groupby(level=['driver_name', 'delivery_date']).last().rename("last_pod_1")
    pod_list = pod_list.join(last_pod_1, on=['driver_name', 'delivery_date'])

    comm_list = cov_delivery_data.loc[:, ['driver_name', 'delivery_date', 'delivery_time', 'signature_time_deduction']].sort_values(by=['driver_name', 'delivery_time'])
    comm_list['delivery_time'] = comm_list['delivery_time'].dt.floor('Min')
    comm_list['delivery_time_hour'] = comm_list['delivery_time'].dt.hour
    comm_list = comm_list.loc[comm_list['delivery_time_hour'] > 4]
    comm_list['driver_day_count'] = comm_list.groupby(['driver_name', 'delivery_date']).cumcount() + 1
    comm_list = comm_list.loc[(comm_list['signature_time_deduction'] == 0) | (comm_list['driver_day_count'] == 1)]
    comm_list = comm_list.drop_duplicates(subset=['driver_name', 'delivery_date', 'delivery_time']).groupby(by=['driver_name', 'delivery_date'])['delivery_time']

    first_pod_2 = comm_list.nsmallest(n=2).groupby(level=['driver_name', 'delivery_date']).last().rename("first_pod_2")
    pod_list = pod_list.join(first_pod_2, on=['driver_name', 'delivery_date'])
    first_pod_3 = comm_list.nsmallest(n=3).groupby(level=['driver_name', 'delivery_date']).last().rename("first_pod_3")
    pod_list = pod_list.join(first_pod_3, on=['driver_name', 'delivery_date'])
    first_pod_4 = comm_list.nsmallest(n=4).groupby(level=['driver_name', 'delivery_date']).last().rename("first_pod_4")
    pod_list = pod_list.join(first_pod_4, on=['driver_name', 'delivery_date'])
    first_pod_5 = comm_list.nsmallest(n=5).groupby(level=['driver_name', 'delivery_date']).last().rename("first_pod_5")
    pod_list = pod_list.join(first_pod_5, on=['driver_name', 'delivery_date'])
    first_pod_6 = comm_list.nsmallest(n=6).groupby(level=['driver_name', 'delivery_date']).last().rename("first_pod_6")
    pod_list = pod_list.join(first_pod_6, on=['driver_name', 'delivery_date'])
    first_pod_7 = comm_list.nsmallest(n=7).groupby(level=['driver_name', 'delivery_date']).last().rename("first_pod_7")
    pod_list = pod_list.join(first_pod_7, on=['driver_name', 'delivery_date'])
    first_pod_8 = comm_list.nsmallest(n=8).groupby(level=['driver_name', 'delivery_date']).last().rename("first_pod_8")
    pod_list = pod_list.join(first_pod_8, on=['driver_name', 'delivery_date'])
    first_pod_9 = comm_list.nsmallest(n=9).groupby(level=['driver_name', 'delivery_date']).last().rename("first_pod_9")
    pod_list = pod_list.join(first_pod_9, on=['driver_name', 'delivery_date'])
    first_pod_10 = comm_list.nsmallest(n=10).groupby(level=['driver_name', 'delivery_date']).last().rename("first_pod_10")
    pod_list = pod_list.join(first_pod_10, on=['driver_name', 'delivery_date'])

    pod_list['first_pod_10'] = pod_list.apply(lambda x: np.nan if x['first_pod_9'] == x['first_pod_10'] else x['first_pod_10'], axis=1)
    pod_list['first_pod_9'] = pod_list.apply(lambda x: np.nan if x['first_pod_8'] == x['first_pod_9'] else x['first_pod_9'], axis=1)
    pod_list['first_pod_8'] = pod_list.apply(lambda x: np.nan if x['first_pod_7'] == x['first_pod_8'] else x['first_pod_8'], axis=1)
    pod_list['first_pod_7'] = pod_list.apply(lambda x: np.nan if x['first_pod_6'] == x['first_pod_7'] else x['first_pod_7'], axis=1)
    pod_list['first_pod_6'] = pod_list.apply(lambda x: np.nan if x['first_pod_5'] == x['first_pod_6'] else x['first_pod_6'], axis=1)
    pod_list['first_pod_5'] = pod_list.apply(lambda x: np.nan if x['first_pod_4'] == x['first_pod_5'] else x['first_pod_5'], axis=1)
    pod_list['first_pod_4'] = pod_list.apply(lambda x: np.nan if x['first_pod_3'] == x['first_pod_4'] else x['first_pod_4'], axis=1)
    pod_list['first_pod_3'] = pod_list.apply(lambda x: np.nan if x['first_pod_2'] == x['first_pod_3'] else x['first_pod_3'], axis=1)
    pod_list['first_pod_2'] = pod_list.apply(lambda x: np.nan if x['first_pod_1'] == x['first_pod_2'] else x['first_pod_2'], axis=1)

    comm_list = cov_delivery_data.loc[:, ['driver_name', 'delivery_date', 'delivery_time', 'signature_time_deduction']].sort_values(by=['driver_name', 'delivery_time'], ascending=False)
    comm_list['delivery_time'] = comm_list['delivery_time'].dt.floor('Min')
    comm_list['delivery_time_hour'] = comm_list['delivery_time'].dt.hour
    comm_list = comm_list.loc[comm_list['delivery_time_hour'] > 4]
    comm_list['driver_day_count'] = comm_list.groupby(['driver_name', 'delivery_date']).cumcount() + 1
    comm_list = comm_list.loc[(comm_list['signature_time_deduction'] == 0) | (comm_list['driver_day_count'] == 1)]
    comm_list = comm_list.drop_duplicates(subset=['driver_name', 'delivery_date', 'delivery_time']).groupby(by=['driver_name', 'delivery_date'])['delivery_time']

    last_pod_2 = comm_list.nlargest(n=2).groupby(level=['driver_name', 'delivery_date']).last().rename("last_pod_2")
    pod_list = pod_list.join(last_pod_2, on=['driver_name', 'delivery_date'])
    last_pod_3 = comm_list.nlargest(n=3).groupby(level=['driver_name', 'delivery_date']).last().rename("last_pod_3")
    pod_list = pod_list.join(last_pod_3, on=['driver_name', 'delivery_date'])
    last_pod_4 = comm_list.nlargest(n=4).groupby(level=['driver_name', 'delivery_date']).last().rename("last_pod_4")
    pod_list = pod_list.join(last_pod_4, on=['driver_name', 'delivery_date'])
    last_pod_5 = comm_list.nlargest(n=5).groupby(level=['driver_name', 'delivery_date']).last().rename("last_pod_5")
    pod_list = pod_list.join(last_pod_5, on=['driver_name', 'delivery_date'])
    last_pod_6 = comm_list.nlargest(n=6).groupby(level=['driver_name', 'delivery_date']).last().rename("last_pod_6")
    pod_list = pod_list.join(last_pod_6, on=['driver_name', 'delivery_date'])
    last_pod_7 = comm_list.nlargest(n=7).groupby(level=['driver_name', 'delivery_date']).last().rename("last_pod_7")
    pod_list = pod_list.join(last_pod_7, on=['driver_name', 'delivery_date'])
    last_pod_8 = comm_list.nlargest(n=8).groupby(level=['driver_name', 'delivery_date']).last().rename("last_pod_8")
    pod_list = pod_list.join(last_pod_8, on=['driver_name', 'delivery_date'])
    last_pod_9 = comm_list.nlargest(n=9).groupby(level=['driver_name', 'delivery_date']).last().rename("last_pod_9")
    pod_list = pod_list.join(last_pod_9, on=['driver_name', 'delivery_date'])
    last_pod_10 = comm_list.nlargest(n=10).groupby(level=['driver_name', 'delivery_date']).last().rename("last_pod_10")
    pod_list = pod_list.join(last_pod_10, on=['driver_name', 'delivery_date'])

    pod_list['last_pod_10'] = pod_list.apply(lambda x: np.nan if x['last_pod_9'] == x['last_pod_10'] else x['last_pod_10'], axis=1)
    pod_list['last_pod_9'] = pod_list.apply(lambda x: np.nan if x['last_pod_8'] == x['last_pod_9'] else x['last_pod_9'], axis=1)
    pod_list['last_pod_8'] = pod_list.apply(lambda x: np.nan if x['last_pod_7'] == x['last_pod_8'] else x['last_pod_8'], axis=1)
    pod_list['last_pod_7'] = pod_list.apply(lambda x: np.nan if x['last_pod_6'] == x['last_pod_7'] else x['last_pod_7'], axis=1)
    pod_list['last_pod_6'] = pod_list.apply(lambda x: np.nan if x['last_pod_5'] == x['last_pod_6'] else x['last_pod_6'], axis=1)
    pod_list['last_pod_5'] = pod_list.apply(lambda x: np.nan if x['last_pod_4'] == x['last_pod_5'] else x['last_pod_5'], axis=1)
    pod_list['last_pod_4'] = pod_list.apply(lambda x: np.nan if x['last_pod_3'] == x['last_pod_4'] else x['last_pod_4'], axis=1)
    pod_list['last_pod_3'] = pod_list.apply(lambda x: np.nan if x['last_pod_2'] == x['last_pod_3'] else x['last_pod_3'], axis=1)
    pod_list['last_pod_2'] = pod_list.apply(lambda x: np.nan if x['last_pod_1'] == x['last_pod_2'] else x['last_pod_2'], axis=1)

    pod_list.set_index(['driver_name', 'delivery_date'], inplace=True)

    cov_delivery_data = cov_delivery_data.join(pod_list, on=['driver_name', 'delivery_date'])

    summarized_delivery_data = cov_delivery_data.groupby(['driver_name', 'driver_code', 'delivery_date']).agg({'packages':['sum'], 'stops':['sum'], 'service_amount': ['sum'], 'amount_charged': ['sum'], 'signature_time_deduction':['sum'], 'signature_time_deduction_taken':['sum'], 'on_time':['sum', 'count']})

    summarized_delivery_data.columns = summarized_delivery_data.columns.to_flat_index()
    summarized_delivery_data = summarized_delivery_data.rename(columns={('packages', 'sum'):'packages', ('stops', 'sum'):'stops', ('service_amount', 'sum'):'service_amount', ('amount_charged', 'sum'):'amount_charged', ('signature_time_deduction', 'sum'):'signature_time_deduction', ('signature_time_deduction_taken', 'sum'):'signature_time_deductions_taken', ('on_time', 'sum'):'on_time_sum', ('on_time', 'count'):'on_time_count'})

    summarized_delivery_data['on_time'] = summarized_delivery_data['on_time_sum'] / summarized_delivery_data['on_time_count']
    summarized_delivery_data = summarized_delivery_data.drop(columns=['on_time_sum', 'on_time_count'])

    pod_list = cov_delivery_data.loc[:, ['driver_name', 'delivery_date', 'first_pod_1', 'first_pod_2', 'first_pod_3', 'first_pod_4', 'first_pod_5', 'first_pod_6', 'first_pod_7', 'first_pod_8', 'first_pod_9', 'first_pod_10', 'last_pod_10', 'last_pod_9', 'last_pod_8', 'last_pod_7', 'last_pod_6', 'last_pod_5', 'last_pod_4', 'last_pod_3', 'last_pod_2', 'last_pod_1']].drop_duplicates().set_index(keys=['driver_name', 'delivery_date'])

    summarized_delivery_data = summarized_delivery_data.join(pod_list, on=['driver_name', 'delivery_date'])

    with dbEngine.connect() as con:
        con.execute(text("DELETE FROM data_analytics_reporting.summarized_delivery_data"))
    insert_cov_with_progress(summarized_delivery_data.reset_index())


    # Other Customer Delivery Data
    print("Updating Other Delivery Data...")
    logger.info("Other Delivery Data")

    other_customer_delivery_data = delivery_data.loc[delivery_data['customer_code'].isin(["100463", "79000"])]

    other_customer_delivery_data = other_customer_delivery_data.loc[:, ['customer_code', 'delivery_name', 'delivery_time', 'delivery_address', 'delivery_state', 'delivery_zip', 'customer_name', 'reference_one', 'reference_two', 'driver_name', 'driver_code', 'pod_name', 'service_type', 'service_amount', 'order_number', 'pickup_name', 'pickup_address', 'amount_charged', 'delivery_date']]

    with dbEngine.connect() as con:
        con.execute(text("DELETE FROM data_analytics_reporting.other_delivery_data"))

    if (len(other_customer_delivery_data) >= 20):

        insert_other_with_progress(other_customer_delivery_data)


    # Detailed Delivery Data
    print("Updating Detailed Delivery Data...")
    logger.info("Detailed Delivery Data")

    detailed_delivery_data = delivery_data.loc[(~delivery_data['customer_code'].isin(["100463", "79000", "101999", "989898999", "102705"])) & (~delivery_data['service_type'].isin(["FLAT", "PICKUP", "PICK-UP", "SORT", "STEM", "SUBSIDY"]))]

    b =  detailed_delivery_data['delivery_date'] + pd.offsets.Week(weekday=5)
    m2 = detailed_delivery_data['delivery_date'] != (b - pd.offsets.Week())
    detailed_delivery_data['week_ending'] = detailed_delivery_data['delivery_date'].mask(m2, b)

    detailed_delivery_data['combined_address'] = (detailed_delivery_data['customer_code'] + detailed_delivery_data['driver_name'] + detailed_delivery_data['delivery_address'].str.replace(pat=" ", repl="")).str.upper()

    detailed_delivery_data.sort_values(by=['driver_name', 'combined_address', 'delivery_time'], inplace=True, ignore_index=True)

    detailed_delivery_data['pre_date'] = detailed_delivery_data['delivery_date'].shift(1)
    detailed_delivery_data['pre_combined_address'] = detailed_delivery_data['combined_address'].shift(1)
    detailed_delivery_data['post_customer_code'] = detailed_delivery_data['customer_code'].shift(-1)
    detailed_delivery_data['post_2_customer_code'] = detailed_delivery_data['customer_code'].shift(-2)
    detailed_delivery_data['post_delivery_time'] = detailed_delivery_data['delivery_time'].shift(-1)
    detailed_delivery_data['post_2_delivery_time'] = detailed_delivery_data['delivery_time'].shift(-2)
    detailed_delivery_data['post_combined_address'] = detailed_delivery_data['combined_address'].shift(-1)
    detailed_delivery_data['post_2_combined_address'] = detailed_delivery_data['combined_address'].shift(-2)
    detailed_delivery_data['ind'] = detailed_delivery_data.index

    detailed_delivery_data['final_stop_count'] = detailed_delivery_data.apply(lambda x: 
        0 if (
            (
                (abs((x['delivery_time'] - x['post_delivery_time'])  / np.timedelta64(1, 'm')) <= 20)
                and (x['customer_code'] == x['post_customer_code'])
                and (x['combined_address'] == x['post_combined_address'])
            )
            or (
                (abs((x['delivery_time'] - x['post_2_delivery_time'])  / np.timedelta64(1, 'm')) <= 20)
                and (x['customer_code'] == x['post_2_customer_code'])
                and (x['combined_address'] == x['post_2_combined_address'])
            )
        )
        else 1,
    axis=1)

    detailed_delivery_data['stop_count_old_model'] = detailed_delivery_data.apply(lambda x: 
        1 if (
            (re.search("ALBERTSONS", str(x['customer_name']), re.IGNORECASE) is not None)
            or (re.search("SCHRADERS", str(x['customer_name']), re.IGNORECASE) is not None)
            or (x['ind'] < 2)
        )
        else 0 if (
            (
                (abs((x['delivery_time'] - x['post_delivery_time'])  / np.timedelta64(1, 'm')) <= 20)
                and (x['customer_code'] == x['post_customer_code'])
                and (x['combined_address'] == x['post_combined_address'])
            )
            or (
                (abs((x['delivery_time'] - x['post_2_delivery_time'])  / np.timedelta64(1, 'm')) <= 20)
                and (x['customer_code'] == x['post_2_customer_code'])
                and (x['combined_address'] == x['post_2_combined_address'])
            )
        )
        else 1,
    axis=1)

    detailed_delivery_data['consolidate_exception'] = (detailed_delivery_data['customer_name'].str.contains(pat="MWI ANIMAL HEALTH|SCHRADERS", case=False, regex=True, na=False))

    detailed_delivery_data['cumsum_count'] = detailed_delivery_data.groupby(by=['combined_address', 'delivery_date']).cumcount() + 1

    detailed_delivery_data['pre_cumsum_count'] = detailed_delivery_data['cumsum_count'].shift(1)

    detailed_delivery_data['consolidation_count'] = detailed_delivery_data.apply(lambda x:
        0 if (not x['consolidate_exception']) else (
            1 if (x['combined_address'] != x['pre_combined_address']) else (
                x['cumsum_count'] if ((x['combined_address'] == x['pre_combined_address']) & (x['delivery_date'] == x['pre_date'])) else (
                    1
                )
            )
        ),                                                                 
    axis=1)

    vape_customers = pd.DataFrame({'vape_customer_codes' : ['103592', '103594', '103610', '103634', '103642', '103647', '103649', '103653'], 'vape_customer_names' : ['PUFF CORPORATION', 'EIGHTVAPE', 'SHYPGYSTYCS', 'None', 'None', 'None', 'None', 'SHYPGYSTYCS-Reattempt']}).set_index(keys=['vape_customer_codes'])

    detailed_delivery_data = detailed_delivery_data.join(vape_customers, on=['customer_code'])

    detailed_delivery_data['vape_stops'] = (((detailed_delivery_data['service_type'].str.contains(pat="STAT", case=False, regex=False, na=False)) & (detailed_delivery_data['customer_name'].str.startswith(pat="ADP", na=False)) & (detailed_delivery_data['amount_charged'] >= 1)) | ((pd.notna(detailed_delivery_data['vape_customer_names'])) & (detailed_delivery_data['event'] == "Delivered"))).replace({True : 1, False : 0})

    detailed_delivery_data['full_stop_rate'] = detailed_delivery_data.apply(lambda x:
        1 if (x['consolidate_exception']) else (
            0 if ((x['vape_stops'] == 1) | (((re.search("STAT", str(x['service_type']), flags=re.IGNORECASE) is not None)) & (re.search("ADP", str(x['customer_name']), flags=re.IGNORECASE) is not None) & (x['amount_charged'] < 1)) | (((x['combined_address'] == x['post_combined_address']) & ((abs((x['delivery_time'] - x['post_delivery_time'])  / np.timedelta64(1, 'm')) <= 20))) | ((x['combined_address'] == x['post_2_combined_address']) & ((abs((x['delivery_time'] - x['post_2_delivery_time'])  / np.timedelta64(1, 'm')) <= 20))))) else (
                1
            )
        ),
    axis=1) - np.where(detailed_delivery_data['consolidation_count'] > 2, 1, 0)

    detailed_delivery_data['discounted_stops'] = np.where(detailed_delivery_data['consolidation_count'] < 3, 0, 1)

    detailed_delivery_data['package_count'] = 1

    detailed_delivery_data['adult_sig'] = None

    detailed_delivery_data = detailed_delivery_data.loc[:, ['customer_code', 'delivery_name', 'delivery_time', 'delivery_address', 'customer_name', 'driver_name', 'pod_name', 'service_type', 'service_amount', 'order_number', 'amount_charged', 'adult_sig', 'delivery_date', 'week_ending', 'combined_address', 'final_stop_count', 'stop_count_old_model', 'consolidate_exception', 'consolidation_count', 'vape_stops', 'full_stop_rate', 'discounted_stops', 'package_count', 'driver_code']]

    with dbEngine.connect() as con:
        con.execute(text("DELETE FROM data_analytics_reporting.detailed_delivery_data"))
    insert_detailed_with_progress(detailed_delivery_data)

    dbEngine.dispose()