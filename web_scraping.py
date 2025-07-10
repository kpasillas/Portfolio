#!/usr/bin/env python3

import numpy as np
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from time import sleep
import os
import logging
from sqlalchemy import text

from concise_exceptions import PayrollMissingFromConciseMenuError
import get_db_connection

def main():

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s:%(message)s")
    file_handler = logging.FileHandler("update_concise_mobile_track_data.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    report_date = datetime.now(timezone.utc).astimezone(ZoneInfo("America/Los_Angeles"))
    new_data = pd.DataFrame()

    dbEngine = get_db_connection.get_mysql_engine()
    
    url = "https://di.concise.io/"
    login = os.environ.get('O365_USERNAME')
    password = os.environ.get('CONCISE_PASSWORD')

    options = Options()
    options.add_argument("--headless")
    options.add_argument("window-size=1400,1500")

    driver = webdriver.Chrome(options=options)
    driver.get(url)
    

    # Login
    
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "email"))
    ).send_keys(login)

    driver.find_element(By.ID, "password").send_keys(password)

    driver.find_element(By.CLASS_NAME, "btn.btn--secondary").click()


    # Navigate to Mobile Track menu

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, "list-nav-link"))
    )

    menu_items = driver.find_elements(By.CLASS_NAME, "list-nav-link")

    for menu_item in menu_items:
        if re. search("mobile track", menu_item.text, re.IGNORECASE) is not None:
            menu_item.click()
            break
    else:
        raise PayrollMissingFromConciseMenuError()

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, "material-icons.custom-select-arrow"))
    )


    # Select Date Range

    start_day = str(report_date.day)
    start_month = report_date.strftime('%b')
    start_year = str(report_date.year)
    end_day = str(report_date.day)
    end_month = report_date.strftime('%b')
    end_year = str(report_date.year)

    driver.find_elements(By.CLASS_NAME, "material-icons.custom-select-arrow")[0].click()
    custom_select_list = driver.find_elements(By.CLASS_NAME, "custom-select")
    
    month_select = Select(custom_select_list[0])
    month_select.select_by_visible_text(start_month)
    
    year_select = Select(custom_select_list[1])
    year_select.select_by_visible_text(start_year)

    driver.find_elements(By.CLASS_NAME, "custom-day")[int(start_day) - 1].click()

    month_select = Select(custom_select_list[0])
    month_select.select_by_visible_text(end_month)
    
    year_select = Select(custom_select_list[1])
    year_select.select_by_visible_text(end_year)

    driver.find_elements(By.CLASS_NAME, "custom-day")[int(end_day) - 1].click()

    driver.find_element(By.CLASS_NAME, "btn.btn--primary.ml-2").click()


    # Collect Table Data
    
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "select"))
    )

    option_buttons = driver.find_elements(By.CSS_SELECTOR, "option")

    option_buttons[-1].click()

    page_buttons = driver.find_elements(By.CLASS_NAME, "cursor-pointer")
    next_button = page_buttons[-1]

    while(True):

        sleep(10)
        driver.execute_script("return arguments[0].scrollIntoView(true);", driver.find_elements(By.CSS_SELECTOR, "th")[0])

        table_rows = driver.find_elements(By.CSS_SELECTOR, "tr")

        for table_row in table_rows:
            
            table_cells = table_row.find_elements(By.CSS_SELECTOR, "td")
            
            if table_cells:

                current_row = dict()
                current_row.update({'date':datetime.strptime(table_cells[0].text, "%m-%d-%Y")})
                
                table_cells[7].find_elements(By.CSS_SELECTOR, "button")[0].click()

                modal_content = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "modal-content"))
                )

                modal_rows = modal_content.find_elements(By.CSS_SELECTOR, "tr")

                for modal_row in modal_rows:
                    
                    modal_cells = modal_row.find_elements(By.CSS_SELECTOR, "td")
                    
                    if len(modal_cells) < 2:

                        current_row.update({'alert':modal_cells[0].text})
                    
                    elif len(modal_cells) >= 2:

                        current_row.update({modal_cells[0].text:modal_cells[1].text})
                
                new_data = pd.concat([new_data, pd.DataFrame.from_dict([current_row])])

                modal_content.find_element(By.CLASS_NAME, "btn.btn--tertiary.w-120").click()

        try:
            _ = next_button.find_element(By.CLASS_NAME, "material-icons.active")
            next_button.click()

        except NoSuchElementException:
            break

    driver.quit()

    new_data = new_data.rename(columns={'Shipment':'shipment', 'Action':'action', 'Content':'content', 'Signature':'signature', 'Gate Code':'gate_code', 'Suite #':'suite_number', 'Driver':'driver', 'Route':'route', 'Expected Delivery':'expected_delivery', 'Recipient Notes':'recipient_notes', 'Dispatcher Notes':'dispatcher_notes', 'Delivery Information':'delivery_information'}).replace({"":None})
    new_data['content'] = new_data['content'].apply(lambda x: None if (x is None) else ("".join(i for i in x if 31 < ord(i) < 127))).str.replace(pat="::|;;|>>", repl="", regex=True)
    new_data['gate_code'] = new_data['gate_code'].astype('str')
    new_data['gate_code'] = new_data['gate_code'].apply(lambda x: None if (x is None) else ("".join(i for i in x if 31 < ord(i) < 127))).str.replace(pat="::|;;|>>", repl="", regex=True)
    new_data['recipient_notes'] = new_data['recipient_notes'].apply(lambda x: None if (x is None) else ("".join(i for i in x if 31 < ord(i) < 127))).str.replace(pat="::|;;|>>", repl="", regex=True)
    new_data['delivery_information'] = new_data['delivery_information'].apply(lambda x: None if (x is None) else ("".join(i for i in x if 31 < ord(i) < 127))).str.replace(pat="::|;;|>>", repl="", regex=True)
    new_data['suite_number'] = new_data['suite_number'].astype('str')
    new_data['suite_number'] = new_data['suite_number'].apply(lambda x: None if (x is None) else ("".join(i for i in x if 31 < ord(i) < 127))).str.replace(pat="::|;;|>>", repl="", regex=True)

    for column in new_data.columns:
        if (new_data[column].dtype == "object"):
            new_data[column] = new_data[column].str[:255]
    
    exististing_data_query = '''
        SELECT *
        
        FROM
            data_analytics_reporting.concise_mobile_track
        
        WHERE
            date = '{}'
    '''.format(report_date.date())

    existing_data = pd.read_sql(exististing_data_query, dbEngine)
    existing_data['ec_id'] = existing_data['ec_id'].astype('str')

    delete_exististing_data_query = '''
        DELETE
        
        FROM
            data_analytics_reporting.concise_mobile_track
        
        WHERE
            date = '{}'
    '''.format(report_date.date())

    with dbEngine.connect() as con:
        con.execute(text(delete_exististing_data_query))

    order_number_list = str(tuple(new_data.loc[new_data['shipment'].notna()]['shipment'].drop_duplicates().astype('str')))

    order_id_query = '''
        SELECT
            id AS 'ec_id',
            order_number
        
        FROM
            integrity.orders
        
        WHERE
            order_number IN {}

    '''.format(order_number_list)

    order_ids = pd.read_sql(order_id_query, dbEngine)
    order_ids['ec_id'] = order_ids['ec_id'].astype('str')
    order_ids = order_ids.set_index(keys=['order_number'])
    
    new_data = new_data.join(order_ids, on=['shipment'])
    new_data = new_data.loc[new_data['ec_id'].notna()]

    if "alert" not in new_data.columns:
        new_data['alert'] = None

    new_data = pd.concat([new_data, existing_data], ignore_index=True)

    new_data = new_data.drop_duplicates()

    new_data.to_sql('concise_mobile_track', dbEngine, schema='data_analytics_reporting', if_exists='append', index=False)

    if ((len(new_data) - len(existing_data)) > 0):

        update_ecourier_updates_query = '''
            INSERT INTO integrity.ecourier_updates (
                event,
                order_number,
                event_note,
                event_timestamp,
                processed,
                created_at,
                ec_id
            )
            SELECT
                'deliveryUpdates',
                cmtd.shipment,
                CONCAT('gatecode:: ', COALESCE(cmtd.gate_code, ''), ';;deliverynotes:: ', COALESCE(cmtd.recipient_notes, '')),
                CURRENT_TIMESTAMP(),
                '-3',
                CURRENT_TIMESTAMP(),
                cmtd.ec_id
            FROM data_analytics_reporting.concise_mobile_track cmtd
            WHERE NOT EXISTS (
                SELECT 1
                FROM integrity.ecourier_updates old
                WHERE
                        cmtd.shipment = old.order_number
                    AND CONCAT('gatecode:: ', COALESCE(cmtd.gate_code, ''), ';;deliverynotes:: ', COALESCE(cmtd.recipient_notes, '')) = old.event_note
                    AND cmtd.ec_id = old.ec_id
            )
        '''
        with dbEngine.connect() as con:
            con.execute(text(update_ecourier_updates_query))

        update_app_configs_query = '''
            UPDATE app_configs
            SET `value` = '1'
            WHERE (`key` = 'DeliveryNotes:HasNewItems')
        '''
        with dbEngine.connect() as con:
            con.execute(text(update_app_configs_query))

    logger.info("Existing Count - {}".format(len(existing_data)))
    logger.info("New Count - {}".format(len(new_data) - len(existing_data)))

    dbEngine.dispose()

if __name__ == '__main__': main()