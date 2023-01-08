
import os
import json
import csv
import requests
import time
import datetime

from dotenv import load_dotenv
from loguru import logger
from requests import ReadTimeout
from requests import HTTPError
from requests import Timeout
from requests import ConnectionError
from requests import ConnectTimeout

from db_sqlite3_connector import create_record
from db_sqlite3_connector import get_names_level_status_0
from db_sqlite3_connector import update_status
from db_sqlite3_connector import create_counter
from db_sqlite3_connector import check_counter
from db_sqlite3_connector import update_counter

load_dotenv()

logger.add("data/logs/debug.log", format="{time} {level} {message}",
           level='DEBUG', rotation="00:00", compression="zip")


LOGIN = os.environ['LOGIN']
PASSWORD = os.environ['PASSWORD']
URL_API = os.environ['URL_API']
PATH_COOKIES = 'data/cookies/cookies.json'
PATH_JSON = 'data/json/'


def read_file_csv():
    temp = []
    with open("name_file.csv", newline='') as csv_file:
        for row in csv.reader(csv_file):
            temp.extend(row)
    return temp


def create_start_records_lvl_1():
    objects = read_file_csv()
    for name in objects:
        create_record(name, 1, 0)


def log_get_cookies(LOGIN: str, PASSWORD: str):
    session = requests.session()
    response = session.post(''.join((URL_API, 'login')), data={"UserName": LOGIN, "Password": PASSWORD}).json()
    cookie = requests.utils.dict_from_cookiejar(session.cookies)
    with open(PATH_COOKIES, 'w') as file:
        json.dump(cookie, file, indent=4)


def create_request(name, number):
    name = str(name)
    with open(PATH_COOKIES) as file:
        templates = json.load(file)
    login_my_seldon = templates['LoginMyseldon']
    session_guid = templates['SessionGuid']
    data = f"LoginMyseldon={login_my_seldon}; SessionGuid={session_guid}"
    headers = {"Cookie": data}

    count = 0
    while count < 6:
        if len(name) == 10:
            try:
                return requests.get(''.join((URL_API, f'get_company_subs?inn={name}')), headers=headers)
            except ConnectTimeout:
                logger.info(f'ConnectTimeout {name}  Count request: {number}')
                time.sleep(20)
            except ConnectionError:
                logger.info(f'ConnectionError {name} Count request: {number}')
            except HTTPError:
                logger.info(f'HTTPError {name}  Count request: {number}')
            except ReadTimeout:
                logger.info(f'ReadTimeout {name}  Count request: {number}')
            except Timeout:
                logger.info(f'Timeout {name}  Count request: {number}')
            time.sleep(5)
            count += 1
        elif len(name) == 13:
            try:
                return requests.get(''.join((URL_API, f'get_company_subs?ogrn={name}')), headers=headers)
            except ConnectTimeout:
                logger.info(f'ConnectTimeout {name}  Count request: {number}')
                time.sleep(20)
            except ConnectionError:
                logger.info(f'ConnectionError {name} Count request: {number}')
            except HTTPError:
                logger.info(f'HTTPError {name}  Count request: {number}')
            except ReadTimeout:
                logger.info(f'ReadTimeout {name}  Count request: {number}')
            except Timeout:
                logger.info(f'Timeout {name}  Count request: {number}')
            
            time.sleep(5)
            count += 1
        else:
            logger.info(f'MISS NAME: {name} Count request: {number}')
            count += 6
    return None


@logger.catch()
def main():
    create_start_records_lvl_1()
    log_get_cookies(LOGIN, PASSWORD)
    level = 1
    while len(get_names_level_status_0(level)) > 1:
        if level == 6:
            break
        for name in get_names_level_status_0(level):
            create_counter(str(datetime.datetime.now().date()), 0)
            name_ = str(name[0])
            number = check_counter(str(datetime.datetime.now().date()))[0][0]
            if number > 9999:
                logger.info('REQUEST LIMIT')
                logger.info('PAUSE')
                pause = (24 - datetime.datetime.now().hour)*60*60
                time.sleep(int(pause))
            response = create_request(name_, number)
            number += 1
            update_counter(number, str(datetime.datetime.now().date()))
            try:
                if response.status_code == 200:
                    update_status(name)
                    data = response.json()
                    data["updated"] = str(datetime.datetime.now())
                    logger.info(f'{name} Response: {response.status_code} Count request: {number}')
                    with open(f'{PATH_JSON}{name_}_{level}.json', 'w', encoding='utf-8') as file:
                        json.dump(data, file, indent=4, ensure_ascii=False)
                else:
                    logger.info(f'{name} Response: {response.status_code} Count request: {number}')
            except AttributeError:
                continue
            try:
                companies = data['companies_list']
            except KeyError:
                continue
            for item in companies:
                create_record(item['company']['ogrn'], level + 1, 0)
        level += 1
    logger.info('Performed')


if __name__ == '__main__':
    main()
