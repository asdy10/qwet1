import asyncio
import os
import pickle
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import logging


def get_cookies(phone, driver):
    driver.get('https://client.work-zilla.com/')
    s = input()
    time.sleep(1)
    pickle.dump(driver.get_cookies(), open(f'cookies_{phone}', 'wb'))
    time.sleep(5)
    driver.close()


def save_cookies(driver, phone):
    time.sleep(1)
    dir_ = os.path.abspath(os.curdir)
    pickle.dump(driver.get_cookies(), open(f'cookies_{phone}', 'wb'))
    time.sleep(1)


def set_cookies(driver, phone):
    driver.get('https://youla.ru')
    dir_ = os.path.abspath(os.curdir)
    for cookie in pickle.load(open(f'cookies_{phone}', 'rb')):
        driver.add_cookie(cookie)
    time.sleep(1)
    driver.refresh()
    time.sleep(1)
    return driver


def get_open_browser():
    logging.getLogger('WDM').setLevel(logging.NOTSET)
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
    opts = Options()
    opts.add_argument('--disable-logging')
    opts.add_argument(f'user-agent={user_agent}')
    opts.add_argument('--start-maximized')
    #opts.headless = True
    driver = webdriver.Chrome(options=opts)
    driver.get('https://google.com')
    return driver

