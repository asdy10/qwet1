import asyncio
import json
import sys
import time

import aiohttp
import requests
from selenium.webdriver.common.by import By

from connect_browser import get_open_browser, set_cookies

for i in reversed(range(1, 5)):
    sys.stderr.write(f"{i:2d}\r")
    time.sleep(0.5)




page3 = """{"operationName":"catalogProductsBoard","variables":{"sort":"DEFAULT","attributes":[{"slug":"categories",
"value":["zhenskaya-odezhda-aksessuary"],"from":null,"to":null}],"datePublished":{"to":1658658568,"from":1658572168},
"location":{"latitude":54.726141,"longitude":55.947499,"city":null,"distanceMax":null},"search":"","cursor":{
"page":0,"totalProductsCount":30,"totalPremiumProductsCount":1,"dateUpdatedTo":1658658566}},"extensions":{
"persistedQuery":{"version":1,"sha256Hash":"6e7275a709ca5eb1df17abfb9d5d68212ad910dd711d55446ed6fa59557e2602"}}} """

page4 = '''{'operationName': 'catalogProductsBoard', 'variables': {'sort': 'DEFAULT', 'attributes': [{'slug': 'categories', 
'value': ['zhenskaya-odezhda-aksessuary'], 'from': None, 'to': None}], 'datePublished': {'to': 1658658568, 'from': 1658572168},
 'location': {'latitude': 54.726141, 'longitude': 55.947499, 'city': None, 'distanceMax': None}, 'search': ''}, 
 'extensions': {'persistedQuery': {'version': 1, 'sha256Hash': '6e7275a709ca5eb1df17abfb9d5d68212ad910dd711d55446ed6fa59557e2602'}},
  'cursor': {'page': 1, 'totalProductsCount': 60, 'totalPremiumProductsCount': 1, 'dateUpdatedTo': 1658658566}}'''

page2 = """{"operationName":"catalogProductsBoard","variables":{"sort":"DEFAULT","attributes":[{"slug":"categories",
"value":["zhenskaya-odezhda-aksessuary"],"from":null,"to":null}],"datePublished":{"to":1658658568,"from":1658572168},
"location":{"latitude":54.726141,"longitude":55.947499,"city":null,"distanceMax":null},"search":""},"extensions":{
"persistedQuery":{"version":1,"sha256Hash":"6e7275a709ca5eb1df17abfb9d5d68212ad910dd711d55446ed6fa59557e2602"}}} """

page1 = """{"operationName":"catalogProductsBoard","variables":{"sort":"DEFAULT","attributes":[{"slug":"categories",
"value":["zhenskaya-odezhda-aksessuary"],"from":null,"to":null}],"datePublished":{"to":1658658568,"from":1658572168},
"location":{"latitude":54.726141,"longitude":55.947499,"city":null,"distanceMax":null},"search":"","cursor":""},
"extensions":{"persistedQuery":{"version":1,
"sha256Hash":"6e7275a709ca5eb1df17abfb9d5d68212ad910dd711d55446ed6fa59557e2602"}}} """


async def parse_products(session, page, category):
    t = round(time.time())
    date_to = 1658663100
    date_from = t - 86400
    page1 = '{"operationName":"catalogProductsBoard","variables":{"sort":"DATE_PUBLISHED_DESC","attributes":[{"slug":"categories",' \
            '"value":["' + f'{category}' + '"],"from":null,"to":null}],"datePublished":null,' \
            '"location":{"latitude":51.7996544,"longitude":58.2975488,"city":null,"distanceMax":null},"search":""},' \
            '"extensions":{"persistedQuery":{"version":1,"sha256Hash":"6e7275a709ca5eb1df17abfb9d5d68212ad910dd711d55446ed6fa59557e2602"}}}'

    page2 = """{"operationName":"catalogProductsBoard","variables":{"sort":"DATE_PUBLISHED_DESC","attributes":[{"slug":"categories",
    "value":["zhenskaya-odezhda-aksessuary"],"from":null,"to":null}],"datePublished":{"to":""" + f'{date_to}' + ""","from":""" + f'{date_from}' + """},
    "location":{"latitude":54.726141,"longitude":55.947499,"city":null,"distanceMax":null},"search":""},"extensions":{
    "persistedQuery":{"version":1,"sha256Hash":"6e7275a709ca5eb1df17abfb9d5d68212ad910dd711d55446ed6fa59557e2602"}}} """
    headers = """Accept-Encoding: gzip, deflate, br
    Accept-Language: ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7
    Connection: keep-alive
    Host: api-gw.youla.io
    Origin: https://youla.ru
    Referer: https://youla.ru/all/zhenskaya-odezhda/aksessuary?attributes[sort_field]=date_published
    Sec-Fetch-Dest: empty
    Sec-Fetch-Mode: cors
    Sec-Fetch-Site: cross-site
    User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36
    accept: */*
    appId: web/3
    authorization: 
    content-type: application/json
    sec-ch-ua: ".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"
    sec-ch-ua-mobile: ?0
    sec-ch-ua-platform: "Windows"
    uid: 62d5752869b72
    x-app-id: web/3
    x-offset-utc: +05:00
    x-uid: 62d5752869b72
    x-youla-splits: 8a=2|8b=2|8c=0|8m=0|8v=0|8z=0|16a=0|16b=0|64a=1|64b=0|100a=21|100b=14|100c=0|100d=0|100m=0"""
    arr = {i.split(':')[0]: i.split(': ')[1] for i in headers.split('\n')}
    r = json.loads(page1)
    if page == 0:
        r['variables']['cursor'] = ''
    else:
        #r['variables']['cursor'] = '{\"page\":' + f'{page-1}' + ',\"totalProductsCount\":' + f'{30 * page}' + ',\"totalPremiumProductsCount\":1,\"dateUpdatedTo\":' + f'{date_to-1}' + '}'
        r['variables']['cursor'] = '{\"page\":' + f'{page-1}' + ',\"totalProductsCount\":' + f'{30 * page}' + ',\"totalPremiumProductsCount\":1,\"dateUpdatedTo\":' + f'1658686926' + '}'
    #print(r['variables']['cursor'])
    #1658686926
    url = 'https://api-gw.youla.io/federation/graphql'
    #res = requests.post(url=url, headers=arr, json=r).json()
    #print(res)
    async with semaphore:
        response = await session.post(url=url, headers=arr, json=r)
        res = await response.json()
    global counter
    counter += 1
    global pages
    sys.stderr.write(f'pars products {counter}/{pages}\r')
    #print(res['data']['feed']['pageInfo'])
    new_ads = []
    for i in res['data']['feed']['items']:
        try:
            new_ads.append([i['product']['id'], 'https://youla.ru' + i['product']['url'], i['product']['price']['realPrice']['price']/100, i['product']['name']])
        except:
            pass
    return new_ads


async def get_all_pages(category):
    async with aiohttp.ClientSession() as session:
        tasks = []
        global counter
        counter = 0
        for i in range(pages - 1):
            task = asyncio.create_task(parse_products(session, i, category))
            tasks.append(task)
        # res = await asyncio.wait(tasks)
        # print(res)
        res = await asyncio.gather(*tasks)
        ads = []
        for i in res:
            ads += i
        for i in ads:
            print(i)
       # print(res)
       #  for i in res:
       #      for j in i:
       #          try:
       #
       #              print(j['product']['id'], f"https://youla.ru{j['product']['url']}")
       #              with open('test2.txt', 'a') as f:
       #                  f.write(j['product']['id'] + f" https://youla.ru{j['product']['url']}" + '\n')
       #              global count
       #              count += 1
       #              global mn
       #              mn.add(j['product']['id'])
       #          except:
       #              pass
    return ads

def check_result():
    # print(time.time() - 1658658568)
    # print(time.time() - 1658572168)
    # print(time.time() - 1658658566)
    count = 0
    with open('test.txt', 'r', encoding="utf-8") as f:
        all = f.read()
    with open('test2.txt', 'r', encoding="utf-8") as f:
        s = f.read()
    for i in s.split('\n'):
        if i.split(' ')[0] in all:
            print(i)
            count += 1
    print(count)
count = 0


def get_all_ads_published():
    driver = get_open_browser()
    driver = set_cookies(driver, '1')

    time.sleep(3)
    category = 'zhenskaya-odezhda/aksessuary'
    url = f'https://youla.ru/all/{category}?attributes[term_of_placement][from]=-1%20day&attributes[term_of_placement][to]=now&attributes[sort_field]=date_published'
    driver.get(url)
    time.sleep(2)
    find = False
    ads = []
    old_add = []
    while not find:
        new_ads, showed_elements = get_all_ads_on_page(driver)
        ads += new_ads

        try:
            if old_add == ads[-1]:
                find = True
                print('find complete')
            else:
                # elements = driver.find_elements(By.CLASS_NAME, 'sc-lkSWcV')
                # new_height = elements[-1].location['y']
                new_height = showed_elements[-1].location['y']
                print(f'find: {len(ads)}, scroll')
                driver.execute_script("window.scrollTo(0, {});".format(new_height))
                time.sleep(0.5)
            old_add = ads[-1]
        except:
            new_height = showed_elements[-1].location['y']
            print(f'find: {len(ads)}, scroll except')
            driver.execute_script("window.scrollTo(0, {});".format(new_height))
            time.sleep(0.5)
    return ads



def get_all_ads_on_page(driver):
    res = []
    elements = driver.find_elements(By.CLASS_NAME, 'sc-lkSWcV')
    showed_elements = []
    for i in elements:
        try:
            href = i.find_element(By.TAG_NAME, 'a')
            id = i.find_element(By.TAG_NAME, 'figure')
            # sc-dYdMoy
            price = i.find_element(By.CLASS_NAME, 'sc-dYdMoy').text
            price = float(price.replace(' ', ''))
            res.append([id.get_attribute('data-test-id'), href.get_attribute('href'), price, id.text.split('\n')[-1]])
            showed_elements.append(i)
        except:
            pass
    return res, showed_elements


counter = 0
semaphore = asyncio.Semaphore(10)
pages = 100
mn = set()
import pandas as pd

if __name__ == '__main__':
    ads = [['62e00e3ab15b9415ec31b446', 'https://youla.ru/tutaev/zhenskaya-odezhda/platya-yubki/prodam-platie-mangho-46-razmier-62e00e3ab15b9415ec31b446', 1000.0, 'Продам платье Манго 46 размер', '61ba2a79c9ae68354f295b23']]
    name = 'zhenskaya-odezhda'
    # df = pd.DataFrame({'Link': [i[0] for i in ads],
    #                    'Phone': [i[1] for i in ads]})
    # df.to_excel(f'./{name}.xlsx', sheet_name='Result', index=False)
    time.sleep(0)
    ads = [1234,54,64,16,42,5,3, 3,1243,54,16]
    ads2 = [[43, 1234], [1243, 43, 'asdf', "fasd"], [54, 123], [16, 54], [7, 5]]
    print(ads)
    new_ads = set(ads)
    new_ads2 = set()
    for i in ads2:
        new_ads2.add(str(i))
    arr = []
    for i in new_ads2:
        i = str(i).replace('[', '').replace(']', '').replace(' ', '')
        arr.append(i.split(','))
    ads2 = str(ads2).replace('[[', '').replace(']]', '')
    print(ads2)
    new_ads = set(ads2.split('], ['))
    print(new_ads)
    new_adss = []
    for i in new_ads:
        append_value = [j.replace("'", '') for j in i.split(', ')]
        append_value[1] = float(append_value[1])
        new_adss.append(append_value)
    print(new_adss)
    # res = pd.read_excel(f'{name}.xlsx', sheet_name='Result')
    # def_arr = res.values.tolist()
    # print(def_arr)
    # for i in ads:
    #     if i[0] not in str(def_arr):
    #         def_arr.append(i)
    # print(def_arr)
    # ads = get_all_ads_published()
    # with open('test.txt', 'a', encoding="utf-8") as f:
    #     for i in ads:
    #         try:
    #             f.write(str(i)+'\n')
    #         except:
    #             pass
    # print('start')
    category = 'zhenskaya-odezhda-aksessuary'
    # loop = asyncio.get_event_loop()
    # res = loop.run_until_complete(get_all_pages(category))
    # print(count)
    # print(res)
    # with open('test3.txt', 'r') as f:
    #     arr = f.read().split('\n')
    # arrs = set(arr)
    # print(len(arrs), len(arr))
    #print(len(mn))
#    asyncio.create_task(get_all_pages())
    #check_result()
    # print(time.time() - 1658663100)
    # print(time.gmtime(1658663100))



