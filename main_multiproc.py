import json
import time
from collections import Counter
import requests
import aiohttp
import asyncio
from itertools import groupby
from data.config import DATABASE
from db.storage import MongoHandler


def get_date_published(id):
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
    }
    url = f'https://api.youla.io/api/v1/product/{id}'
    res = requests.get(url=url, headers=headers).json()

    return res['data']['date_published']


def delete_ads_price(ads, max_p, min_p):
    new_ads = []
    count = 0
    for i in ads:
        count += 1
        #sys.stderr.write(f'check price {count}/{len(ads)}\r')
        if min_p <= i[2] <= max_p:
            new_ads.append(i)
    return new_ads


def delete_ads_published(ads, published):
    new_ads = []
    count = 0
    for i in ads:
        count += 1
        #sys.stderr.write(f'check published date {count}/{len(ads)}\r')
        if time.time() - get_date_published(i[0]) <= published:
            new_ads.append(i)
    return new_ads


async def delete_ads_published_all(ads, date_published):
    new_ads = []
    global counter
    counter = 0
    # global semaphore
    # semaphore = asyncio.Semaphore(100)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in ads:
            task = asyncio.create_task(get_date_published_async(session, i[0]))
            tasks.append(task)
        #res = await asyncio.wait(tasks)
        #print(res)
        res = await asyncio.gather(*tasks)
        for i, j in zip(ads, res):
            #if time.time() - j <= date_published:
            new_ads.append(i + [j])
    return new_ads


async def get_date_published_async(session, id):
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
    }
    url = f'https://api.youla.io/api/v1/product/{id}'
    f = False
    async with semaphore:
        try:
            response = await session.get(url=url, headers=headers)
            try:
                res = await response.json(content_type='application/json')
                f = True
            except:
                f = False
        except Exception as e:
            #print(e)
            pass
    if f:
        global counter
        counter += 1
        global ads
        #sys.stderr.write(f'check published date {counter}/{len(ads)}\r')
        try:
            return res['data']['date_created']
        except:
            return 1658000000
    else:
        return 1658000000


async def delete_ads_by_owner_async(ads, params):

    global counter
    counter = 0
    tasks = []
    async with aiohttp.ClientSession() as session:
        for i in ads:
            task = asyncio.create_task(get_owner_async(session, i[0]))
            tasks.append(task)
        res = await asyncio.gather(*tasks)
    new_ads_ = []

    for i, owner_param in zip(ads, res):
        try:
            # if params['active_ad_min'] <= owner_param['prods_active_cnt'] <= params['active_ad_max'] \
            #         and params['close_ad_min'] <= owner_param['prods_sold_cnt'] <= params['close_ad_max'] \
            #         and params['views_min'] <= owner_param['views'] <= params['views_max'] \
            #         and params['followers_min'] <= owner_param['followers'] <= params['followers_max']:

            new_ads_.append(i + [owner_param['date_created'], owner_param['owner_id'], owner_param['views'], owner_param['prods_sold_cnt']])
        except:
            pass
    return new_ads_


async def get_owner_async(session, pid):
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
    }
    url = f'https://api.youla.io/api/v1/product/{pid}'
    arr = {}
    f = False
    async with semaphore:
        try:
            response = await session.get(url=url, headers=headers)
            try:
                res1 = await response.json(content_type='application/json')
                f = True
            except:
                f = False
        except Exception as e:
            #print(e)
            pass
    if f:
        try:
            res = res1['data']
            owner = res['owner']
            try:
                arr['date_created'] = res['date_created']
            except:
                arr['date_created'] = 1658000000
            arr['prods_active_cnt'] = owner['prods_active_cnt']
            arr['prods_sold_cnt'] = owner['prods_sold_cnt']
            arr['owner_id'] = owner['id']
            arr['views'] = res['views']
            arr['price'] = res['discounted_price']
        except:
            f = False
    if not f:
        arr['prods_active_cnt'] = 1000000
        arr['prods_sold_cnt'] = 1000000
        arr['owner_id'] = 1000000
        arr['views'] = 1000000
        arr['price'] = 1000000
        arr['date_created'] = 1658000000

    timestamp = round(time.time() * 100)
    # url = f'https://api.youla.io/api/v1/user/{arr["owner_id"]}?app_id=web%2F3&uid=62d5752869b72&timestamp={timestamp}'
    # f = False
    # async with semaphore:
    #     try:
    #         response = await session.get(url=url, headers=headers)
    #         try:
    #             res1 = await response.json(content_type='application/json')
    #             f = True
    #         except:
    #             f = False
    #     except Exception as e:
    #         print(e)
    # if f:
    #     try:
    #         res = res1['data']
    #         arr['followers'] = res['followers_cnt']
    #     except:
    #         arr['followers'] = 100000
    # else:
    #     arr['followers'] = 1000000
    # global counter
    # global ads
    # counter += 1
    # sys.stderr.write(f'check owner {counter}/{len(ads)}\r')
    return arr


async def delete_by_active_sold_ads_async(ads, params):
    new_ads = []
    global counter
    counter = 0
    tasks = []
    async with aiohttp.ClientSession() as session:
        for i in ads:
            task = asyncio.create_task(get_products_owner_async(session, i[5]))
            tasks.append(task)
        res = await asyncio.gather(*tasks)
    for i, active_sold in zip(ads, res):
        count_active = 0
        count_sold = 0
        arr_active, arr_sold = active_sold
        for j in arr_active:
            if params['count_active_ad_by_price_price_min'] <= j <= params['count_active_ad_by_price_price_max']:
                count_active += 1
        for j in arr_sold:
            if params['count_sold_ad_by_price_price_min'] <= j <= params['count_sold_ad_by_price_price_max']:
                count_sold += 1
        if params['count_active_ad_by_price_min'] <= count_active <= params['count_active_ad_by_price_max'] \
                and params['count_sold_ad_by_price_min'] <= count_sold <= params['count_sold_ad_by_price_max']:
            new_ads.append(i)
    return new_ads


async def get_products_owner_async(session, id):
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
    }
    timestamp = round(time.time() * 100)
    active_product_url = f'https://api.youla.io/api/v1/user/{id}/profile/products?app_id=web/3&uid=62d5752869b72&timestamp={timestamp}&limit=1000&show_inactive=0&features='
    sold_product_url = f'https://api.youla.io/api/v1/user/{id}/profile/soldproducts?app_id=web/3&uid=62d5752869b72&timestamp={timestamp}&limit=1000'
    f, f2 = False, False
    async with semaphore:
        try:
            response = await session.get(url=active_product_url, headers=headers)
            try:
                res1 = await response.json(content_type='application/json')
                f = True
            except:
                f = False
        except Exception as e:
            #print(e)
            pass
    if f:
        try:
            res = res1['data']
        except:
            f = False
    async with semaphore:
        try:
            response = await session.get(url=sold_product_url, headers=headers)
            try:
                res1 = await response.json(content_type='application/json')
                f2 = True
            except:
                f2 = False
        except Exception as e:
            #print(e)
            pass
    if f2:
        try:
            res2 = res1['data']
        except:
            f2 = False
    if f and f2:
        arr_active = [i['discounted_price'] / 100 for i in res]
        arr_sold = [i['discounted_price'] / 100 for i in res2]
    else:
        arr_active = []
        arr_sold = []
    global counter
    global ads
    counter += 1
    #sys.stderr.write(f'check active and sold {counter}/{len(ads)}\r')
    return arr_active, arr_sold


async def parse_products(session, page, category, price_from, price_to, published, sort):
    if minimum_price != price_from:
        price_from += 1
    t = round(time.time())
    date_to = t
    date_from = t - published
    # page1 = '{"operationName":"catalogProductsBoard","variables":{"sort":"' + sort + '","attributes":[' \
    #         '{"slug":"price","value":null,"from":' + f'{price_from*100}' + ',"to":' + f'{price_to*100}' + '},' \
    #         '{"slug":"zhenskaya_odezhda_aksessuary_tip","value":["8251"],"from":null,"to":null},{"slug":"categories",' \
    #         '"value":["' + f'{category}' + '"],"from":null,"to":null}],"datePublished":{"to":' + f'{date_to}' + ',"from":' + f'{date_from}' + '},' \
    #         '"location":{"latitude":51.7996544,"longitude":58.2975488,"city":null,"distanceMax":null},"search":""},' \
    #         '"extensions":{"persistedQuery":{"version":1,"sha256Hash":"6e7275a709ca5eb1df17abfb9d5d68212ad910dd711d55446ed6fa59557e2602"}}}'
    page2 = '{"operationName":"catalogProductsBoard","variables":{"sort":"' + sort + '",' \
           '"attributes":[{"slug":"price","value":null,"from":' + f'{price_from*100}' + ',"to":' + f'{price_to*100}' + '},' \
           '{"slug":"categories","value":["' + f'{category}' + '"],"from":null,"to":null}],' \
           '"datePublished":{"to":' + f'{date_to}' + ',"from":' + f'{date_from}' + '},' \
           '"location":{"latitude":51.7996544,"longitude":58.2975488,"city":null,"distanceMax":null},' \
           '"search":""},"extensions":{"persistedQuery":{"version":1,"sha256Hash":"6e7275a709ca5eb1df17abfb9d5d68212ad910dd711d55446ed6fa59557e2602"}}}'
    r = json.loads(page2)
    if page == 0:
        r['variables']['cursor'] = ''
    else:
        r['variables']['cursor'] = '{\"page\":' + f'{page - 1}' + ',\"totalProductsCount\":' + f'{30 * page}' + ',\"totalPremiumProductsCount\":1,\"dateUpdatedTo\":' + f'{t - 2}' + '}'

    headers2 = """Accept-Encoding: gzip, deflate, br
Accept-Language: ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7
Connection: keep-alive
Host: api-gw.youla.io
Origin: https://youla.ru
Sec-Fetch-Dest: empty
Sec-Fetch-Mode: cors
Sec-Fetch-Site: cross-site
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36
accept: */*
appId: web/3
authorization: 
content-type: application/json
sec-ch-ua: ".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"
sec-ch-ua-mobile: ?0
sec-ch-ua-platform: "Windows"
uid: 62d9c8e0d5ec0
x-app-id: web/3
x-offset-utc: +05:00
x-uid: 62d9c8e0d5ec0
x-youla-splits: 8a=3|8b=7|8c=0|8m=0|8v=0|8z=0|16a=0|16b=0|64a=6|64b=0|100a=60|100b=39|100c=0|100d=0|100m=0"""

    arr = {i.split(':')[0]: i.split(': ')[1] for i in headers2.split('\n')}
    #print(r['variables']['cursor'])
    #1658686926
    url = 'https://api-gw.youla.io/federation/graphql'
    #res = requests.post(url=url, headers=arr, json=r).json()
    #print(res)
    f = False
    async with semaphore:
        try:
            response = await session.post(url=url, headers=arr, json=r)
            try:
                res = await response.json(content_type='application/json')
                f = True
            except:
                #print('ERROR')
                f = False
        except Exception as e:
            pass
            #print(e)

    global counter
    counter += 1
    global counter_ads

    global pages

    #print(res['data']['feed']['pageInfo'])
    new_adss = []
    #print('res2', res)
    if f:
        try:
            for i in res['data']['feed']['items']:
                try:
                    new_adss.append([i['product']['id'], 'https://youla.ru' + i['product']['url'], i['product']['price']['realPrice']['price']/100, i['product']['name'].replace(',', ' ')])
                except:
                    pass
        except:
            pass
    counter_ads += len(new_adss)
    # global res_arr_price
    # sys.stderr.write(f'pars products {counter}/{len(res_arr_price) * (pages - 1) * 4} sum_ads: {counter_ads}\r')
    return new_adss


async def get_all_pages(category, res_arr_price_, published):
    async with aiohttp.ClientSession() as session:
        tasks = []
        global counter
        counter = 0
        global counter_ads
        counter_ads = 0
        for i in range(len(res_arr_price_) - 1):

            for j in range(pages - 1):
                task1 = asyncio.create_task(parse_products(session, j, category, res_arr_price_[i], res_arr_price_[i + 1], published, 'DATE_PUBLISHED_DESC'))
                task2 = asyncio.create_task(parse_products(session, j, category, res_arr_price_[i], res_arr_price_[i + 1], published, 'DEFAULT'))
                task3 = asyncio.create_task(parse_products(session, j, category, res_arr_price_[i], res_arr_price_[i + 1], published, 'PRICE_ASC'))
                task4 = asyncio.create_task(parse_products(session, j, category, res_arr_price_[i], res_arr_price_[i + 1], published, 'DISTANCE_ASC'))

                tasks.append(task1)
                tasks.append(task2)
                tasks.append(task3)
                tasks.append(task4)

        # res = await asyncio.wait(tasks)
        # print(res)
        res = await asyncio.gather(*tasks)
        #print('res', res)
        new_ads_ = []
        for i in res:
            for j in i:
                if j != []:
                    new_ads_.append(j)
    return new_ads_


def transform_set_to_list(arr):
    new_arr = set()
    for i in arr:
        new_arr.add(str(i))
    arr = []
    for i in new_arr:
        i = str(i).replace('[', '').replace(']', '').replace(' ', '')
        arr.append(i.split(','))
    return arr


def transform_arr_to_set(arr):
    arr = str(arr).replace('[[', '').replace(']]', '')
    new_arr = set(arr.split('], ['))
    return new_arr


def delete_copy(arr, name):
    old_len = len(arr)
    DB_CONF = DATABASE['mongodb']
    DBHandler = MongoHandler(**DB_CONF)
    db = DBHandler
    # Получить коллекцию
    col = db.collection(name)
    condition2 = {}
    res2 = db.find_records(col, condition2)
    arr_set_idx = set()
    for r in res2:
        arr_set_idx.add(r['idx'])
    new_arr = []
    for i in arr:
        if i[0] not in arr_set_idx:
            new_arr.append(i)
    print(f'deleted copy               -{old_len - len(new_arr)}')
    return new_arr


def main(params):
    start_full_time = time.time()
    #params = {}
    params['max_price'] = 100000
    params['min_price'] = 1
    global minimum_price
    minimum_price = params['min_price']
    params['active_ad_min'] = 0
    params['active_ad_max'] = 100000
    params['close_ad_min'] = 0
    params['close_ad_max'] = 100000
    params['views_min'] = 0
    params['views_max'] = 100000
    params['followers_min'] = 0
    params['followers_max'] = 100000
    params['published'] = 86400
    params['count_active_ad_by_price_min'] = 0
    params['count_active_ad_by_price_max'] = 100000
    params['count_active_ad_by_price_price_min'] = 0
    params['count_active_ad_by_price_price_max'] = 100000
    params['count_sold_ad_by_price_min'] = 0
    params['count_sold_ad_by_price_max'] = 100000
    params['count_sold_ad_by_price_price_min'] = 0
    params['count_sold_ad_by_price_price_max'] = 100000


    print('END MAZAFAKA', time.time() - start_full_time)


def run_get_all_pages(category, arr_price, published):
    loop = asyncio.new_event_loop()
    new_ads = loop.run_until_complete(get_all_pages(category, arr_price, published))
    global ads
    ads += new_ads


def script(params):
    try:
        print('START', params['category'])
        global ads
        global minimum_price
        ads = []

        st_time = time.time()
        mid_time = time.time()
        loop = asyncio.get_event_loop()
        arr_min_price = []
        min_price = params['min_price']

        arr_price = [500, 700, 999, 1000, 1001, 1100, 1199, 1200, 1299, 1300, 1400, 1499, 1500, 1600, 1700, 1899, 1999, 2000, 2499, 2500, 2700, 2999, 3000, 3500, 4000, 4499, 4500, 4999, 5000, 5500, 6000, 7500, 9500, 11500, 13500, 15500, 17500, 19500, 21500, 23500, 25500, 27500, 29500, 31500, 33500, 35500, 37500, 40000, 100000]
        arr_min_price.append(params['max_price'])
        global res_arr_price
        res_arr_price = []
        for i in arr_price:
            if params['min_price'] <= i <= params['max_price']:
                res_arr_price.append(i)

        if params['min_price'] < res_arr_price[0]:
            res_arr_price.insert(0, params['min_price'])
        if params['max_price'] > res_arr_price[-1]:
            res_arr_price.append(params['max_price'])
        ads = []

        # cut_ads = lambda lst, sz: [lst[i:i + sz] for i in range(0, len(lst), sz)]
        #
        # cutted = cut_ads(res_arr_price, 30)
        # procs = []
        # for i in cutted:
        #     proc = Process(target=run_get_all_pages, args=(params['category'], i, params['published']))
        #     procs.append(proc)
        #     proc.start()
        #
        # for proc in procs:
        #     proc.join()
        ads = asyncio.get_event_loop().run_until_complete(get_all_pages(params['category'], res_arr_price, params['published']))
        ads = [el for el, _ in groupby(ads)]
        try:
            with open('res.txt', 'a') as f:
                f.write(f'{len(ads)} {params["category"]}\n')
        except Exception as e:
            print(e)
        print('got all ads                ', len(ads), round(time.time() - mid_time, 4), params['category'])
        mid_time = time.time()
        ads = delete_copy(ads, params['category'])
        print('filtered by database       ', len(ads), round(time.time() - mid_time, 4), params['category'])
        # mid_time = time.time()
        # ads = asyncio.get_event_loop().run_until_complete(delete_ads_published_all(ads, params['published']))
        # print('filtered by published date ', len(ads), round(time.time() - mid_time, 4))
        mid_time = time.time()
        ads = asyncio.get_event_loop().run_until_complete(delete_ads_by_owner_async(ads, params))
        print('filtered by owner          ', len(ads), round(time.time() - mid_time, 4), params['category'])
        mid_time = time.time()
        ads = [el for el, _ in groupby(ads)]
        # ads = asyncio.get_event_loop().run_until_complete(delete_by_active_sold_ads_async(ads, params))
        # print('filtered by active and sold', len(ads), round(time.time() - mid_time, 4))
        # for i in ads:
        #     print(i)


        if ads:
            DB_CONF = DATABASE['mongodb']
            DBHandler = MongoHandler(**DB_CONF)
            db = DBHandler
            # Получить коллекцию
            #col = db.collection(params['category'])
            col2 = db.collection('full')
            stu_list = [{'idx': i[0], 'link': i[1], 'phone': ' ', 'price': i[2], 'name': i[3], 'pub_date': i[4], 'seller': i[5], 'views': i[6], 'sold_count': i[7]} for i in ads]

            #db.insert_many_records(col, stu_list)
            db.insert_many_records(col2, stu_list)
            print('FINISH', len(ads), round(time.time() - st_time, 4), params['category'])
    except:
        pass

# get_owner('5eb16abd22a449ae77127253')
category1 = ['Вещи, электроника и прочее', 'Запчасти и автотовары']
category2 = {'Женский гардероб': 'zhenskaya-odezhda', 'Мужской гардероб': 'muzhskaya-odezhda',
             'Детский гардероб': 'detskaya-odezhda',
             'Детские товары': 'detskie', 'Хэндмейд': 'hehndmejd', 'Телефоны и планшеты': 'smartfony-planshety',
             'Фото- и видеокамеры': 'foto-video', 'Компьютерная техника': 'kompyutery',
             'ТВ, аудио, видео': 'ehlektronika',
             'Бытовая техника': 'bytovaya-tekhnika', 'Для дома и дачи': 'dom-dacha',
             'Стройматериалы и инструменты': 'remont-i-stroitelstvo',
             'Красота и здоровье': 'krasota-i-zdorove', 'Спорт и отдых': 'sport-otdyh',
             'Хобби и развлечения': 'hobbi-razvlecheniya', 'Прочее': 'prochee'}

avto_moto = {'Запчасти': 'avtozapchasti', 'Шины и диски': 'shiny-diski', 'Масла и автохимия': 'avtohimiya',
             'Автоэлектроника и GPS': 'avtoelektronika-i-gps',
             'Аксессуары и инструменты': 'aksessuary-i-instrumenty',
             'Аудио и видео': 'audio-video','Противоугонные устройства': 'protivougonnye-ustrojstva',
             'Багажные системы и прицепы': 'bagazhniki-farkopy',
             'Мотоэкипировка': 'motoehkipirovka','Другое': 'drugoe'}


full_array = {'Женский гардероб': {'Аксессуары': 'aksessuary', 'Блузы и рубашки': 'bluzy-rubashki', 'Будущим мамам': 'dlya-beremennyh', 'Верхняя одежда': 'verhnyaya', 'Головные уборы': 'golovnye-ubory', 'Домашняя одежда': 'domashnyaya', 'Комбинезоны': 'kombinezony', 'Купальники': 'kupalniki', 'Нижнее белье': 'bele-kupalniki', 'Обувь': 'obuv', 'Пиджаки и костюмы': 'pidzhaki-kostyumy', 'Платья и юбки': 'platya-yubki', 'Свитеры и толстовки': 'svitery-tolstovki', 'Спортивная одежда': 'sportivnaya', 'Футболки и топы': 'futbolki-topy', 'Штаны и шорты': 'dzhinsy-bryuki', 'Другое': 'drugoe'},
              'Мужской гардероб': {'Аксессуары': 'aksessuary', 'Верхняя одежда': 'verhnyaya', 'Головные уборы': 'golovnye-ubory', 'Домашняя одежда': 'domashnyaya', 'Комбинезоны': 'kombinezony', 'Нижнее белье': 'nizhnee-bele-plavki', 'Обувь': 'obuv', 'Пиджаки и костюмы': 'pidzhaki-kostyumy', 'Рубашки': 'rubashki', 'Свитеры и толстовки': 'svitery-tolstovki', 'Спецодежда': 'specodezhda', 'Спортивная одежда': 'sportivnaya', 'Футболки и поло': 'futbolki-polo', 'Штаны и шорты': 'dzhinsy-bryuki', 'Другое': 'drugoe'},
              'Детский гардероб': {'Аксессуары': 'aksessuary', 'Блузы и рубашки': 'bluzy-i-rubashki', 'Верхняя одежда': 'verhnyaya-odezhda', 'Головные уборы': 'golovnye-ubory', 'Домашняя одежда': 'domashnyaya-odezhda', 'Комбинезоны и боди': 'kombinezony-i-bodi', 'Конверты': 'konverty', 'Нижнее белье': 'nizhnee-bele', 'Обувь': 'obuv', 'Пиджаки и костюмы': 'pidzhaki-i-kostyumy', 'Платья и юбки': 'platya-i-yubki', 'Ползунки и распашонки': 'polzunki-i-raspashonki', 'Свитеры и толстовки': 'svitery-i-tolstovki', 'Спортивная одежда': 'sportivnaya-odezhda', 'Футболки': 'futbolki', 'Штаны и шорты': 'shtany-i-shorty', 'Другое': 'drugoe'},
              'Детские товары': {'Автокресла': 'avtokresla', 'Здоровье и уход': 'zdorove-i-uhod', 'Игрушки и игры': 'kukly-igrushki', 'Коляски': 'kolyaski', 'Кормление и питание': 'kormlenie-pitanie', 'Купание': 'kupanie', 'Обустройство детской': 'mebel', 'Подгузники и горшки': 'podguzniki-pelenki', 'Радио- и видеоняни': 'radio-i-videonyani', 'Товары для мам': 'tovary-dlya-mam', 'Товары для учебы': 'tovary-dlya-ucheby', 'Другое': 'drugoe'},
              'Хэндмейд': {'Косметика': 'kosmetika', 'Украшения': 'ukrasheniya', 'Куклы и игрушки': 'kukly-igrushki', 'Оформление интерьера': 'oformlenie-interera', 'Аксессуары': 'aksessuary', 'Оформление праздников': 'oformlenie-prazdnikov', 'Канцелярия': 'kancelyarskie-tovary', 'Посуда': 'posuda', 'Другое': 'drugoe'},
              'Телефоны и планшеты': {'Мобильные телефоны': 'smartfony', 'Планшеты': 'planshety', 'Умные часы и браслеты': 'umnye-chasy', 'Стационарные телефоны': 'stacionarnye-telefony', 'Рации и спутниковые телефоны': 'racii-i-sputnikovye-telefony', 'Запчасти': 'zapchasti', 'Внешние аккумуляторы': 'vneshnie-akkumulyatory', 'Зарядные устройства': 'zaryadnye-ustrojstva', 'Чехлы': 'chekhly', 'Аксессуары': 'aksessuary'},
              'Фото- и видеокамеры': {'Фотоаппараты': 'fotoapparaty', 'Видеокамеры': 'videokamery', 'Видеонаблюдение': 'videonablyudenie', 'Объективы': 'obektivy', 'Фотовспышки': 'fotovspyshki', 'Аксессуары': 'aksessuary', 'Штативы и стабилизаторы': 'shtativy-monopody', 'Студийное оборудование': 'studyinoe-oborudovanie', 'Цифровые фоторамки': 'fotoramki', 'Компактные фотопринтеры': 'fotoprintery', 'Бинокли и оптические приборы': 'binokli-teleskopy'},
              'Компьютерная техника': {'Ноутбуки': 'noutbuki', 'Компьютеры': 'monobloki', 'Мониторы': 'monitory', 'Клавиатуры и мыши': 'klaviatury-i-myshi', 'Оргтехника и расходники': 'printery-i-skanery', 'Сетевое оборудование': 'setevoe-oborudovanie', 'Мультимедиа': 'multimedia', 'Накопители данных и картридеры': 'nakopiteli-dannyh-i-kartridery', 'Программное обеспечение': 'programmnoe-obespechenie', 'Рули, джойстики, геймпады': 'ruli-dzhoistiki-geympady', 'Комплектующие и запчасти': 'komplektuyushchie', 'Аксессуары': 'aksessuary'},
              'ТВ, аудио, видео': {'Телевизоры': 'televizory-proektory', 'Проекторы': 'proektory', 'Акустика, колонки, сабвуферы': 'akusticheskie-sistemy', 'Домашние кинотеатры': 'domashnie-kinoteatry', 'DVD, Blu-ray и медиаплееры': 'mediapleery', 'Музыкальные центры и магнитолы': 'muzykalnye-centry-i-magnitoly', 'MP3-плееры и портативное аудио': 'mp3-pleery', 'Электронные книги': 'ehlektronnye-knigi', 'Спутниковое и цифровое ТВ': 'sputnikovoe-i-cifrovoe-tv', 'Аудиоусилители и ресиверы': 'usiliteli-resivery', 'Наушники': 'naushniki', 'Микрофоны': 'mikrofony', 'Аксессуары': 'aksessuary'},
              'Бытовая техника': {'Весы': 'vesy', 'Вытяжки': 'vytyazhki', 'Измельчение и смешивание': 'izmelchenie-i-smeshivanie', 'Климатическая техника': 'klimaticheskaya', 'Кулеры и фильтры для воды': 'kulery-i-filtry-dlya-vody', 'Плиты и духовые шкафы': 'plity', 'Посудомоечные машины': 'posudomoechnye-mashiny', 'Приготовление еды': 'prigotovlenie-edy', 'Приготовление напитков': 'prigotovlenie-napitkov', 'Пылесосы и пароочистители': 'pylesosy', 'Стиральные машины': 'stiralnye-mashiny', 'Утюги и уход за одеждой': 'utyugi', 'Холодильники': 'holodilniki', 'Швейное оборудование': 'shvejnoe-oborudovanie'},
              'Для дома и дачи': {'Бытовая химия': 'bitovaya-himiya', 'Диваны и кресла': 'divany-kresla', 'Кровати и матрасы': 'krovati', 'Кухонные гарнитуры': 'kuhonnaya-mebel', 'Освещение': 'osveshchenie', 'Оформление интерьера': 'oformlenie-interera', 'Охрана и сигнализации': 'ohrana-signalizaciya', 'Подставки и тумбы': 'podstavki-tumby', 'Посуда': 'posuda', 'Растения и семена': 'rasteniya', 'Сад и огород': 'sad-ogorod', 'Садовая мебель': 'sadovaya-mebel', 'Столы и стулья': 'stoly-stulya', 'Текстиль и ковры': 'tekstil-kovry', 'Шкафы и комоды': 'shkafy-komody', 'Другое': 'drugoe'},
              'Стройматериалы и инструменты': {'Двери': 'dveri', 'Измерительные инструменты': 'izmeritelnye-instrumenty', 'Окна': 'okna', 'Отопление и вентиляция': 'otoplenie-ventilyaciya', 'Потолки': 'potolki', 'Ручные инструменты': 'ruchnye-instrumenty', 'Сантехника и водоснабжение': 'santekhnika', 'Стройматериалы': 'strojmaterialy', 'Электрика': 'ehlektrika', 'Электроинструменты': 'ehlektroinstrumenty', 'Другое': 'drugoe'},
              'Красота и здоровье': {'Макияж': 'makiyazh', 'Маникюр и педикюр': 'manikyur-pedikyur', 'Товары для здоровья': 'medicinskie-tovary', 'Парфюмерия': 'parfyumeriya', 'Стрижка и удаление волос': 'strizhka-brite', 'Уход за волосами': 'uhod-za-volosami', 'Уход за кожей': 'uhod-za-licom', 'Фены и укладка': 'feny-ukladka', 'Тату и татуаж': 'tatu-i-tatuazh', 'Солярии и загар': 'solyarii-i-zagar', 'Средства для гигиены': 'sredstva-dlya-gigieny', 'Другое': 'drugoe'},
              'Спорт и отдых': {'Спортивная защита': 'sportivnaya-zashhita', 'Велосипеды': 'velosipedy-samokaty', 'Ролики и скейтбординг': 'roliki-skejtbording', 'Самокаты и гироскутеры': 'samokaty-i-giroskutery', 'Бильярд и боулинг': 'bilyard-i-bouling', 'Водные виды спорта': 'vodnye-vidy', 'Единоборства': 'edinoborstva', 'Зимние виды спорта': 'zimnie-vidy', 'Игры с мячом': 'igry-s-myachom', 'Охота и рыбалка': 'ohota-rybalka', 'Туризм и отдых на природе': 'turizm', 'Теннис, бадминтон, дартс': 'tennis-badminton-ping-pong', 'Тренажеры и фитнес': 'trenazhery-fitnes', 'Спортивное питание': 'sportivnoe-pitanie', 'Другое': 'drugoe'},
              'Хобби и развлечения': {'Билеты': 'bilety', 'Видеофильмы': 'filmy', 'Игровые приставки': 'konsoli-igry', 'Игры для приставок и ПК': 'igry-dlya-pristavok-i-pk', 'Книги и журналы': 'knigi-zhurnaly', 'Коллекционирование': 'kollekcionirovanie', 'Материалы для творчества': 'materialy-dlya-tvorchestva', 'Музыка': 'muzyka', 'Музыкальные инструменты': 'muzykalnye-instrumenti', 'Настольные игры': 'nastolnye-igry', 'Другое': 'drugoe'},
              'Прочее': {'Другое': 'drugoe'}}
arr_names = []
for i in full_array:
    for j in full_array[i]:
        arr_names.append(full_array[i][j])
arr_ = Counter(arr_names)
arr_new_names = ['svitery-i-tolstovki', 'konverty', 'bluzy-i-rubashki', 'nizhnee-bele',
                 'sportivnaya-odezhda', 'verhnyaya-odezhda', 'platya-yubki', 'kupalniki', 'zdorove-i-uhod',
                 'radio-i-videonyani', 'ukrasheniya', 'kupanie', 'tovary-dlya-mam', 'futbolki',
                 'domashnyaya-odezhda', 'pidzhaki-i-kostyumy', 'platya-i-yubki',
                 'polzunki-i-raspashonki',
                 'kombinezony-i-bodi', 'shtany-i-shorty', 'tovary-dlya-ucheby']
for i in arr_:
    if arr_[i] > 1:
        arr_new_names.append(i)
#print(arr_new_names)


mn = set()
pages = 25
minimum_price = 0
ads = []
counter = 0
counter_ads = 0
res_arr_price = []
semaphore = asyncio.Semaphore(100)
from multiprocessing import Pool
if __name__ == "__main__":
    while True:
        start_full_time = time.time()
        global arr_names_set
        params = {}
        task_array = []
        for cat in avto_moto:
            params['default_category'] = 'avto-moto'
            if avto_moto[cat] in arr_new_names:
                params['category'] = f'avto-moto-{avto_moto[cat]}'
            else:
                params['category'] = f'{avto_moto[cat]}'
            params['max_price'] = 100000
            params['min_price'] = 1000
            params['published'] = 1000
            task_array.append(params.copy())
            params = {}
            # print(params['category'])
        for cat in category2:
            params['default_category'] = category2[cat]
            for cat2 in full_array[cat]:
                if full_array[cat][cat2] in arr_new_names:
                    params['category'] = f'{category2[cat]}-{full_array[cat][cat2]}'
                else:
                    params['category'] = f'{full_array[cat][cat2]}'
                # print(params['category'])
                if params['category'] == 'muzhskaya-odezhda-domashnyaya':
                    params['category'] = 'domashnyaya'
                if params['category'] == 'ehlektronika-aksessuary':
                    params['category'] = 'aksessuary'
                params['max_price'] = 100000
                params['min_price'] = 1000
                params['published'] = 1000
                task_array.append(params.copy())
                params = {}

        # cut_params = lambda lst, sz: [lst[i:i + sz] for i in range(0, len(lst), sz)]
        # cutted = cut_params(task_array, 30)
        pool = Pool(2)
        pool.map(script, task_array)
        s = f'END MAZAFAKA {time.time() - start_full_time}'
        print(s)
        with open('log.txt', 'a', encoding='utf-8') as f:
            f.write(s)
