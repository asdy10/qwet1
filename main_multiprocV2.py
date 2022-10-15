import json
import time
from collections import Counter
import requests
import aiohttp
import asyncio
from itertools import groupby
from data.config import DATABASE, THREADS, SEMAPHORES
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
    for i in ads:
        if min_p <= i[2] <= max_p:
            new_ads.append(i)
    return new_ads


def delete_ads_published(ads, published):
    new_ads = []
    for i in ads:
        if time.time() - get_date_published(i[0]) <= published:
            new_ads.append(i)
    return new_ads


async def delete_ads_published_all(ads, date_published):
    new_ads = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in ads:
            task = asyncio.create_task(get_date_published_async(session, i[0]))
            tasks.append(task)
        res = await asyncio.gather(*tasks)
        for i, j in zip(ads, res):
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
        global ads
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
            #print(owner_param)
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
            #print(res)
            try:
                arr['date_created'] = res['date_published']
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
    global ads
    return arr_active, arr_sold


async def parse_products(session, page, category, published, sort):

    t = round(time.time())
    price_from = 1000
    price_to = 100000
    date_to = t + 10
    date_from = t - 30
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
    url = 'https://api-gw.youla.io/federation/graphql'
    f = False
    async with semaphore:
        try:
            response = await session.post(url=url, headers=arr, json=r)
            try:
                res = await response.json(content_type='application/json')
                f = True
            except:
                f = False
        except Exception as e:
            pass
    global pages
    new_adss = []
    if f:
        try:
            for i in res['data']['feed']['items']:
                try:
                    new_adss.append([i['product']['id'], 'https://youla.ru' + i['product']['url'], i['product']['price']['realPrice']['price']/100, i['product']['name'].replace(',', ' ')])
                except:
                    pass
        except:
            pass
    if len(new_adss) > 20:
        print(price_from, price_to, len(new_adss), category)
    return {category: new_adss}


async def get_all_pages(category, res_arr_price_, published):
    async with aiohttp.ClientSession() as session:
        tasks = []
        all_ads = []
        for i in category:
            task1 = asyncio.create_task(parse_products(session, 0, i, published, 'DATE_PUBLISHED_DESC'))
            tasks.append(task1)
        res = await asyncio.gather(*tasks)
        new_ads_ = []
        for k in res:
            #print('k', k)
            for i in k:
                if k[i]:
                    if len(k[i]) > 29:
                        tasks = []
                        print('find more', i)
                        for j in range(pages - 1):
                            task1 = asyncio.create_task(parse_products(session, j, i, published, 'DATE_PUBLISHED_DESC'))
                            tasks.append(task1)

                        res = await asyncio.gather(*tasks)
                    new_ads_ = []
                    for i2 in res:
                        for j2 in i2:
                            if i2[j2] != []:
                                new_ads_ += i2[j2]
            #print('new ads?', new_ads_)
            if new_ads_:
                all_ads += new_ads_

    return all_ads


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
    DB_CONF = DATABASE['mongodb']
    DBHandler = MongoHandler(**DB_CONF)
    db = DBHandler
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
    return new_arr


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
        st_time = time.time()
        mid_time = time.time()
        ads = asyncio.get_event_loop().run_until_complete(get_all_pages(params['category'], [1000, 100000], params['published']))
        ads = [el for el, _ in groupby(ads)]
        start_len = len(ads)
        print('got all ads                ', len(ads), round(time.time() - mid_time, 4), params['category'])
        mid_time = time.time()
        old_len = len(ads)
        ads = delete_copy(ads, 'full')
        print('deleted copy -', old_len-len(ads))
        print('filtered by database       ', len(ads), round(time.time() - mid_time, 4), params['category'][0])
        if time.time() - mid_time > 20:
            # clear table
            DB_CONF = DATABASE['mongodb']
            DBHandler = MongoHandler(**DB_CONF)
            db = DBHandler
            col = db.collection('full')
            db.delete_records(col, {})
        mid_time = time.time()
        ads = asyncio.get_event_loop().run_until_complete(delete_ads_by_owner_async(ads, params))
        print('filtered by owner          ', len(ads), round(time.time() - mid_time, 4), params['category'][0])
        mid_time = time.time()
        ads = [el for el, _ in groupby(ads)]
        # ads = asyncio.get_event_loop().run_until_complete(delete_by_active_sold_ads_async(ads, params))
        # print('filtered by active and sold', len(ads), round(time.time() - mid_time, 4))
        if ads:
            DB_CONF = DATABASE['mongodb']
            DBHandler = MongoHandler(**DB_CONF)
            db = DBHandler
            # Получить коллекцию
            #col = db.collection('buffer')
            col2 = db.collection('full')
            stu_list = [{'idx': i[0], 'link': i[1], 'phone': ' ', 'price': i[2], 'name': i[3], 'pub_date': i[4], 'seller': i[5], 'views': i[6], 'sold_count': i[7]} for i in ads]

            #db.insert_many_records(col, stu_list)
            db.insert_many_records(col2, stu_list)
            print('FINISH', len(ads), round(time.time() - st_time, 4), params['category'])
    except Exception as e:
        print(e)


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
semaphore = asyncio.Semaphore(SEMAPHORES)
from multiprocessing import Pool
if __name__ == "__main__":
    while True:
        with open('log.txt', 'a', encoding='utf-8') as f:
            f.write('\n###########################\n')
        start_full_time = time.time()
        global arr_names_set
        params = {}
        task_array = []
        cat1 = ['igry-dlya-pristavok-i-pk', 'prochee-drugoe', 'sport-otdyh-drugoe', 'zapchasti', 'foto-video-aksessuary', 'sportivnoe-pitanie', 'hehndmejd-aksessuary', 'konsoli-igry', 'kompyutery-aksessuary', 'proektory', 'smartfony-planshety-aksessuary', 'komplektuyushchie', 'strizhka-brite', 'avtohimiya', 'muzhskaya-odezhda-svitery-tolstovki', 'potolki', 'zhenskaya-odezhda-golovnye-ubory', 'detskie-kupanie', 'podstavki-tumby', 'muzhskaya-odezhda-drugoe', 'muzykalnye-instrumenti', 'detskaya-odezhda-konverty', 'tennis-badminton-ping-pong', 'turizm', 'setevoe-oborudovanie', 'solyarii-i-zagar', 'kuhonnaya-mebel', 'fotoapparaty', 'detskie-zdorove-i-uhod', 'motoehkipirovka', 'plity', 'muzhskaya-odezhda-dzhinsy-bryuki', 'medicinskie-tovary', 'knigi-zhurnaly', 'stoly-stulya', 'detskaya-odezhda-aksessuary', 'dlya-beremennyh', 'zhenskaya-odezhda-svitery-tolstovki', 'krovati', 'bagazhniki-farkopy', 'vytyazhki', 'zhenskaya-odezhda-verhnyaya']
        cat2 = ['filmy', 'nastolnye-igry', 'edinoborstva', 'kancelyarskie-tovary', 'rubashki', 'izmelchenie-i-smeshivanie', 'roliki-skejtbording', 'samokaty-i-giroskutery', 'detskaya-odezhda-golovnye-ubory', 'ohrana-signalizaciya', 'klimaticheskaya', 'mikrofony', 'muzhskaya-odezhda-kombinezony', 'audio-video', 'naushniki', 'uhod-za-volosami', 'prigotovlenie-napitkov', 'planshety', 'ruli-dzhoistiki-geympady', 'specodezhda', 'igry-s-myachom', 'aksessuary', 'detskaya-odezhda-shtany-i-shorty', 'muzhskaya-odezhda-golovnye-ubory', 'zhenskaya-odezhda-kombinezony', 'ehlektrika', 'ehlektroinstrumenty', 'umnye-chasy', 'kollekcionirovanie', 'shkafy-komody', 'krasota-i-zdorove-drugoe', 'remont-i-stroitelstvo-drugoe', 'makiyazh', 'nizhnee-bele-plavki', 'strojmaterialy', 'detskaya-odezhda-obuv', 'kormlenie-pitanie', 'manikyur-pedikyur', 'osveshchenie', 'hehndmejd-kukly-igrushki', 'avtozapchasti']
        cat3 = ['vodnye-vidy', 'programmnoe-obespechenie', 'sputnikovoe-i-cifrovoe-tv', 'detskie-radio-i-videonyani', 'tatu-i-tatuazh', 'mp3-pleery', 'usiliteli-resivery', 'chekhly', 'studyinoe-oborudovanie', 'zimnie-vidy', 'muzykalnye-centry-i-magnitoly', 'kosmetika', 'muzhskaya-odezhda-sportivnaya', 'uhod-za-licom', 'aksessuary-i-instrumenty', 'zhenskaya-odezhda-pidzhaki-kostyumy', 'otoplenie-ventilyaciya', 'detskaya-odezhda-kombinezony-i-bodi', 'santekhnika', 'binokli-teleskopy', 'futbolki-topy', 'ehlektronnye-knigi', 'sportivnaya-zashhita', 'protivougonnye-ustrojstva', 'dom-dacha-oformlenie-interera', 'detskie-tovary-dlya-ucheby', 'videokamery', 'videonablyudenie', 'parfyumeriya', 'prigotovlenie-edy', 'detskaya-odezhda-svitery-i-tolstovki', 'detskie-kukly-igrushki', 'domashnyaya', 'zhenskaya-odezhda-sportivnaya', 'pylesosy', 'dom-dacha-drugoe', 'zhenskaya-odezhda-aksessuary', 'detskaya-odezhda-polzunki-i-raspashonki', 'obektivy', 'fotoprintery', 'muzhskaya-odezhda-obuv']
        cat4 = ['bilety', 'avtoelektronika-i-gps', 'bilyard-i-bouling', 'fotovspyshki', 'feny-ukladka', 'materialy-dlya-tvorchestva', 'detskaya-odezhda-futbolki', 'oformlenie-prazdnikov', 'posudomoechnye-mashiny', 'shtativy-monopody', 'izmeritelnye-instrumenty', 'detskaya-odezhda-drugoe', 'podguzniki-pelenki', 'fotoramki', 'kulery-i-filtry-dlya-vody', 'hehndmejd-posuda', 'zaryadnye-ustrojstva', 'zhenskaya-odezhda-drugoe', 'zhenskaya-odezhda-domashnyaya', 'printery-i-skanery', 'domashnie-kinoteatry', 'okna', 'stacionarnye-telefony', 'racii-i-sputnikovye-telefony', 'shvejnoe-oborudovanie', 'avtokresla', 'detskaya-odezhda-sportivnaya-odezhda', 'rasteniya', 'noutbuki', 'muzhskaya-odezhda-pidzhaki-kostyumy', 'dom-dacha-posuda', 'akusticheskie-sistemy', 'hehndmejd-ukrasheniya', 'shiny-diski', 'velosipedy-samokaty', 'stiralnye-mashiny', 'muzhskaya-odezhda-verhnyaya', 'smartfony', 'ohota-rybalka', 'detskaya-odezhda-pidzhaki-i-kostyumy', 'zhenskaya-odezhda-obuv']
        cat5 = ['klaviatury-i-myshi', 'hobbi-razvlecheniya-drugoe', 'hehndmejd-drugoe', 'detskaya-odezhda-nizhnee-bele', 'multimedia', 'nakopiteli-dannyh-i-kartridery', 'vneshnie-akkumulyatory', 'detskaya-odezhda-domashnyaya-odezhda', 'bitovaya-himiya', 'utyugi', 'mediapleery', 'sadovaya-mebel', 'ruchnye-instrumenty', 'tekstil-kovry', 'zhenskaya-odezhda-kupalniki', 'futbolki-polo', 'sad-ogorod', 'holodilniki', 'muzyka', 'muzhskaya-odezhda-aksessuary', 'detskaya-odezhda-platya-i-yubki', 'sredstva-dlya-gigieny', 'televizory-proektory', 'detskie-tovary-dlya-mam', 'avto-moto-drugoe', 'dveri', 'hehndmejd-oformlenie-interera', 'trenazhery-fitnes', 'zhenskaya-odezhda-dzhinsy-bryuki', 'monobloki', 'detskaya-odezhda-bluzy-i-rubashki', 'divany-kresla', 'vesy', 'monitory', 'bele-kupalniki', 'mebel', 'bluzy-rubashki', 'zhenskaya-odezhda-platya-yubki', 'kolyaski', 'detskie-drugoe', 'detskaya-odezhda-verhnyaya-odezhda']
        cats = cat1 + cat2 + cat3 + cat4 + cat5
        cut_ads = lambda lst, sz: [lst[i:i + sz] for i in range(0, len(lst), sz)]
        cutted = cut_ads(cats, 20)
        for cat in cutted:
            params['category'] = cat
            params['max_price'] = 100000
            params['min_price'] = 1000
            params['published'] = 30
            task_array.append(params.copy())
            params = {}
        pool = Pool(THREADS)
        pool.map(script, task_array)
        s = f'END MAZAFAKA {time.time() - start_full_time}\n'
        print(s)
        with open('log.txt', 'a', encoding='utf-8') as f:
            f.write(s)

