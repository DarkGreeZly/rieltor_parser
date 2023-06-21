import datetime
import time
import requests
from bs4 import BeautifulSoup
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
import aiohttp
import asyncio
import tracemalloc
import zlib
import base64
import json
from sqlalchemy import select

MAIN_URL = 'https://rieltor.ua'
urls_parameters = ['flats-sale/',
                   'flats-sale/newhouse/',
                   'flats-rent/newhouse/',
                   'flats-rent/',
                   'houses-sale/',
                   'houses-rent/',
                   'areas-sale/',
                   'commercials-sale/',
                   'commercials-rent/']

metadata = db.MetaData()
engine = db.create_engine("mysql+pymysql://yarikOdmen:developer70@localhost:3306/eBazaDB")


def create_base(engine):
    rieltor_data = db.Table("rieltor_data", metadata,
                            db.Column("id", db.Integer, primary_key=True, autoincrement=1),
                            db.Column("city", db.String(250)),
                            db.Column("city_name", db.String(250)),
                            db.Column("region", db.String(250)),
                            db.Column("street", db.String(250)),
                            db.Column("price", db.String(250)),
                            db.Column("rooms", db.String(250)),
                            db.Column("floors", db.String(250)),
                            db.Column("meters", db.String(250)),
                            db.Column("land_area", db.String(250)),
                            db.Column("markers", db.String(1000)),
                            db.Column("agency", db.String(250)),
                            db.Column("image", db.String(5000)),
                            db.Column("longitude", db.String(250)),
                            db.Column("latitude", db.String(250)),
                            db.Column("rieltor_id", db.String(250), unique=True),
                            db.Column("option", db.String(250)),
                            db.Column("phone_number", db.String(250)))
    metadata.create_all(engine)
    return rieltor_data


def get_cities():
    cities = {}
    html = requests.get(MAIN_URL)
    soup = BeautifulSoup(html.text, 'html.parser')
    for city in soup.findAll('div', class_="nav_item_option_geo_city js_nav_input"):
        cities[city['data-index-url']] = city.text.strip()
    return cities


def get_obls(cities):
    obls = {}
    for city in cities:
        html = requests.get(MAIN_URL + city)
        soup = BeautifulSoup(html.text, 'html.parser')
        for obl in soup.findAll('div', class_='nav_item_option_geo_obl js_nav_input'):
            obls[obl['data-index-url']] = obl.text.strip()
    return obls

    # async def template_cards(city, city_name, option, rieltor_data, connection):
    #     async def get_prices(card):
    #         price = await card.find('strong', class_='catalog-card-price-title')
    #         return price.text
    #
    #     async def get_flat_info(card):
    #         room = ''
    #         floor = ''
    #         meter = ''
    #         data = {"rooms": '',
    #                 "floors": '',
    #                 "meters": ''}
    #         data_html = await card.find('div', class_='catalog-card-details')
    #         for info in data_html.findAll('span', class_=""):
    #             if "кімнат" in info.text:
    #                 room = info.text
    #             elif "поверх" in info.text:
    #                 floor = info.text
    #             else:
    #                 meter = info.text
    #         data['rooms'] = room
    #         data['floors'] = floor
    #         data['meters'] = meter
    #         return data
    #
    #     async def get_land_area(card):
    #         details = await card.find('div', class_='catalog-card-details-row')
    #         land_area = await details.find('span')
    #         return land_area.text
    #
    #     async def get_markers(card):
    #         markers = {}
    #         chips = await card.find('div', class_='catalog-card-chips')
    #         for marker in chips.findAll('span'):
    #             if marker.text.strip() == 'БЕЗ КОМІСІЇ':
    #                 markers['commission'] = 'БЕЗ КОМІСІЇ'
    #         for marker in chips.findAll('a'):
    #             if marker['data-analytics-event'] == 'card-click-subway_chip':
    #                 markers['metro'] = marker.text.strip()
    #             if marker['data-analytics-event'] == 'card-click-landmark_chip':
    #                 markers['landmark'] = marker.text.strip()
    #             if marker['data-analytics-event'] == 'card-click-newhouse_chip':
    #                 markers['newhouse'] = marker.text.strip()
    #
    #         return markers
    #
    #     async def get_agency(card):
    #         author = await card.find('div', class_='catalog-card-author-subtitle')
    #         agency = await author.find('div', class_='catalog-card-author-company')
    #         if agency is not None:
    #             return agency.find('a').text
    #         else:
    #             return author.find('span').text
    #
    #     async def get_image(card):
    #         images = []
    #         media = await card.find('div', class_='offer-photo-slider-slides-container')
    #         for img in media.findAll('img'):
    #             images.append(img['data-src'])
    #         return images
    #
    #     async def get_street(card):
    #         street = await card.find('div', class_='catalog-card-address')
    #         return street.text
    #
    #     async def get_region(card):
    #         region = await card.find('div', class_='catalog-card-region')
    #         return region.text
    #
    #     async def get_phone(card):
    #         phone = await card.find('div', class_='hide catalog-card-author-phones')
    #         phone_number = await phone.find('a')
    #         return phone_number.text
    #
    #     html = requests.get(MAIN_URL + city + option)
    #     soup = BeautifulSoup(html.text, 'html.parser')
    #     for card in soup.findAll('div', class_='catalog-card'):
    #         phone = get_phone(card)
    #         price = get_prices(card)
    #         info = get_flat_info(card)
    #         markers = get_markers(card)
    #         agency = get_agency(card)
    #         images = get_image(card)
    #         street = get_street(card)
    #         region = get_region(card)
    #         region = [i.strip() for i in region.split(',')]
    #         region = ', '.join(region)
    #         land_area = get_land_area(card)
    #         lg = card['data-longitude']
    #         lt = card['data-latitude']
    #         id = card['data-catalog-item-id']
    #         city = city.strip('/')
    #         # for deserialization use json.loads(markers)
    #         selection_query = db.select(rieltor_data).where(rieltor_data.c.rieltor_id == id)
    #         if connection.execute(selection_query).fetchone() is None:
    #             insertion_query = await rieltor_data.insert().values(
    #                 city=city, region=region, street=street, price=price, rooms=info['rooms'], floors=info['floors'], meters=info['meters'],
    #                 markers=json.dumps(markers), land_area=land_area, agency=agency,
    #                 image=json.dumps(images), longitude=lg, latitude=lt, rieltor_id=id, option=option, city_name=city_name, phone_number=phone)
    #             await connection.execute(insertion_query)
    #             await connection.commit()
    # print(phone, price, info, markers, agency, street, region, lg, lt, city_name, id)

async def template_cards(city, city_name, option, rieltor_data, connection):
        async def get_prices(card):
            price = card.find('strong', class_='catalog-card-price-title')
            return price.text

        async def get_flat_info(card):
            room = ''
            floor = ''
            meter = ''
            data = {
                "rooms": '',
                "floors": '',
                "meters": ''
            }
            data_html = card.find('div', class_='catalog-card-details')
            for info in data_html.findAll('span', class_=""):
                if "кімнат" in info.text:
                    room = info.text
                elif "поверх" in info.text:
                    floor = info.text
                else:
                    meter = info.text
            data['rooms'] = room
            data['floors'] = floor
            data['meters'] = meter
            return data

        async def get_land_area(card):
            details = card.find('div', class_='catalog-card-details-row')
            land_area = details.find('span')
            return land_area.text

        async def get_markers(card):
            markers = {}
            chips = card.find('div', class_='catalog-card-chips')
            for marker in chips.findAll('span'):
                if marker.text.strip() == 'БЕЗ КОМІСІЇ':
                    markers['commission'] = 'БЕЗ КОМІСІЇ'
            for marker in chips.findAll('a'):
                if marker['data-analytics-event'] == 'card-click-subway_chip':
                    markers['metro'] = marker.text.strip()
                if marker['data-analytics-event'] == 'card-click-landmark_chip':
                    markers['landmark'] = marker.text.strip()
                if marker['data-analytics-event'] == 'card-click-newhouse_chip':
                    markers['newhouse'] = marker.text.strip()

            return markers

        async def get_agency(card):
            author = card.find('div', class_='catalog-card-author-subtitle')
            agency = author.find('div', class_='catalog-card-author-company')
            if agency is not None:
                return agency.find('a').text
            else:
                return author.find('span').text

        async def get_image(card):
            images = []
            media = card.find('div', class_='offer-photo-slider-slides-container')
            for img in media.findAll('img'):
                images.append(img['data-src'])
            return images

        async def get_street(card):
            street = card.find('div', class_='catalog-card-address')
            return street.text

        async def get_region(card):
            region = card.find('div', class_='catalog-card-region')
            return region.text

        async def get_phone(card):
            phone = card.find('div', class_='hide catalog-card-author-phones')
            phone_number = phone.find('a')
            return phone_number.text

        tracemalloc.start()

        async with aiohttp.ClientSession() as session:
            url = MAIN_URL + city + option
            async with session.get(url) as response:
                html = await response.text()

        snapshot_before = tracemalloc.take_snapshot()

        soup = BeautifulSoup(html, 'html.parser')
        tasks = []

        for card in soup.findAll('div', class_='catalog-card'):
            tasks.append(asyncio.ensure_future(get_phone(card)))
            tasks.append(asyncio.ensure_future(get_prices(card)))
            tasks.append(asyncio.ensure_future(get_flat_info(card)))
            tasks.append(asyncio.ensure_future(get_markers(card)))
            tasks.append(asyncio.ensure_future(get_agency(card)))
            tasks.append(asyncio.ensure_future(get_image(card)))
            tasks.append(asyncio.ensure_future(get_street(card)))
            tasks.append(asyncio.ensure_future(get_region(card)))

        results = await asyncio.gather(*tasks)

        for i in range(0, len(results), 8):
            phone = results[i]
            price = results[i + 1]
            info = results[i + 2]
            markers = results[i + 3]
            agency = results[i + 4]
            images = results[i + 5]
            street = results[i + 6]
            region = results[i + 7]

            images = json.dumps(images)
            images = zlib.compress(images.encode())
            images = base64.b64encode(images).decode()
            region = [i.strip() for i in region.split(',')]
            region = ', '.join(region)
            land_area = await get_land_area(card)
            lg = card['data-longitude']
            lt = card['data-latitude']
            id = card['data-catalog-item-id']
            city = city.strip('/')

            selection_query = select(rieltor_data).where(rieltor_data.c.rieltor_id == id)
            if connection.execute(selection_query).fetchone() is None:
                insertion_query = rieltor_data.insert().values(
                    city=city, region=region, street=street, price=price, rooms=info['rooms'], floors=info['floors'],
                    meters=info['meters'],
                    markers=json.dumps(markers), land_area=land_area, agency=agency,
                    image=images, longitude=lg, latitude=lt, rieltor_id=id, option=option,
                    city_name=city_name, phone_number=phone)
                connection.execute(insertion_query)

                connection.commit()
                snapshot_after = tracemalloc.take_snapshot()  # Capture memory allocation after the processing
                # top_stats = snapshot_after.compare_to(snapshot_before, 'lineno')
                # for stat in top_stats[:10]:  # Print top 10 statistics
                #     print(stat)

                tracemalloc.stop()


async def get_page_count(city):
    html = requests.get(MAIN_URL + city + urls_parameters[0])
    temp = 2
    soup = BeautifulSoup(html.text, 'html.parser')
    for page in soup.findAll('a', class_='pager-btn'):
        if int(page.text) > temp:
            temp = int(page.text)
    return temp


async def start_parser():
    cities = get_cities()
    obls = get_obls(cities)
    global rieltor_data
    rieltor_data = create_base(engine)
    connection = engine.connect()
    delete_query = db.delete(rieltor_data)
    connection.execute(delete_query)
    connection.commit()
    for city in cities:
        print(city, cities[city])
        for option in urls_parameters:
                # try:
            await template_cards(city, cities[city], option, rieltor_data, connection)
                # except Exception:
                #     time.sleep(10)
                #     pass
    for obl in obls:
        print(obls[obl])
        for option in urls_parameters:
            try:
                await template_cards(obl, obls[obl], option, rieltor_data, connection)
            except Exception:
                time.sleep(10)
                pass
    print("parsing completed")


if __name__ == "__main__":
    asyncio.run(start_parser())
    # while True:
    #     now = datetime.datetime.now()
    #     if now.hour == 0 and now.minute == 0:
    #         asyncio.run(start_parser())
    #         time.sleep((24 * 60 * 60) - 10)
