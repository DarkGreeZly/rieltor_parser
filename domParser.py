import time
import requests
from bs4 import BeautifulSoup
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
import json
from sqlalchemy import select

MAIN_URL = 'https://rieltor.ua'
urls_parameters = ['flats-sale/',
                   'flats-sale/newhouse/',
                   'flats-rent/',
                   'houses-sale/',
                   'houses-rent/',
                   'areas-sale/',
                   'commercials-sale/',
                   'commercials-rent/']


def create_base(engine):
    metadata = db.MetaData()

    rieltor_data = db.Table("rieltor_data", metadata,
                            db.Column("id", db.Integer, primary_key=True),
                            db.Column("city", db.String),
                            db.Column("region", db.String),
                            db.Column("street", db.String),
                            db.Column("price", db.String),
                            db.Column("rooms", db.String),
                            db.Column("floors", db.String),
                            db.Column("meters", db.String),
                            db.Column("markers", db.String),
                            db.Column("agency", db.String),
                            db.Column("image", db.String),
                            db.Column("longitude", db.String),
                            db.Column("latitude", db.String),
                            db.Column("rieltor_id", db.String),
                            db.Column("option", db.String),
                            db.Column("phone_number", db.String))
    metadata.create_all(engine)
    return rieltor_data


def get_cities():
    cities = []
    html = requests.get(MAIN_URL)
    soup = BeautifulSoup(html.text, 'html.parser')
    for city in soup.findAll('div', class_="nav_item_option_geo_city js_nav_input"):
        cities.append(city['data-index-url'])
    return cities


def get_obls(cities):
    obls = []
    for city in cities:
        html = requests.get(MAIN_URL + city)
        soup = BeautifulSoup(html.text, 'html.parser')
        for obl in soup.findAll('div', class_='nav_item_option_geo_obl js_nav_input'):
            obls.append(obl['data-index-url'])
    return obls


def template_cards(city, option, rieltor_data, connection):
    def get_prices(card):
        price = card.find('strong', class_='catalog-card-price-title')
        return price.text

    def get_flat_info(card):
        room = ''
        floor = ''
        meter = ''
        data = {"rooms": '',
                "floors": '',
                "meters": ''}
        data_html = card.find('div', class_='catalog-card-details')
        for info in data_html.findAll('span', class_=""):
            if "кімнат" in info.text:
                room = info.text
            elif "поверх" in info.text:
                floor = info.text
            else:
                meter = info.text
        data['room'] = room
        data['floor'] = floor
        data['meter'] = meter
        return data

    def get_markers(card):
        markers = []
        chips = card.find('div', class_='catalog-card-chips')
        for marker in chips.findAll('a'):
            markers.append(marker.text.strip())
        return markers

    def get_agency(card):
        author = card.find('div', class_='catalog-card-author-subtitle')
        agency = author.find('div', class_='catalog-card-author-company')
        if agency is not None:
            return agency.find('a').text
        else:
            return author.find('span').text

    def get_image(card):
        images = []
        media = card.find('div', class_='offer-photo-slider-slides-container')
        for img in media.findAll('img'):
            images.append(img['data-src'])
        return images

    def get_street(card):
        street = card.find('div', class_='catalog-card-address')
        return street.text

    def get_region(card):
        region = card.find('div', class_='catalog-card-region')
        return region.text

    def get_phone(card):
        phone = card.find('div', class_='hide catalog-card-author-phones')
        phone_number = phone.find('a')
        return phone_number.text

    html = requests.get(MAIN_URL + city + option)
    soup = BeautifulSoup(html.text, 'html.parser')
    for card in soup.findAll('div', class_='catalog-card'):
        phone = get_phone(card)
        price = get_prices(card)
        info = get_flat_info(card)
        markers = get_markers(card)
        agency = get_agency(card)
        images = get_image(card)
        street = get_street(card)
        region = get_region(card)
        region = region.split().join(' ')
        lg = card['data-longitude']
        lt = card['data-latitude']
        id = card['data-catalog-item-id']
        city_name = city.strip('/')
        # for deserialization use json.loads(markers)
        insertion_query = rieltor_data.insert().values(
            city=city_name, region=region, street=street, price=price, rooms=info['room'], floors=info['floor'], meters=info['meter'],
            markers=json.dumps(markers), agency=agency,
            image=json.dumps(images), longitude=lg, latitude=lt, rieltor_id=id, option=option, phone_number=phone)
        connection.execute(insertion_query)
        connection.commit()


def get_page_count(city):
    html = requests.get(MAIN_URL + city + urls_parameters[0])
    temp = 2
    soup = BeautifulSoup(html.text, 'html.parser')
    for page in soup.findAll('a', class_='pager-btn'):
        if int(page.text) > temp:
            temp = int(page.text)
    return temp


if __name__ == '__main__':
    cities = get_cities()
    obls = get_obls(cities)
    engine = db.create_engine("sqlite:///from_rieltor.db")
    global rieltor_data
    rieltor_data = create_base(engine)
    connection = engine.connect()
    while True:
        delete_query = db.delete(rieltor_data)
        connection.execute(delete_query)
        connection.commit()
        for city in cities:
            for option in urls_parameters:
                try:
                    template_cards(city, option, rieltor_data, connection)
                except Exception:
                    time.sleep(10)
                    pass
        for obl in obls:
            for option in urls_parameters:
                try:
                    template_cards(obl, option, rieltor_data, connection)
                except Exception:
                    time.sleep(10)
                    pass
        time.sleep(3600)
