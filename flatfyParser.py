import time
import requests
from bs4 import BeautifulSoup
import sqlalchemy as db
import json

MAIN_URL = 'https://rieltor.ua/'
engine = db.create_engine("sqlite:///full_base.db")
connection = engine.connect()
metadata = db.MetaData()


def get_admin_dist(city):
    admin_data = {}
    dists = []
    for i in range(10):
        html = requests.get(MAIN_URL + city + '/flats-sale/?page=' + str(i))
        soup = BeautifulSoup(html.text, 'html.parser')
        for region in soup.find_all('div', class_='catalog-card-region'):
            if len(region.find_all('a')) > 1:
                dists.append(region.find_all('a')[1].text.strip())
            else:
                dists.append(region.find_all('a')[0].text)
        admin_data[city] = list(set(dists))
    return admin_data


def get_micro_dists():
    data = {}
    city = ''
    markers = []
    table = db.Table('rieltor_data', metadata, autoload_with=engine)
    selection_query = db.select(table)
    selection_result = connection.execute(selection_query)
    for row in selection_result.fetchall():
        if city != row[1]:
            if city != '':
                data[city] = list(set(markers))
            city = row[1]
            markers = []
        elif city == row[1]:
            for marker in json.loads(row[6]):
                markers.append(marker)
    return data


def get_page_count(city, option):
    html = requests.get(MAIN_URL + city + '/' + option)
    soup = BeautifulSoup(html.text, 'html.parser')
    max = 0
    for page in soup.find_all('a', class_='pager-btn'):
        if max < int(page.text):
            max = int(page.text)
    return max


def get_street():
    table = db.Table('rieltor_data', metadata, autoload_with=engine)
    selection_query = db.select(table)
    selection_result = connection.execute(selection_query)
    cities = []
    options = []
    data = {}
    streets = []
    for row in selection_result.fetchall():
        cities.append(row[1])
        options.append(row[-1])
    cities = set(cities)
    options = set(options)
    for town in cities:
        for option in options:
            for page in range(int(get_page_count(town, option))):
                html = requests.get(MAIN_URL + town + '/' + option + '?page=' + str(page))
                soup = BeautifulSoup(html.text, 'html.parser')
                for address in soup.find_all('div', class_='catalog-card-address'):
                    streets.append(address.text.split(',')[0])
        data[town] = list(set(streets)) if streets != [] else []
        print(data)
        streets = []
    return data


if __name__ == '__main__':
    admin_data = {}
    table = db.Table('rieltor_data', metadata, autoload_with=engine)
    city = ''
    selection_query = db.select(table)
    selection_result = connection.execute(selection_query)
    with open('streets.json', 'w', encoding='utf8') as f:
        json.dump(get_micro_dists(), f, ensure_ascii=False)
    for row in selection_result.fetchall():
        if city != row[1]:
            city = row[1]

