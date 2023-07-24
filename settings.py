import sqlalchemy as db
from config import metadata, engine, connection, phone_number, current_row, current_time
import aiohttp
from firebase_admin import credentials, firestore
import uuid
import datetime as dt
import haversine as hs
import re
import numpy as np
from shapely.geometry.polygon import Polygon
from shapely.geometry import Point


def create_db_control():
    user_data = db.Table("control_data", metadata,
                         db.Column("id", db.Integer, primary_key=True),
                         db.Column("user_id", db.String(250)),
                         db.Column("phone_number", db.String(250)),
                         db.Column("favorite", db.String(250)),
                         db.Column("complaint", db.String(250)),
                         db.Column("announcement_id", db.String(250)),
                         db.Column("referral", db.String(250)),
                         db.Column("support", db.Boolean, default=False),
                         db.Column("coins", db.Integer, default=0))
    metadata.create_all(engine)


def on_snapshot(col_snapshot, changes, read_time):
    for change in changes:
        doc_data = change.document.to_dict()
        doc_id = change.document.id
        if change.type.name == 'MODIFIED':
            print(f'{change.document.id}')
            print(change.document.to_dict())


def check_id_form1(user_id):
    fire_base = firestore.client()

    collection_ref = fire_base.collection('WebFormOne')
    docs = collection_ref.stream()

    collection_watch = collection_ref.on_snapshot(on_snapshot)

    for doc in docs:
        if str(user_id) == str(doc.id):
            print(doc.to_dict())
            return doc.to_dict()


def check_id_form2(user_id):
    announcements = []
    fire_base = firestore.client()

    collection_ref = fire_base.collection('WebFormTwo')
    docs = collection_ref.stream()

    collection_watch = collection_ref.on_snapshot(on_snapshot)

    for doc in docs:
        if str(user_id) != str(doc.id) and len(str(user_id)) == len(str(doc.id)):
            for id, announcement in doc.to_dict().items():
                print(announcement)
                announcements.append(announcement)
    return announcements


def add_new_user(form, user_id):
    fire_base = firestore.client()
    if form == 'first':
        collection_ref = fire_base.collection('authUserID').document('formOne')
        collection_ref.set(
            {
                'userID': user_id,
                'phone_number': phone_number
            }
        )
    elif form == 'second':
        unique_id = uuid.uuid4().int
        unique_id = unique_id % 10000000000
        collection_ref = fire_base.collection('authUserID').document('formTwo')
        collection_ref.set(
            {
                'userID': user_id,
                'phone_number': phone_number,
                'announcement_id': unique_id
            }
        )


def check_data_from_user(user_id):
    global current_time
    filter = check_id_form1(user_id)
    announcements = check_id_form2(user_id)
    if current_time == '':
        current_time = dt.datetime.now() - dt.timedelta(seconds=30)
        current_time = current_time.strftime("%H:%M:%S %Y-%m-%d")
    temp_time = filter['datatime'][0] + " " + filter['datatime'][1]
    accepted_announcements = []
    if temp_time >= current_time:
        for announcement in announcements:
            if 'currentCity' in filter['GEO'] and 'currentCity' in announcement['GEO']:
                if filter['GEO']['currentCity'] != announcement['GEO']['currentCity']:
                    continue
            if 'typeEstate' in filter['buttons'] and 'typeEstate' in announcement['buttons']:
                if 'Усі Варіанти' in filter['buttons']['typeHouse']:
                    if filter['buttons']['typeEstate'] == ['Квартира'] and announcement['buttons']['typeEstate'] != [
                        'Квартира'] and \
                            filter['buttons']['section'] == ['Оренда'] and announcement['buttons']['section'] != [
                        'Здати в оренду']:
                        continue

                    if filter['buttons']['typeEstate'] == ['Квартира'] and announcement['buttons']['typeEstate'] != [
                        'Квартира'] and \
                            filter['buttons']['section'] == ['Купити'] and announcement['buttons']['section'] != [
                        'Продати']:
                        continue

                if 'Вторинна' in filter['buttons']['typeHouse']:
                    if filter['buttons']['typeEstate'] == ['Квартира'] and announcement['buttons']['typeEstate'] != [
                        'Квартира'] and \
                            filter['buttons']['section'] == ['Оренда'] and announcement['buttons']['section'] != [
                        'Здати в оренду'] and announcement['buttons']['typeHouse'] != ['Вторинна']:
                        continue

                    if filter['buttons']['typeEstate'] == ['Квартира'] and announcement['buttons']['typeEstate'] != [
                        'Квартира'] and \
                            filter['buttons']['section'] == ['Купити'] and announcement['buttons']['section'] != [
                        'Продати'] and announcement['buttons']['typeHouse'] != ['Вторинна']:
                        continue

                if 'Новобудова' in filter['buttons']['typeHouse']:
                    if filter['buttons']['typeEstate'] == ['Квартира'] and announcement['buttons']['typeEstate'] != [
                        'Квартира'] and \
                            filter['buttons']['section'] == ['Оренда'] and announcement['buttons']['section'] != [
                        'Здати в оренду'] and announcement['buttons']['typeHouse'] != ['Новобудова']:
                        continue

                    if filter['buttons']['typeEstate'] == ['Квартира'] and announcement['buttons']['typeEstate'] != [
                        'Квартира'] and \
                            filter['buttons']['section'] == ['Купити'] and announcement['buttons']['section'] != [
                        'Продати'] and announcement['buttons']['typeHouse'] != ['Новобудова']:
                        continue

                if filter['buttons']['typeEstate'] == ['Будинок'] and filter['buttons']['section'] == ['Продаж'] and \
                        announcement['buttons']['typeEstate'] != ['Будинок'] and announcement['buttons']['section'] != [
                    'Продати']:
                    continue

                if filter['buttons']['typeEstate'] == ['Будинок'] and filter['buttons']['section'] == ['Оренда'] and \
                        announcement['buttons']['typeEstate'] != ['Будинок'] and announcement['buttons']['section'] != [
                    'Здати в оренду']:
                    continue

                if filter['buttons']['typeEstate'] == ['Земельна Ділянка'] and filter['buttons']['section'] == [
                    'Продаж'] and announcement['buttons']['typeEstate'] != ['Земельна Ділянка'] and \
                        announcement['buttons']['section'] != ['Продати']:
                    continue

                if filter['buttons']['typeEstate'] == ['Комерційна Нерухомість'] and filter['buttons']['section'] == [
                    'Продаж'] and announcement['buttons']['typeEstate'] != ['Комерційна Нерухомість'] and \
                        announcement['buttons']['section'] != ['Продати']:
                    continue

                if filter['buttons']['typeEstate'] == ['Комерційна Нерухомість'] and filter['buttons']['section'] == [
                    'Оренда'] and announcement['buttons']['typeEstate'] != ['Комерційна Нерухомість'] and \
                        announcement['buttons']['section'] != ['Здати в оренду']:
                    continue

            # if 'newBuilding' in filter['buttons']:
            #     if 'Будинок зданий' in filter['buttons']['newBuilding'] and

            if 'buildingFloor' in filter['input']:
                floors = [int(i) for i in filter['input']['buildingFloor']]
                desired_floors = [int(i) for i in filter['input']['desiredFloor']]
                if filter['buttons']['floorCount'] == 'Окрім п’ятиповерхових будинків' and \
                        announcement['input']['areaFloorInHouse'][0] == 5:
                    continue
                if announcement['input']['areaFloor'] not in range(floors[0], floors[1]):
                    continue
                if announcement['input']['areaFloor'] not in range(desired_floors[0], desired_floors[1]):
                    continue

                if 'Не останій' in filter['buttons']['floor']:
                    if announcement['input']['areaFloor'][0] == announcement['input']['areaFloorInHouse'][0]:
                        continue

                if 'Не перший' in filter['buttons']['floor']:
                    if announcement['input']['areaFloor'][0] == 1:
                        continue

                if 'Не перший і не останій' in filter['buttons']['floor']:
                    if announcement['input']['areaFloor'][0] == 1 and announcement['input']['areaFloor'][0] == \
                            announcement['input']['areaFloorInHouse'][0]:
                        continue

                if 'Тільки останій' in filter['buttons']['floor']:
                    if announcement['input']['areaFloor'][0] != announcement['input']['areaFloorInHouse'][0]:
                        continue

            if 'totalArea' in filter['input']:
                areas = [int(i) for i in filter['input']['totalArea']]
                if announcement['input']['areaTotal'] not in range(areas[0], areas[1]):
                    continue

            if 'cost' in filter['input']:
                costs = [int(i) for i in filter['input']['cost']]
                if announcement['input']['cost'] not in range(costs[0], costs[1]):
                    continue

            if filter['GEO']['streets'] != []:
                street = announcement['GEO']['googleAdress'][1]['long_name'].split(' ')
                street.pop(0)
                street = " ".join(street)
                if street not in filter['GEO']['streets']:
                    continue
                if announcement['GEO']['streets'] not in filter['GEO']['streets']:
                    continue
                if announcement['GEO']['complex'] not in filter['GEO']['streets']:
                    continue

            if filter['GEO']['metroStation'] != []:
                if announcement['GEO']['metroStation'] not in filter['GEO']['metroStation']:
                    continue

            if 'role' in filter['buttons']:
                if announcement['buttons']['role'] != filter['buttons']['role']:
                    continue

            if 'newBuilding' in filter['buttons']:
                if announcement['buttons']['newBuilding'].sort() != filter['buttons']['newBuilding'].sort():
                    continue

            if 'numbRooms' in filter['buttons']:
                if announcement['buttons']['numbRooms'][0] not in filter['buttons']['numbRooms']:
                    continue

            if 'comission' in announcement['input'] and 'Без комісії для покупця' not in filter['buttons']['role']:
                if announcement['input']['comission'] == False:
                    continue

            if filter['GEO']['metroTime'] != []:
                with open("metro_coordinates.json", encoding='utf-8') as metro_stations_data:
                    metro_coordinates = metro_stations_data.read()
                object_location = (announcement['GEO']['googleAdress'][-1]['googleCoordinates']['longitude'],
                                   announcement['GEO']['googleAdress'][-1]['googleCoordinates']['latitude'])
                metro_accepted = []
                for metro_stations in metro_coordinates[announcement['GEO']['currentCity']]:
                    metro_location = (metro_stations[announcement['GEO']['metroStation'][0]][0],
                                      metro_stations[announcement['GEO']['metroStation'][0]][1])
                    if hs.haversine(object_location, metro_location) in range(filter['GEO']['metroTime'][0],
                                                                              filter['GEO']['metroTime'][1]):
                        metro_accepted.append([station_name for station_name in metro_stations][0])
                if announcement['GEO']['metroStation'][0] not in metro_accepted:
                    continue

            if filter['GEO']['range'] != {}:
                center_coordinates = [coords for coords in filter['GEO']['range']][0]
                center = (center_coordinates.split(',')[1], center_coordinates.split(',')[0])
                if hs.haversine(center, (announcement['GEO']['googleAdress'][-1]['googleCoordinates']['longitude'],
                                         announcement['GEO']['googleAdress'][-1]['googleCoordinates']['latitude'])) > [
                    radius for key, radius in filter['GEO']['range'][0]]:
                    continue

            if 'floorsHouse' in filter['input'] or 'floorCommercial' in filter['input']:
                if announcement['input']['areaFloor'][0] not in range(filter['input']['floorsHouse'][0],
                                                                      filter['input']['floorsHouse'][1]) or \
                        announcement['input']['areaFloor'][0] not in range(
                    filter['input']['floorCommercial'][0], filter['input']['floorCommercial'][1]):
                    continue
            accepted_announcements.append(announcement)
        return accepted_announcements


async def filters(doc, long, lat, floor, area, price, city_name, role, option, street, metro, room,
            new_building, commission, land_area, landmark, city):
    global current_time
    if current_time == '':
        current_time = dt.datetime.now() - dt.timedelta(seconds=30)
        current_time = current_time.strftime("%H:%M:%S %Y-%m-%d")
    temp_time = doc['datatime'][0] + " " + doc['datatime'][1]
    if temp_time >= current_time:
        if 'currentCity' in doc['GEO']:
            if doc['GEO']['currentCity'] != city:
                return False
        if 'typeEstate' in doc['buttons']:
            if 'Усі Варіанти' in doc['buttons']['typeHouse']:
                if doc['buttons']['typeEstate'] == ['Квартира'] and doc['buttons']['section'] == [
                    'Оренда'] and (option != 'flats-rent/' and option != 'flats-rent/newhouse/'):
                    return False
                if doc['buttons']['typeEstate'] == ['Квартира'] and doc['buttons']['section'] == [
                    'Продаж'] and (option != 'flats-sale/' and option != 'flats-sale/newhouse/'):
                    return False

            if 'Вторинна' in doc['buttons']['typeHouse']:
                if doc['buttons']['typeEstate'] == ['Квартира'] and doc['buttons']['section'] == [
                    'Оренда'] and option != 'flats-rent/':
                    return False
                if doc['buttons']['typeEstate'] == ['Квартира'] and doc['buttons']['section'] == [
                    'Продаж'] and option != 'flats-sale/':
                    return False
            if 'Новобудова' in doc['buttons']['typeHouse']:
                if doc['buttons']['typeEstate'] == ['Квартира'] and doc['buttons']['section'] == ['Оренда'] and \
                        doc['buttons']['typeHouse'] == ['Новобудова'] and option != 'flats-rent/newhouse/':
                    return False
                else:
                    if new_building == {}:
                        return False
                if doc['buttons']['typeEstate'] == ['Квартира'] and doc['buttons']['section'] == ['Продаж'] and \
                        doc['buttons']['typeHouse'] == ['Новобудова'] and option != 'flats-sale/newhouse/':
                    return False
                else:
                    if new_building == {}:
                        return False
            if doc['buttons']['typeEstate'] == ['Будинок'] and doc['buttons']['section'] == [
                'Продаж'] and option != 'houses-sale/':
                return False

            if doc['buttons']['typeEstate'] == ['Будинок'] and doc['buttons']['section'] == [
                'Оренда'] and option != 'houses-rent/':
                return False

            if 'landArea' in doc['input']:
                if land_area not in range(doc['input']['landArea'][0], doc['input']['landArea'][1]):
                    return False

            if doc['buttons']['typeEstate'] == ['Земельна Ділянка'] and doc['buttons']['section'] == [
                'Продаж'] and option != 'areas-sale/':
                return False

            if doc['buttons']['typeEstate'] == ['Комерційна Нерухомість'] and doc['buttons']['section'] == [
                'Продаж'] and option != 'commercials-sale/':
                return False

            if doc['buttons']['typeEstate'] == ['Комерційна Нерухомість'] and doc['buttons']['section'] == [
                'Оренда'] and option != 'commercials-rent/':
                return False

        if 'buildingFloor' in doc['input']:
            floor = re.findall("\d+", floor)
            floors = [int(i) for i in doc['input']['buildingFloor']]
            desired_floors = [int(i) for i in doc['input']['desiredFloor']]
            if doc['buttons']['floorCount'] == 'Окрім п’ятиповерхових будинків' and floor[1] == 5:
                return False
            if int(floor[1]) not in range(floors[0], floors[1]):
                return False
            if int(floor[0]) not in range(desired_floors[0], desired_floors[1]):
                return False

            if 'Не останій' in doc['buttons']['floor']:
                if int(floor[0]) == floors[1]:
                    return False
            if 'Не перший' in doc['buttons']['floor']:
                if int(floor[0]) == 1:
                    return False
            if 'Не перший і не останій' in doc['buttons']['floor']:
                if int(floor[0]) == 1 and int(floor[0]) == floors[1]:
                    return False
            if 'Тільки останій' in doc['buttons']['floor']:
                if int(floor[0]) != floors[1]:
                    return False

        if 'totalArea' in doc['input']:
            area = re.findall("\d+", area)
            area = [int(i) for i in area]
            areas = [int(i) for i in doc['input']['totalArea']]
            if sum(area) not in range(areas[0], areas[1]):
                return False

        # if doc['buttons']['section'] == ['Продаж'] and (option != 'commercials-sale/' or option != 'flats-sale/' or option != 'flats-sale/newhouse' or option != 'houses-sale/' or option != 'areas-sale/'):
        #     return False
        # if doc['buttons']['section'] == ['Оренда'] and (option != 'flats-rent/' or option != 'flats-rent/newhouse/' or option != 'houses-rent/' or option != 'commercials-rent/'):
        #     return False

        # print(doc['input']['cost'])
        if 'cost' in doc['input'] and 'typeCurrency' in doc['buttons']:
            if doc['buttons']['section'] == ['Оренда'] and (
                    option == 'flats-rent/' or option == 'flats-rent/newhouse/'):
                currency = price.split(' ')
                print(currency[-1])
                # if currency[-1] == '$' and doc['buttons']['typeCurrency'] == 'USD':
                #     price = re.findall("\d+", price)
                #     price = int(''.join(price))
                #     prices = [int(i) for i in doc['input']['cost']]
                #     if price not in range(prices[0], prices[1]):
                #         return False
                # else:
                if currency[-1] == '$/міс':
                    price = re.findall("\d+", price)
                    price = int(''.join(price))

                    price = await ConvertUSDToUAH(int(price))
                else:
                    price = re.findall("\d+", price)
                    price = int(''.join(price))
                prices = [int(i) for i in doc['input']['cost']]
                if price not in range(prices[0], prices[1]):
                    return False
            else:
                currency = price.strip(' ')
                if currency[-1] == '$' and doc['buttons']['typeCurrency'] == 'USD':
                    price = re.findall("\d+", price)
                    price = int(''.join(price))
                    prices = [int(i) for i in doc['input']['cost']]
                    if price not in range(prices[0], prices[1]):
                        return False
                else:
                    price = re.findall("\d+", price)
                    price = int(''.join(price))
                    prices = [int(i) for i in doc['input']['cost']]
                    if price not in range(prices[0], prices[1]):
                        return False

        if doc['GEO']['streets'] != []:
            if street not in doc['GEO']['streets']:
                return False
            elif new_building not in doc['GEO']['streets']:
                return False
            elif landmark not in doc['GEO']['streets']:
                return False

        if metro not in doc['GEO']['metroStation'] and doc['GEO']['metroStation'] != []:
            return False

        if 'role' in doc['buttons']:
            if role == 'Власник':
                if role not in doc['buttons']['role']:
                    return False
            elif 'Ріелтор' not in doc['buttons']['role'] and role != 'Власник':
                return False
            if 'Без комісії для покупця' in doc['buttons']['role'] and commission != 'БЕЗ КОМІСІЇ':
                return False

        if 'numbRooms' in doc['buttons']:
            if doc['buttons']['numbRooms'] != []:
                room = re.findall("\d+", room)[0]
                if room not in doc['buttons']['numbRooms']:
                    return False
                elif doc['buttons']['numbRooms'] == '5+':
                    if int(room) < 5:
                        return False

        if doc['GEO']['polygon'] != {}:
            polygons = doc['GEO']['polygon'][list(doc['GEO']['polygon'].keys())]
            wrong_polygon = 0
            for key, coords in polygons:
                coords_keys = list(coords.keys())
                coords_keys.sort()
                coords = {i: coords[i] for i in coords_keys}
                lats_vect = []
                longs_vect = []
                for coord in coords.values():
                    longs_vect.append(coord[0])
                    lats_vect.append(coord[1])
                longs_lats_vect = np.column_stack((longs_vect, lats_vect))
                polygon = Polygon(longs_lats_vect)
                point = Point(long, lat)
                if not polygon.contains(point):
                    wrong_polygon += 1
            if wrong_polygon > 0:
                return False

        if doc['GEO']['metroTime'] != []:
            with open("metro_coordinates.json", encoding='utf-8') as metro_stations_data:
                metro_coordinates = metro_stations_data.read()
            object_location = (long, lat)
            metro_accepted = []
            for metro_stations in metro_coordinates[city_name]:
                metro_location = (metro_stations[metro][0], metro_stations[metro][1])
                if hs.haversine(object_location, metro_location) in range(doc['GEO']['metroTime'][0],
                                                                          doc['GEO']['metroTime'][1]):
                    metro_accepted.append([station_name for station_name in metro_stations][0])
            if metro not in metro_accepted:
                return False

        if doc['GEO']['range'] != {}:
            center_coordinates = [coords for coords in doc['GEO']['range']][0]
            center = (center_coordinates.split(',')[1], center_coordinates.split(',')[0])
            if hs.haversine(center, (long, lat)) > [radius for key, radius in doc['GEO']['range'][0]]:
                return False

        if 'floorsHouse' in doc['input'] or 'floorCommercial' in doc['input']:
            if floor not in range(doc['input']['floorsHouse'][0],
                                  doc['input']['floorsHouse'][1]) or floor not in range(
                doc['input']['floorCommercial'][0], doc['input']['floorCommercial'][1]):
                return False
        return True


async def ConvertUSDToUAH(amount: int = 0) -> float:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.exchangerate-api.com/v4/latest/USD") as resp:
                payload = await resp.json()
                rate = payload["rates"]["UAH"]
                return amount * rate
    except Exception as e:
        print(f"[ConvertUSDToUAH] {e}")
        return None