import json
import logging
import re
import time

import numpy as np
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
import sqlalchemy as db
from sqlalchemy import select
from sqlalchemy.sql.expression import exists
from aiogram.utils.callback_data import CallbackData
from aiogram.utils import markdown

from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
import haversine as hs
import datetime as dt

import firebase_admin
from firebase_admin import credentials, firestore

TOKEN = "6247426236:AAEQKdagFgu6Xe8f9L_Yb_cPWmFvuP8DJsA"

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

engine = db.create_engine("sqlite:///from_rieltor1.db")
connection = engine.connect()
metadata = db.MetaData()
current_row = ()
temp = 1
cb_inline = CallbackData("post", "action", "data")
media_id = {}
not_checked = 0
current_time = ''


def open_rieltor_data():
    global current_row, temp
    rieltor_table = db.Table("rieltor_data", metadata, autoload_with=engine)
    select_query = db.select(rieltor_table)
    selection_result = connection.execute(select_query)
    current_row = selection_result.fetchone()
    temp = 1


@dp.message_handler(commands=['start'])
async def command_start(message: types.Message):
    start = InlineKeyboardButton(text="✅Пуск", callback_data="start")
    mar = InlineKeyboardMarkup().add(start)
    control_table = db.Table('control_data', metadata, autoload_with=engine)
    selection_query = select(control_table).where(control_table.c.user_id == message.from_user.id)
    selection_query = exists(selection_query).select()
    selection_result = connection.execute(selection_query)
    if not selection_result.fetchone()[0]:
        if str(message.text[7:]) != "":
            insertion_query = db.insert(control_table).values(user_id=message.from_user.id,
                                                              referral=str(message.text[7:]))
            connection.execute(insertion_query)
            connection.commit()
        else:
            insertion_query = db.insert(control_table).values(user_id=message.from_user.id, referral="None")
            connection.execute(insertion_query)
            connection.commit()

    await bot.send_message(message.from_user.id, "🏡 Вітаю! Я — єБАЗА нерухомості бот.\n\n"
                                                 "👋🏻 <b>З моєю допомогою ти зможеш:</b>\n\n"
                                                 "▫️ знаходити варіанти квартир, будинків за параметрами чи по карті;\n"
                                                 "▫️ знаходити покупців чи орендарів на свою нерухомість;\n"
                                                 "▫️ швидко та оперативно отримувати інформацію про нові об'єкти;\n"
                                                 "▫️ одночасно можеш додати до трьох оголошень в кожну рубрику,"
                                                 " а якщо буде потрібно більше оголошень — ділись посиланням на бот "
                                                 "з друзями і отримуй додаткові оголошення.", parse_mode='HTML')
    time.sleep(2)
    await bot.send_message(message.from_user.id, "❗️ Будь-ласка, дотримуйся правил!\n"
                                                 "Заборонено розміщувати “фейкові” оголошення.\n"
                                                 "Якщо продаж/оренда твого об'єкта вже неактуальна — не забудь "
                                                 "відправити в архів.\n"
                                                 "Якщо ти ріелтор — розміщуй лише ті оголошення, де в тебе є "
                                                 "договір з власником.\n"
                                                 "За порушення правил — можливий бан!")
    time.sleep(2)
    await bot.send_message(message.from_user.id, "Почнемо зараз?", reply_markup=mar)


@dp.callback_query_handler(text="start")
async def start(callback_query: types.CallbackQuery):
    control_table = db.Table("control_data", metadata, autoload_with=engine)
    selection_query = select(control_table).where(control_table.c.user_id == callback_query.message.from_user.id)
    selection_result = connection.execute(selection_query)
    search = InlineKeyboardButton(text="Пошук", callback_data="search")
    sell = InlineKeyboardButton(text="Додати оголошення", callback_data="sell")
    if len(selection_result.fetchall()) != 0:
        favorite = InlineKeyboardButton(text=f"Обране({len(selection_result.fetchall())})", callback_data="favorite")
    else:
        favorite = InlineKeyboardButton(text=f"Обране", callback_data="favorite")
    my_message = InlineKeyboardButton(text="Мої повідомлення", callback_data="my_message")
    my_ann = InlineKeyboardButton(text="Мої оголошення", callback_data="announcement")
    share = InlineKeyboardButton(text="Розповісти про бота", callback_data="share")
    help = InlineKeyboardButton(text="Звернутися в підтримку", callback_data="help")
    mar = InlineKeyboardMarkup(row_width=2).add(search, sell, favorite, my_message, my_ann, share, help)
    await bot.edit_message_text(chat_id=callback_query.from_user.id, message_id=callback_query.message.message_id,
                                text="Оберіть, що ви хочете зробити?", reply_markup=mar)


@dp.callback_query_handler(text='search')
async def search_menu(callback_query: types.CallbackQuery):
    control_table = db.Table("control_data", metadata, autoload_with=engine)
    selection_query = select(control_table).where(control_table.c.user_id == callback_query.message.from_user.id)
    selection_result = connection.execute(selection_query)
    print(callback_query.from_user.id)
    search_by_params = KeyboardButton(text="Пошук за параметрами",
                                      web_app=WebAppInfo(
                                          url=f"https://testwebform142125.000webhostapp.com/FormFirst/idUser/{callback_query.from_user.id}"))
    # url=f"https://testwebform142125.000webhostapp.com/FormFirst/idUser/{callback_query.from_user.id}"))  , callback_data="search_by_params"
    if len(selection_result.fetchall()) != 0:
        favorite = InlineKeyboardButton(text=f"Обране({len(selection_result.fetchall())})", callback_data="favorite")
    else:
        favorite = InlineKeyboardButton(text=f"Обране", callback_data="favorite")
    my_message = InlineKeyboardButton(text="Мої повідомлення", callback_data="my_message")
    my_ann = InlineKeyboardButton(text="Мої оголошення", callback_data="announcement")
    stop_search = InlineKeyboardButton(text="Зупинити пошук", callback_data="stop_search")
    if not_checked != 0:
        show_not_checked = InlineKeyboardButton(text=f"Показати не переглянуте({not_checked})",
                                                callback_data="show_not_checked")
    else:
        show_not_checked = InlineKeyboardButton(text=f"Показати не переглянуте",
                                                callback_data="show_not_checked")
    mar = InlineKeyboardMarkup(resize_keyboard=True, row_width=2).add(favorite, my_message, my_ann, stop_search,
                                                                      show_not_checked)
    mar1 = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True).add(search_by_params)
    await bot.edit_message_text(chat_id=callback_query.from_user.id, message_id=callback_query.message.message_id,
                                text="Ваш вибір:", reply_markup=mar)
    await bot.send_message(callback_query.from_user.id, 'Кнопка для переходу до форми вибору фільтрів',
                           reply_markup=mar1)


@dp.callback_query_handler(text='announcement')
async def announcement_menu(callback_query: types.CallbackQuery):
    sell = InlineKeyboardButton(text="Продам", callback_data="for_ann")
    rent_out = InlineKeyboardButton(text="Оренда", callback_data="for_ann")
    purchase = InlineKeyboardButton(text="Куплю", callback_data="for_ann")
    rent_in = InlineKeyboardButton(text="Зніму", callback_data="for_ann")
    mar = InlineKeyboardMarkup(row_width=2).add(sell, rent_out, purchase, rent_in)
    await bot.edit_message_text(chat_id=callback_query.from_user.id, message_id=callback_query.message.message_id,
                                text="Мої оголошення", reply_markup=mar)


@dp.callback_query_handler(text='for_seller_ann')
async def for_seller_ann(message: types.Message):
    # here must be data for card
    details = InlineKeyboardButton(text="Деталі", callback_data="details")
    edit = InlineKeyboardButton(text="Редагувати", callback_data="edit")
    get_buyers = InlineKeyboardButton(text="Підібрати покупців/орендарів", callback_data="get_buyers")
    phone_num = InlineKeyboardButton(text="Показати номер телефону", callback_data="phone_num")
    search = InlineKeyboardButton(text="Пошук", callback_data="search")
    share = InlineKeyboardButton(text="Розповісти про бота", callback_data="share")
    mar = InlineKeyboardMarkup(row_width=3).add(details, edit, get_buyers, phone_num, search, share)


@dp.callback_query_handler(text="edit")
async def update(callback_query: types.CallbackQuery):
    actualize = InlineKeyboardButton(text="Актуалізувати", callback_data="actualize")
    update = InlineKeyboardButton(text="Редагувати", callback_data="update")
    delete = InlineKeyboardButton(text="Видалити", callback_data="delete")
    back = InlineKeyboardButton(text="Назад🔙", callback_data="back")
    mar = InlineKeyboardMarkup(row_width=2).add(actualize, update, delete, back)
    await bot.edit_message_text(chat_id=callback_query.from_user.id, message_id=callback_query.message.message_id,
                                text="Редагувати", reply_markup=mar)


@dp.callback_query_handler(text='for_buyer_ann')
async def for_buyer_ann(message: types.Message):
    # here must be data for card
    details = InlineKeyboardButton(text="Деталі", callback_data="details")
    edit = InlineKeyboardButton(text="Редагувати", callback_data="edit")
    get_realty = InlineKeyboardButton(text="Підібрати нерухомість", callback_data="get_realty")
    phone_num = InlineKeyboardButton(text="Показати номер телефону", callback_data="phone_num")
    search = InlineKeyboardButton(text="Пошук", callback_data="search")
    share = InlineKeyboardButton(text="Розповісти про бота", callback_data="share")
    mar = InlineKeyboardMarkup(row_width=3).add(details, edit, get_realty, phone_num, search, share)


@dp.callback_query_handler(text='sell')
async def sell(message: types.Message):
    await bot.send_message(message.from_user.id, "sell page is loading...")


def on_snapshot(col_snapshot, changes, read_time):
    for change in changes:
        doc_data = change.document.to_dict()
        doc_id = change.document.id
        if change.type.name == 'MODIFIED':
            print(f'{change.document.id}')
            print(change.document.to_dict())


def check_id(user_id):
    fire_base = firestore.client()

    collection_ref = fire_base.collection('WebFormOne')
    docs = collection_ref.stream()

    collection_watch = collection_ref.on_snapshot(on_snapshot)
    print(collection_watch)

    for doc in docs:
        if str(user_id) == str(doc.id):
            return doc.to_dict()


def rent_flat_info_check(doc, long, lat, floor, area, price, city_name, role, option, street, metro, room,
                         new_building, commission, land_area, landmark):
    global current_time
    if current_time == '':
        current_time = dt.datetime.now() - dt.timedelta(seconds=30)
        current_time = current_time.strftime("%H:%M:%S %Y-%m-%d")
    false_count = 0
    temp_time = doc['datatime'][0] + " " + doc['datatime'][1]
    if temp_time >= current_time:
        if city_name == doc['GEO']['currentCity']:
            # flats
            if (doc['buttons']['typeEstate'] == ['Квартира'] and doc['buttons']['section'] == [
                'Оренда'] and option == 'flats-rent/') or (
                    doc['buttons']['typeEstate'] == ['Квартира'] and doc['buttons']['section'] == [
                'Продаж'] and option == 'flats-sale/'):
                if (doc['buttons']['typeEstate'] == ['Квартира'] and doc['buttons']['section'] == ['Оренда'] and
                    doc['buttons']['typeHouse'] == ['Новобудова'] and option == 'flats-rent/newhouse') or (
                        doc['buttons']['typeEstate'] == ['Квартира'] and doc['buttons']['section'] == ['Продаж'] and
                        doc['buttons']['typeHouse'] == ['Новобудова'] and option == 'flats-sale/newhouse'):
                    if new_building == {}:
                        false_count += 1
                if 'buildingFloor' in doc['input']:
                    floor = re.findall("\d+", floor)
                    floors = [int(i) for i in doc['input']['buildingFloor']]
                    desired_floors = [int(i) for i in doc['input']['desiredFloor']]
                    if doc['buttons']['floorCount'] == 'Окрім п’ятиповерхових будинків' and floor[1] != 5:
                        if int(floor[1]) in range(floors[0], floors[1]):
                            if int(floor[0]) not in range(desired_floors[0], desired_floors[1]):
                                false_count += 1
                        else:
                            false_count += 1
                    elif int(floor[1]) in range(floors[0], floors[1]):
                        if int(floor[0]) in range(desired_floors[0], desired_floors[1]):
                            false_count += 1
                    elif 'Не останій' in doc['buttons']['floor']:
                        if int(floor[0]) == floors[1]:
                            false_count += 1
                    elif 'Не перший' in doc['buttons']['floor']:
                        if int(floor[0]) == 1:
                            false_count += 1
                    elif 'Не перший і не останій' in doc['buttons']['floor']:
                        if int(floor[0]) == 1 and int(floor[0]) == floors[1]:
                            false_count += 1
                    elif 'Тільки останій' in doc['buttons']['floor']:
                        if int(floor[0]) != floors[1]:
                            false_count += 1
                    else:
                        false_count += 1
                if 'totalArea' in doc['input']:
                    area = re.findall("\d+", area)
                    area = [int(i) for i in area]
                    areas = [int(i) for i in doc['input']['totalArea']]
                    if sum(area) not in range(areas[0], areas[1]):
                        false_count += 1

                if 'cost' in doc['input']:
                    if doc['buttons']['section'] == ['Оренда'] and (option == 'flats-rent/' or option == 'flats-rent/newhouse'):
                        price = re.findall("\d+", price)
                        price = int(''.join(price))
                        prices = [int(i) for i in doc['input']['cost']]
                        if price not in range(prices[0], prices[1]):
                            false_count += 1
                    else:
                        price = re.findall("\d+", price)
                        price = int(''.join(price))
                        prices = [int(i) for i in doc['input']['cost']]
                        if price not in range(prices[0], prices[1]):
                            false_count += 1

                if doc['GEO']['streets'] != []:
                    if street not in doc['GEO']['streets']:
                        false_count += 1
                    elif new_building not in doc['GEO']['streets']:
                        false_count += 1
                    elif landmark not in doc['GEO']['streets']:
                        false_count += 1

                if metro not in doc['GEO']['metroStation'] and doc['GEO']['metroStation'] != []:
                    false_count += 1

                if role == 'Власник':
                    if role not in doc['buttons']['role']:
                        false_count += 1
                elif 'Ріелтор' not in doc['buttons']['role'] and role != 'Власник':
                    false_count += 1

                if 'Без комісії для покупця' not in doc['buttons']['role'] and role == 'БЕЗ КОМІСІЇ':
                    false_count += 1

                if doc['buttons']['numbRooms'] != []:
                    room = re.findall("\d+", room)[0]
                    if room not in doc['buttons']['numbRooms']:
                        false_count += 1
                    elif doc['buttons']['numbRooms'] == '5+':
                        if int(room) < 5:
                            false_count += 1

                if doc['GEO']['polygon'] != {}:
                    coords = doc['GEO']['polygon'][list(doc['GEO']['polygon'].keys())[0]]
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
                        false_count += 1

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
                        false_count += 1

                if doc['GEO']['range'] != {}:
                    center_coordinates = [coords for coords in doc['GEO']['range']][0]
                    center = (center_coordinates.split(',')[1], center_coordinates.split(',')[0])
                    if hs.haversine(center, (long, lat)) > [radius for key, radius in doc['GEO']['range'][0]]:
                        false_count += 1

            # houses
            elif (doc['buttons']['typeEstate'] == ['Будинок'] and doc['buttons']['section'] == [
                'Продаж'] and option == 'houses-sale/') or (
                    doc['buttons']['typeEstate'] == ['Будинок'] and doc['buttons']['section'] == [
                'Оренда'] and option == 'houses-rent/'):
                if doc['buttons']['numbRooms'] != []:
                    if room not in doc['buttons']['numbRooms']:
                        false_count += 1
                    elif doc['buttons']['numbRooms'] == '5+':
                        if int(room) < 5:
                            false_count += 1

                price = re.findall("\d+", price)
                price = int(''.join(price))
                prices = [int(i) for i in doc['input']['cost']]
                if price not in range(prices[0], prices[1]):
                    false_count += 1

                if doc['GEO']['polygon'] != {}:
                    coords = doc['GEO']['polygon'][list(doc['GEO']['polygon'].keys())[0]]
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
                        false_count += 1

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
                        false_count += 1

                if doc['GEO']['range'] != {}:
                    center_coordinates = [coords for coords in doc['GEO']['range']][0]
                    center = (center_coordinates.split(',')[1], center_coordinates.split(',')[0])
                    if hs.haversine(center, (long, lat)) > [radius for key, radius in doc['GEO']['range'][0]]:
                        false_count += 1

                if street not in doc['GEO']['streets'] and doc['GEO']['streets'] != []:
                    false_count += 1

                if metro not in doc['GEO']['metroStation'] and doc['GEO']['metroStation'] != []:
                    false_count += 1

                area = re.findall("\d+", area)
                area = [int(i) for i in area]
                areas = [int(i) for i in doc['input']['totalArea']]
                if sum(area) not in range(areas[0], areas[1]):
                    false_count += 1

                if role == 'Власник':
                    if role not in doc['buttons']['role']:
                        false_count += 1
                elif 'Ріелтор' not in doc['buttons']['role'] and role != 'Власник':
                    false_count += 1

                if 'Без комісії для покупця' not in doc['buttons']['role'] and commission == 'БЕЗ КОМІСІЇ':
                    false_count += 1

                if land_area not in range(doc['input']['landArea'][0], doc['input']['landArea'][1]):
                    false_count += 1

            # areas
            elif doc['buttons']['typeEstate'] == ['Земельна Ділянка'] and doc['buttons']['section'] == [
                'Продаж'] and option == 'areas-sale/':
                if doc['GEO']['polygon'] != {}:
                    coords = doc['GEO']['polygon'][list(doc['GEO']['polygon'].keys())[0]]
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
                        false_count += 1

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
                        false_count += 1

                if doc['GEO']['range'] != {}:
                    center_coordinates = [coords for coords in doc['GEO']['range']][0]
                    center = (center_coordinates.split(',')[1], center_coordinates.split(',')[0])
                    if hs.haversine(center, (long, lat)) > [radius for key, radius in doc['GEO']['range'][0]]:
                        false_count += 1

                if street not in doc['GEO']['streets'] and doc['GEO']['streets'] != []:
                    false_count += 1

                if metro not in doc['GEO']['metroStation'] and doc['GEO']['metroStation'] != []:
                    false_count += 1

                if land_area not in range(doc['input']['landArea'][0], doc['input']['landArea'][1]):
                    false_count += 1

                price = re.findall("\d+", price)
                price = int(''.join(price))
                prices = [int(i) for i in doc['input']['cost']]
                if price not in range(prices[0], prices[1]):
                    false_count += 1

                if role == 'Власник':
                    if role not in doc['buttons']['role']:
                        false_count += 1
                elif 'Ріелтор' not in doc['buttons']['role'] and role != 'Власник':
                    false_count += 1

                if 'Без комісії для покупця' not in doc['buttons']['role'] and commission == 'БЕЗ КОМІСІЇ':
                    false_count += 1

            # commercial
            elif (doc['buttons']['typeEstate'] == ['Комерційна Нерухомість'] and doc['buttons']['section'] == [
                'Продаж'] and option == 'commercials-sale/') or (
                    doc['buttons']['typeEstate'] == ['Комерційна Нерухомість'] and doc['buttons']['section'] == [
                'Оренда'] and option == 'commercials-rent/'):
                if doc['GEO']['polygon'] != {}:
                    coords = doc['GEO']['polygon'][list(doc['GEO']['polygon'].keys())[0]]
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
                        false_count += 1

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
                        false_count += 1

                if doc['GEO']['range'] != {}:
                    center_coordinates = [coords for coords in doc['GEO']['range']][0]
                    center = (center_coordinates.split(',')[1], center_coordinates.split(',')[0])
                    if hs.haversine(center, (long, lat)) > [radius for key, radius in doc['GEO']['range'][0]]:
                        false_count += 1

                if street not in doc['GEO']['streets'] and doc['GEO']['streets'] != []:
                    false_count += 1

                if metro not in doc['GEO']['metroStation'] and doc['GEO']['metroStation'] != []:
                    false_count += 1

                price = re.findall("\d+", price)
                price = int(''.join(price))
                prices = [int(i) for i in doc['input']['cost']]
                if price not in range(prices[0], prices[1]):
                    false_count += 1

                if role == 'Власник':
                    if role not in doc['buttons']['role']:
                        false_count += 1
                elif 'Ріелтор' not in doc['buttons']['role'] and role != 'Власник':
                    false_count += 1

                if 'Без комісії для покупця' not in doc['buttons']['role'] and commission == 'БЕЗ КОМІСІЇ':
                    false_count += 1

                area = re.findall("\d+", area)
                area = [int(i) for i in area]
                areas = [int(i) for i in doc['input']['totalArea']]
                if sum(area) not in range(areas[0], areas[1]):
                    false_count += 1

                if floor not in range(doc['input']['floorsHouse'][0],
                                      doc['input']['floorsHouse'][1] or floor not in range(
                                          doc['input']['floorCommercial'][0], doc['input']['floorCommercial'][1])):
                    false_count += 1
            else:
                false_count += 1
        else:
            false_count += 1
        if false_count > 0:
            return False
        else:
            return True

    # get objects from database and sort them using filter from web-form, then add this data to list and send to method
    # of handling objects and bot sending


@dp.callback_query_handler(text='show_not_checked')
@dp.callback_query_handler(text='more')
@dp.message_handler(content_types=['web_app_data'])
async def web_app(message: types.Message):
    global current_row, temp, not_checked
    rieltor_table = db.Table("rieltor_data", metadata, autoload_with=engine)
    select_query = db.select(rieltor_table)
    selection_result = connection.execute(select_query)
    doc = check_id(message.from_user.id)

    details = InlineKeyboardButton(text="Детальніше", callback_data="details")
    error = InlineKeyboardButton(text="Помилка/Поскаржитись", callback_data="error")
    phone_num = InlineKeyboardButton(text="Показати номер телефону", callback_data="phone_num")
    change = InlineKeyboardButton(text="Змінити пошук", callback_data="change")
    stop = InlineKeyboardButton(text="Зупинити пошук", callback_data="stop")
    share = InlineKeyboardButton(text="Розповісти про бот", callback_data="share")
    more = InlineKeyboardButton(text="Показати ще", callback_data="more")
    mar = InlineKeyboardMarkup(row_width=2).add(details, error, phone_num, change, stop, share, more)
    breaking = False
    rows = selection_result.fetchall()

    for check_row in rows:
        if check_row == current_row:
            for row_num in range(int(current_row[0]) - 1, len(rows)):
                row = rows[row_num]
                not_checked = len(rows) - temp
                current_row = row

                images = json.loads(row[-6])
                media = types.MediaGroup()
                count = 0
                markers = json.loads(row[-8])
                metro = ''
                new_building = ''
                landmark = ''
                commission = ''
                if 'metro' in markers:
                    metro = markers['metro']
                if 'newhouse' in markers:
                    new_building = markers['newhouse']
                if 'landmark' in markers:
                    landmark = markers['landmark']
                if 'commission' in markers:
                    commission = markers['commission']
                if rent_flat_info_check(doc=doc, long=row[-5], lat=row[-4], floor=row[7],
                                        area=row[8], price=row[5], city_name=row[2], role=row[-7],
                                        option=row[-2], street=row[4], metro=metro, room=row[6],
                                        new_building=new_building, commission=commission, land_area=row[9],
                                        landmark=landmark):
                    if temp % 6 != 0:
                        for image in images:
                            if count < len(images) and count < 10:
                                media.attach_photo(types.InputMediaPhoto(image, caption=f"📌ID:{row[-3]}\n"
                                                                                        f"📍Розташування: {row[3]}\n"
                                                                                        f"📫{row[4]}\n"
                                                                                        f"🏢{row[7]}\n"
                                                                                        f"📈Площа: {row[8]}\n"
                                                                                        f"🛏{row[6]}\n"
                                                                                        f"💰Ціна:{row[5]}\n"
                                                                                        f"👥{row[-7]}\n📞{row[-1]}" if count == 0 else ''))
                            elif count == len(images) or count == 10:
                                temp += 1
                                await bot.send_media_group(message.from_user.id, media=media)
                                await bot.send_message(message.from_user.id, f'📌ID:{row[-3]} меню', reply_markup=mar)
                            elif count > len(images) or count > 10:
                                break
                            count += 1
                    else:
                        breaking = True
                        temp += 1
                        break
        elif breaking:
            break


async def card(message: types.Message):
    global current_row, temp, not_checked
    rieltor_table = db.Table("rieltor_data", metadata, autoload_with=engine)
    select_query = db.select(rieltor_table)
    selection_result = connection.execute(select_query)

    details = InlineKeyboardButton(text="Детальніше", callback_data="details")
    error = InlineKeyboardButton(text="Помилка/Поскаржитись", callback_data="error")
    phone_num = InlineKeyboardButton(text="Показати номер телефону", callback_data="phone_num")
    change = InlineKeyboardButton(text="Змінити пошук", callback_data="change")
    stop = InlineKeyboardButton(text="Зупинити пошук", callback_data="stop")
    share = InlineKeyboardButton(text="Розповісти про бот", callback_data="share")
    more = InlineKeyboardButton(text="Показати ще", callback_data="more")
    mar = InlineKeyboardMarkup(row_width=2).add(details, error, phone_num, change, stop, share, more)
    breaking = False
    rows = selection_result.fetchall()

    for check_row in rows:
        if check_row == current_row:
            for row_num in range(temp, len(rows)):
                if temp % 6 != 0:
                    row = rows[row_num]
                    not_checked = len(rows) - temp
                    current_row = row

                    images = json.loads(row[-6])
                    media = types.MediaGroup()
                    count = 0
                    markers = json.loads(row[-8])
                    metro = ''
                    new_building = ''
                    landmark = ''
                    commission = ''
                    if 'metro' in markers:
                        metro = markers['metro']
                    if 'newhouse' in markers:
                        new_building = markers['newhouse']
                    if 'landmark' in markers:
                        landmark = markers['landmark']
                    if 'commission' in markers:
                        commission = markers['commission']
                    for image in images:
                        if count < 10:
                            media.attach_photo(types.InputMediaPhoto(image, caption=f"📌ID:{row[-3]}\n"
                                                                                    f"📍Розташування: {row[3]}\n"
                                                                                    f"📫{row[4]}\n"
                                                                                    f"Ⓜ️{metro}\n" if metro != '' else ''
                                                                                                                       f"🏢{row[7]}\n"
                                                                                                                       f"📈Площа: {row[8]}\n"
                                                                                                                       f"🛏{row[6]}\n"
                                                                                                                       f"💰Ціна:{row[5]}\n"
                                                                                                                       f"👥{row[-7]}\n📞{row[-1]}" if count == 0 else ''))
                        elif count == 10:
                            temp += 1
                            await bot.send_media_group(message.from_user.id, media=media)
                            await bot.send_message(message.from_user.id, f'📌ID:{row[-3]} меню', reply_markup=mar)
                        elif count > 10:
                            break
                        count += 1
                else:
                    breaking = True
                    break
        elif breaking:
            break


@dp.callback_query_handler(text="details")
async def details_view(callback_query: types.CallbackQuery):
    fav = InlineKeyboardButton(text="Додати в обране", callback_data=cb_inline.new(action="add_fav", data=
    re.findall("\d+", callback_query.message.text)[0]))
    res_complex = InlineKeyboardButton(text="Квартири в цьому ЖК", callback_data="res_complex")
    complaints = InlineKeyboardButton(text="Скарги", callback_data="complaints")
    back = InlineKeyboardButton(text="Назад🔙", callback_data=cb_inline.new(action="back",
                                                                            data=re.findall("\d+",
                                                                                            callback_query.message.text)[
                                                                                0]))
    mar = InlineKeyboardMarkup(row_width=2).add(fav, res_complex, complaints, back)
    await bot.edit_message_text(chat_id=callback_query.from_user.id, message_id=callback_query.message.message_id,
                                text="Детальніше", reply_markup=mar)


@dp.callback_query_handler(cb_inline.filter(action='back'))
async def back(callback_query: types.CallbackQuery, callback_data):
    details = InlineKeyboardButton(text="Детальніше", callback_data="details")
    error = InlineKeyboardButton(text="Помилка/Поскаржитись", callback_data="error")
    phone_num = InlineKeyboardButton(text="Показати номер телефону", callback_data="phone_num")
    change = InlineKeyboardButton(text="Змінити пошук", callback_data="change")
    stop = InlineKeyboardButton(text="Зупинити пошук", callback_data="stop")
    share = InlineKeyboardButton(text="Розповісти про бот", callback_data="share")
    more = InlineKeyboardButton(text="Показати ще", callback_data="more")
    mar = InlineKeyboardMarkup(row_width=2).add(details, error, phone_num, change, stop, share, more)
    await bot.edit_message_text(chat_id=callback_query.from_user.id, message_id=callback_query.message.message_id,
                                text=f"📌ID:{callback_data['data']} меню", reply_markup=mar)


@dp.callback_query_handler(text="share")
async def share(callback_query: types.CallbackQuery):
    control_table = db.Table("control_data", metadata, autoload_with=engine)
    selection_query = select(control_table).where(control_table.c.referral == callback_query.from_user.id)
    selection_result = connection.execute(selection_query)
    await bot.send_message(chat_id=callback_query.from_user.id, text=f"Це твоє реферальне посилання.\n"
                                                                     f"https://t.me/eBAZA_estate_bot?start={callback_query.from_user.id}\n"
                                                                     f"Кількість рефералів: {len(selection_result.fetchall())}")


@dp.callback_query_handler(cb_inline.filter(action="add_fav"))
async def add_fav(callback_query: types.CallbackQuery, callback_data):
    control_table = db.Table('control_data', metadata, autoload_with=engine)
    insertion_query = control_table.insert().values(user_id=callback_query.from_user.id, favorite=callback_data['data'])
    connection.execute(insertion_query)
    connection.commit()
    mess = await bot.send_message(callback_query.from_user.id, f"Оголошення {callback_data['data']} додане до Обране")
    time.sleep(20)
    await bot.delete_message(callback_query.from_user.id, mess.message_id)


@dp.callback_query_handler(text='favorite')
async def show_favorite(callback_query: types.CallbackQuery):
    global media_id
    control_table = db.Table('control_data', metadata, autoload_with=engine)
    rieltor_table = db.Table('rieltor_data', metadata, autoload_with=engine)
    control_selection = select(control_table).where(control_table.c.user_id == callback_query.from_user.id)
    control_selection_result = connection.execute(control_selection)

    details = InlineKeyboardButton(text="Детальніше", callback_data="details_in_fav")
    error = InlineKeyboardButton(text="Помилка/Поскаржитись", callback_data="error")
    phone_num = InlineKeyboardButton(text="Показати номер телефону", callback_data="phone_num")
    share = InlineKeyboardButton(text="Розповісти про бот", callback_data="share")
    mar = InlineKeyboardMarkup(row_width=2).add(details, error, phone_num, share)

    for control_element in control_selection_result.fetchall():
        rieltor_selection = select(rieltor_table).where(rieltor_table.c.rieltor_id == control_element[2])
        rieltor_selection_result = connection.execute(rieltor_selection)
        row = rieltor_selection_result.fetchone()
        images = json.loads(row[8])
        markers = json.loads(row[6])
        count = 0
        media = types.MediaGroup()
        for image in images:
            if count < 10:
                media.attach_photo(types.InputMediaPhoto(image, caption=f"📌ID:{row[-3]}\n"
                                                                        f"📍Розташування: {row[1].upper()},"
                                                                        f" {' '.join(markers)}\n"
                                                                        f"📫 {row[2]}, {row[3]}\n"
                                                                        f"🏢{row[4]}\n"
                                                                        f"📈Площа: {row[5]}\n"
                                                                        f"🛏{row[3]}\n"
                                                                        f"💰Ціна:{row[2]}\n"
                                                                        f"👥{row[7]}\n📞<spoiler>{row[-1]}</spoiler>" if count == 0 else '',
                                                         parse_mode='html'))
            elif count == 10:
                media_message = await bot.send_media_group(callback_query.from_user.id, media=media)
                message_id = media_message[0]
                media_id[row[-3]] = message_id['message_id']
                await bot.send_message(callback_query.from_user.id, f'📌ID:{row[-3]} меню', reply_markup=mar)
            elif count > 10:
                break
            count += 1


@dp.callback_query_handler(text="details_in_fav")
async def details_in_fav(callback_query: types.CallbackQuery):
    fav = InlineKeyboardButton(text="Видалити з обране", callback_data=cb_inline.new(action="del_fav", data=
    re.findall("\d+", callback_query.message.text)[0]))
    res_complex = InlineKeyboardButton(text="Квартири в цьому ЖК", callback_data="res_complex")
    complaints = InlineKeyboardButton(text="Скарги", callback_data="complaints")
    back = InlineKeyboardButton(text="Назад🔙", callback_data=cb_inline.new(action="back", data=
    re.findall("\d+", callback_query.message.text)[0]))
    mar = InlineKeyboardMarkup(row_width=2).add(fav, res_complex, complaints, back)
    await bot.edit_message_text(chat_id=callback_query.from_user.id, message_id=callback_query.message.message_id,
                                text="Детальніше", reply_markup=mar)


@dp.callback_query_handler(cb_inline.filter(action="del_fav"))
async def del_fav(callback_query: types.CallbackQuery, callback_data):
    for media_key in list(media_id.keys()):
        if media_key == callback_data['data']:
            control_table = db.Table("control_data", metadata, autoload_with=engine)
            del_query = db.delete(control_table).where(control_table.c.favorite == media_key)
            connection.execute(del_query)
            connection.commit()
    mes = await bot.send_message(callback_query.from_user.id, "Оголошення видалено з Обране")
    time.sleep(10)
    await bot.delete_message(callback_query.from_user.id, mes.message_id)


@dp.callback_query_handler(text="error")
async def complaints_view(callback_query: types.CallbackQuery):
    mess1 = InlineKeyboardButton(text="Неактуально/Фейк", callback_data=cb_inline.new(action="complaint", data=[
        re.findall("\d+", callback_query.message.text)[0], "Неактуально/Фейк"]))
    mess2 = InlineKeyboardButton(text="Невідповідні фото", callback_data=cb_inline.new(action="complaint", data=[
        re.findall("\d+", callback_query.message.text)[0], "Невідповідні фото"]))
    mess3 = InlineKeyboardButton(text="Невірні (поверх,площа або ціна)",
                                 callback_data=cb_inline.new(action="complaint", data=[
                                     re.findall("\d+", callback_query.message.text)[0], "Невірний опис"]))
    mess4 = InlineKeyboardButton(text="Це мій ексклюзив", callback_data=cb_inline.new(action="complaint", data=[
        re.findall("\d+", callback_query.message.text)[0], "Це мій ексклюзив"]))
    mess5 = InlineKeyboardButton(text="Підозрілий об`єкт", callback_data=cb_inline.new(action="complaint", data=[
        re.findall("\d+", callback_query.message.text)[0], "Підозрілий об`єкт"]))
    back = InlineKeyboardButton(text="Назад🔙", callback_data=cb_inline.new(action="back", data=
    re.findall("\d+", callback_query.message.text)[0]))
    mar = InlineKeyboardMarkup(row_width=3).add(mess1, mess2, mess3, mess4, mess5, back)
    await bot.edit_message_text(chat_id=callback_query.from_user.id, message_id=callback_query.message.message_id,
                                text="Помилка/Поскаржитись", reply_markup=mar)


@dp.callback_query_handler(cb_inline.filter(action='complaint'))
async def send_complaint(callback_query: types.CallbackQuery, callback_data):
    announcement_id = re.findall(r'\d+', callback_data['data'])[0]
    complaint = re.findall(r'\w+\s*', callback_data['data'])
    complaint.pop(0)
    control_table = db.Table('control_data', metadata, autoload_with=engine)
    insertion_query = control_table.insert().values()
    await bot.send_message(callback_query.from_user.id, text=f"{announcement_id}, {' '.join(complaint)}")


def create_db_control():
    user_data = db.Table("control_data", metadata,
                         db.Column("id", db.Integer, primary_key=True),
                         db.Column("user_id", db.String),
                         db.Column("favorite", db.String),
                         db.Column("complaint", db.String),
                         db.Column("referral", db.String))
    metadata.create_all(engine)


if __name__ == "__main__":
    cred = credentials.Certificate("aleksandr-c0286-firebase-adminsdk-4k3sz-ebc5beaae1.json")
    firebase_admin.initialize_app(cred)
    open_rieltor_data()
    create_db_control()
    executor.start_polling(dp, skip_updates=True)
