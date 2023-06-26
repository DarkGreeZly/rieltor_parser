import json
import logging
import re
import time
import zlib

import requests
from domParser import start_parser

import numpy as np
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
import sqlalchemy as db
from sqlalchemy import select
from sqlalchemy.sql.expression import exists
from aiogram.utils.callback_data import CallbackData
import base64
from aiogram.utils import markdown

from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
import haversine as hs
import datetime as dt
import phonenumbers
import uuid

import firebase_admin
from firebase_admin import credentials, firestore

TOKEN = "6247426236:AAEQKdagFgu6Xe8f9L_Yb_cPWmFvuP8DJsA"

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

engine = db.create_engine("mysql+pymysql://devuser:r2d2c3po@localhost:3306/eBazaDB")
connection = engine.connect()
metadata = db.MetaData()
current_row = ()
current_num_row = 0
temp = 1
cb_inline = CallbackData("post", "action", "data")
media_id = {}
not_checked = 0
current_time = ''
count_of_coins = 0
phone_number = ''
favorites = 0
count_complaints = 0


def open_rieltor_data():
    global current_row, temp
    rieltor_table = db.Table("rieltor_data", metadata, autoload_with=engine)
    select_query = db.select(rieltor_table)
    selection_result = connection.execute(select_query)
    current_row = selection_result.fetchall()[0]
    temp = 1


@dp.message_handler(commands=['start'])
async def command_start(message: types.Message):
    control_table = db.Table('control_data', metadata, autoload_with=engine)
    selection_query = select(control_table).where(control_table.c.user_id == message.from_user.id)
    selection_query = exists(selection_query).select()
    selection_result = connection.execute(selection_query)
    # print(selection_result.fetchone()[0])
    if selection_result.fetchone()[0] == False:
        if str(message.text[7:]) != "":
            insertion_query = db.insert(control_table).values(user_id=message.from_user.id,
                                                              referral=str(message.text[7:]),
                                                              coins=30)
            connection.execute(insertion_query)
            connection.commit()
            insertion_query_referral = db.insert(control_table).values(user_id=str(message.text[7:]),
                                                                       coins=5)
            connection.execute(insertion_query_referral)
            connection.commit()
        else:
            insertion_query = db.insert(control_table).values(user_id=message.from_user.id, referral="None",
                                                              coins=30)
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
    send_num = KeyboardButton("Поділитися номером телефону", request_contact=True)
    mar = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True).add(send_num)
    await bot.send_message(message.from_user.id, "Для ефективної взаємодії потрібен ваш номер телефону",
                           reply_markup=mar)

    @dp.message_handler()
    @dp.message_handler(content_types=types.ContentType.CONTACT)
    async def user_number(message: types.Message):
        global phone_number
        continue_button = InlineKeyboardButton("Продовжити⏩", callback_data="start")
        mar = InlineKeyboardMarkup().add(continue_button)
        phone = ''
        try:
            phone = message.contact.phone_number
        except Exception:
            phone = None
            pass
        if phone is None:
            check_number = phonenumbers.parse(message.text)
            if phonenumbers.is_valid_number(check_number):
                phone_number = message.text
                await bot.send_message(message.from_user.id, "Номер затверджено✅", reply_markup=mar)
            else:
                await bot.send_message(message.from_user.id,
                                       "Невірний формат номеру, спробуйте в такому форматі - +380xxxxxxxxx")
        else:
            phone_number = message.contact.phone_number
            await bot.send_message(message.from_user.id, "Номер затверджено✅", reply_markup=mar)


@dp.message_handler(commands=['add'])
@dp.callback_query_handler(cb_inline.filter(action="start"))
@dp.callback_query_handler(text='start')
async def start(callback_query: types.CallbackQuery, command: types.BotCommand = None, callback_data=None):
    # if callback_data:
    #     await bot.delete_message(callback_query.from_user.id, callback_query.message.message_id)
    global count_of_coins, favorites, count_complaints
    control_table = db.Table("control_data", metadata, autoload_with=engine)
    selection_query = select(control_table).where(control_table.c.user_id == callback_query.from_user.id)
    selection_result = connection.execute(selection_query)
    search = InlineKeyboardButton(text="Пошук", callback_data="search")
    for user in selection_result.fetchall():
        print(user[3])
        if user[3]:
            favorites += 1
        if user[4]:
            count_complaints += 1
        count_of_coins += user[-1]
    if count_of_coins >= 10:
        sell = KeyboardButton(text="Додати оголошення", web_app=WebAppInfo(
            url=f"https://testwebform142125.000webhostapp.com/FormSecond/idUser/{callback_query.from_user.id}"))
    else:
        sell = KeyboardButton(text="Додати оголошення", callback_data="not_enough_coins")
    wallet = InlineKeyboardButton(text="Перевірити гаманець", callback_data="wallet")
    favorite = InlineKeyboardButton(text=f"Обране({favorites})", callback_data="favorite")
    my_message = InlineKeyboardButton(text=f"Мої повідомлення({count_complaints})", callback_data="my_messages")
    my_ann = InlineKeyboardButton(text="Мої оголошення", callback_data="announcement")
    share = InlineKeyboardButton(text="Розповісти про бота", callback_data="share")
    help = InlineKeyboardButton(text="Звернутися в підтримку", callback_data="help")
    mar = InlineKeyboardMarkup(row_width=2).add(search, wallet, favorite, my_message, my_ann, share, help)
    mar1 = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True).add(sell)

    if command and command.command == 'add':
        await bot.send_message(callback_query.from_user.id, "Перейти до додавання оголошення", reply_markup=mar1)
    else:
        await bot.edit_message_text(chat_id=callback_query.from_user.id, message_id=callback_query.message.message_id,
                                    text="Оберіть, що ви хочете зробити?", reply_markup=mar)

        await bot.send_message(callback_query.from_user.id, "Перейти до додавання оголошення", reply_markup=mar1)


@dp.message_handler(commands='search')
@dp.callback_query_handler(text='search')
async def search_menu(callback_query: types.CallbackQuery, command: types.BotCommand = None):
    print(callback_query.from_user.id)
    search_by_params = KeyboardButton(text="Пошук за параметрами",
                                      web_app=WebAppInfo(
                                          url=f"https://testwebform142125.000webhostapp.com/FormFirst/idUser/{callback_query.from_user.id}"))
    favorite = InlineKeyboardButton(text=f"Обране({favorites})", callback_data="favorite")
    my_message = InlineKeyboardButton(text=f"Мої повідомлення({count_complaints})", callback_data="my_messages")
    my_ann = InlineKeyboardButton(text="Мої оголошення", callback_data="announcement")
    stop_search = InlineKeyboardButton(text="Зупинити пошук", callback_data="stop_search")
    back = InlineKeyboardMarkup(text="Назад🔙", callback_data="start")
    if not_checked != 0:
        show_not_checked = InlineKeyboardButton(text=f"Показати не переглянуте({not_checked})",
                                                callback_data=cb_inline.new(action="show_not_checked", data='for_ann'))
    else:
        show_not_checked = InlineKeyboardButton(text=f"Показати не переглянуте",
                                                callback_data="show_not_checked")
    mar = InlineKeyboardMarkup(resize_keyboard=True, row_width=2).add(favorite, my_message, my_ann, stop_search,
                                                                      show_not_checked, back)
    mar1 = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True).add(search_by_params)

    if command and command.command == 'search':
        await bot.send_message(callback_query.from_user.id, 'Перейти до пошуку за параметрами',
                               reply_markup=mar1)
    else:
        await bot.edit_message_text(chat_id=callback_query.from_user.id, message_id=callback_query.message.message_id,
                                    text="Ваш вибір:", reply_markup=mar)

        await bot.send_message(callback_query.from_user.id, 'Перейти до пошуку за параметрами',
                               reply_markup=mar1)


@dp.message_handler(commands="my_messages")
@dp.callback_query_handler(text='my_messages')
async def my_messages(callback_query: types.CallbackQuery, command: types.BotCommand = None):
    control_table = db.Table("control_data", metadata, autoload_with=engine)
    selection_query = select(control_table).where(control_table.c.user_id == callback_query.from_user.id)
    selection_result = connection.execute(selection_query)
    rows = selection_result.fetchall()
    last_row = rows[-1]
    count_complaints = 0
    for row in rows:
        if row[4] and row != last_row:
            await bot.send_message(callback_query.from_user.id, f"{row[5]}\n"
                                                                f"{row[4]}\n"
                                                                f"від {callback_query.from_user.full_name} {row[2]}")
            count_complaints += 1
        elif row == last_row and count_complaints == 0:
            await bot.send_message(callback_query.from_user.id, "Скарги відсутні")


@dp.message_handler(commands='my_advertisements')
@dp.callback_query_handler(text='announcement')
async def announcement_menu(callback_query: types.CallbackQuery, command: types.BotCommand = None):
    fire_base = firestore.client()

    collection_ref = fire_base.collection('WebFormTwo')
    docs = collection_ref.stream()

    collection_watch = collection_ref.on_snapshot(on_snapshot)
    count_of_sells = 0
    count_of_rents = 0
    count_of_purchases = 0
    count_of_leases = 0
    for user_docs in docs:
        if str(user_docs.id) == str(callback_query.from_user.id):
            for id, doc in user_docs.to_dict().items():
                if doc['buttons']['section'] == ['Продати']:
                    count_of_sells += 1
                elif doc['buttons']['section'] == ['Здати в оренду']:
                    count_of_rents += 1
                elif doc['buttons']['section'] == ['Купити']:
                    count_of_purchases += 1
                elif doc['buttons']['section'] == ['Орендувати']:
                    count_of_leases += 1
    sell = InlineKeyboardButton(text=f"Продам({count_of_sells})",
                                callback_data=cb_inline.new(action="show_ann", data="sell"))
    rent_out = InlineKeyboardButton(text=f"Оренда({count_of_rents})",
                                    callback_data=cb_inline.new(action="show_ann", data="rent_out"))
    purchase = InlineKeyboardButton(text=f"Куплю({count_of_purchases})",
                                    callback_data=cb_inline.new(action="show_ann", data="purchase"))
    rent_in = InlineKeyboardButton(text=f"Зніму({count_of_leases})",
                                   callback_data=cb_inline.new(action="show_ann", data="rent_in"))
    back = InlineKeyboardButton(text="Назад🔙", callback_data="search")
    mar = InlineKeyboardMarkup(row_width=2).add(sell, rent_out, purchase, rent_in, back)
    if command and command.command == "my_advertisements":
        await bot.send_message(callback_query.from_user.id, text="Мої оголошення", reply_markup=mar)
    else:
        await bot.edit_message_text(chat_id=callback_query.from_user.id, message_id=callback_query.message.message_id,
                                    text="Мої оголошення", reply_markup=mar)


@dp.callback_query_handler(cb_inline.filter(action="show_ann"))
async def sell_ann(callback_query: types.CallbackQuery, callback_data):
    fire_base = firestore.client()

    collection_ref = fire_base.collection('WebFormTwo')
    docs = collection_ref.stream()

    collection_watch = collection_ref.on_snapshot(on_snapshot)

    for user_docs in docs:
        if str(callback_query.from_user.id) == str(user_docs.id):
            for id, doc in user_docs.to_dict().items():
                complaints = InlineKeyboardButton("Скарги", callback_data=cb_inline.new(action="complaints_show",
                                                                                        data=doc['announcementID']))
                actualize = InlineKeyboardButton("Актуалізація", callback_data=cb_inline.new(action="actualize",
                                                                                             data=doc[
                                                                                                 'announcementID']))
                non_actualize = InlineKeyboardButton("Не актуально",
                                                     callback_data=cb_inline.new(action="deactualization", data=id))
                mar = InlineKeyboardMarkup(row_width=1).add(complaints, actualize, non_actualize)
                if doc['buttons']['section'] == ['Продати'] and callback_data['data'] == "sell":
                    media = types.MediaGroup()
                    for image in doc['photoUrl']:
                        media.attach_photo(types.InputMediaPhoto(image['url']))
                    await bot.send_media_group(callback_query.from_user.id, media=media)
                    await bot.send_message(callback_query.from_user.id, f"📌ID:{doc['userID']}\n"
                                                                        f"📍Розташування: {doc['GEO']['currentCity'][0]} {doc['GEO']['streets'][0]}\n"
                                                                        f"📫{doc['GEO']['googleAdress'][1]['long_name']}, {doc['GEO']['googleAdress'][0]['long_name']}\n"
                                                                        f"🏢{doc['input']['areaFloor'][0]} з {doc['input']['areaFloorInHouse'][0]}\n"
                                                                        f"📈Площа: {doc['input']['areaTotal'][0]} м²\n"
                                                                        f"🛏Кількість кімнат: {doc['buttons']['numbRooms'][0]}\n"
                                                                        f"💰Ціна: {doc['input']['cost'][0]}$\n"
                                                                        f"👥{doc['buttons']['role'][0]}\n"
                                                                        f"{' '.join(doc['buttons']['newBuilding'])}",
                                           reply_markup=mar)
                elif doc['buttons']['section'] == ['Здати в оренду'] and callback_data['data'] == "rent_out":
                    media = types.MediaGroup()
                    for image in doc['photoUrl']:
                        media.attach_photo(types.InputMediaPhoto(image['url']))
                    await bot.send_media_group(callback_query.from_user.id, media=media)
                    await bot.send_message(callback_query.from_user.id, f"📌ID:{doc['userID']}\n"
                                                                        f"📍Розташування: {doc['GEO']['currentCity']} {doc['GEO']['streets']}\n"
                                                                        f"📫{doc['GEO']['googleAdress'][1]['long_name']}, {doc['GEO']['googleAdress'][0]['long_name']}\n"
                                                                        f"🏢{doc['input']['areaFloor'][0]} з {doc['input']['areaFloorInHouse'][0]}\n"
                                                                        f"📈Площа: {doc['input']['areaTotal'][0]} м²\n"
                                                                        f"🛏Кількість кімнат: {doc['buttons']['numbRooms'][0]}\n"
                                                                        f"💰Ціна: {doc['input']['cost'][0]} грн\n"
                                                                        f"👥{doc['buttons']['role'][0]}\n"
                                                                        f"{' '.join(doc['buttons']['newBuilding'])}",
                                           reply_markup=mar)

                elif doc['buttons']['section'] == ['Купити'] and callback_data['data'] == "purchase":
                    # media = types.MediaGroup()
                    # for image in doc['photoUrl']:
                    #     media.attach_photo(types.InputMediaPhoto(image['url']))
                    # await bot.send_media_group(callback_query.from_user.id, media=media)
                    await bot.send_message(callback_query.from_user.id, f"📌ID:{doc['userID']}\n"
                                                                        f"📍Розташування: {doc['GEO']['currentCity']}\n"
                                                                        f"Ⓜ {doc['GEO']['metroStation']}"
                                                                        f"📫{' '.join(doc['GEO']['streets'])}\n"
                                                                        f"🏢{'-'.join(doc['input']['areaFloor'])} з {'-'.join(doc['input']['areaFloorInHouse'])}\n"
                                                                        f"📈Площа: {'-'.join(doc['input']['areaTotal'])}\n"
                                                                        f"🛏Кількість кімнат: {' '.join(doc['buttons']['numbRooms'])}\n"
                                                                        f"💰Ціна:{'-'.join(doc['input']['cost'])}$\n"
                                                                        f"👥{doc['buttons']['role']}", reply_markup=mar)

                elif doc['buttons']['section'] == ['Орендувати'] and callback_data['data'] == "rent_in":
                    # media = types.MediaGroup()
                    # for image in doc['photoUrl']:
                    #     media.attach_photo(types.InputMediaPhoto(image['url']))
                    # await bot.send_media_group(callback_query.from_user.id, media=media)
                    await bot.send_message(callback_query.from_user.id, f"📌ID:{doc['userID']}\n"
                                                                        f"📍Розташування: {doc['GEO']['currentCity']}\n"
                                                                        f"Ⓜ {doc['GEO']['metroStation']}"
                                                                        f"📫{' '.join(doc['GEO']['streets'])}\n"
                                                                        f"🏢{'-'.join(doc['input']['areaFloor'])} з {'-'.join(doc['input']['areaFloorInHouse'])}\n"
                                                                        f"📈Площа: {'-'.join(doc['input']['areaTotal'])}\n"
                                                                        f"🛏Кількість кімнат: {' '.join(doc['buttons']['numbRooms'])}\n"
                                                                        f"💰Ціна:{'-'.join(doc['input']['cost'])} грн\n"
                                                                        f"👥{doc['buttons']['role']}", reply_markup=mar)


@dp.callback_query_handler(cb_inline.filter(action="actualize"))
async def actualize(callback_query: types.CallbackQuery, callback_data):
    fire_base = firestore.client()

    collection_ref = fire_base.collection('WebFormTwo')
    announcements_list = collection_ref.stream()

    collection_watch = collection_ref.on_snapshot(on_snapshot)

    for announcements in announcements_list:
        if str(callback_query.from_user.id) == str(announcements.id):
            for id, announcement in announcements.to_dict().items():
                if str(callback_data['data']) == str(id):
                    ann_date = dt.datetime.strptime(announcement['actualize_date'], "%Y-%m-%d")
                    if announcement['actualize_date'] == '':
                        actualize_set = InlineKeyboardButton("Актуалізувати",
                                                             callback_data=cb_inline.new(action='actualize_set',
                                                                                         data=callback_data['data']))
                        mar = InlineKeyboardMarkup().add(actualize_set)
                        await bot.edit_message_text("Оголошення не актуалізовано, актуалізуйте будь ласка",
                                                    callback_query.from_user.id, callback_query.message.message_id,
                                                    reply_markup=mar)
                    elif ann_date < dt.date.today() and int(
                            str(dt.date.today() - announcement['actualize_date']).split(' ')[0]) >= 30:
                        actualize_set = InlineKeyboardButton("Актуалізувати",
                                                             callback_data=cb_inline.new(action='actualize_set',
                                                                                         data=callback_data['data']))
                        mar = InlineKeyboardMarkup().add(actualize_set)
                        await bot.edit_message_text("Потрібна повторна акуалізація",
                                                    callback_query.from_user.id, callback_query.message.message_id,
                                                    reply_markup=mar)
                    elif announcement['actualize_date'] > dt.date.today():
                        await bot.edit_message_text(
                            f"Термін актуалізації закінчується {announcement['acualize_date']}\n"
                            f"Залишилось: {str(announcement['actualize_date'] - dt.date.today()).split(' ')[0]} днів",
                            callback_query.from_user.id, callback_query.message.message_id)


@dp.callback_query_handler(cb_inline.filter(action="actualize_set"))
async def set_actualize(callback_query: types.CallbackQuery, callback_data):
    fire_base = firestore.client()

    collection_ref = fire_base.collection('WebFormTwo')
    announcements_list = collection_ref.stream()

    collection_watch = collection_ref.on_snapshot(on_snapshot)

    for announcements in announcements_list:
        if str(callback_query.from_user.id) == str(announcements.id):
            for id, announcement in announcements.to_dict().items():
                if str(callback_data['data']) == str(id):
                    document_ref = fire_base.collection('WebFormTwo').document(str(callback_query.from_user.id))
                    doc = document_ref.get().to_dict()
                    if callback_data['data'] in doc:
                        doc[callback_data['data']]['actualize_date'] = str(dt.date.today())
                        document_ref.update(doc)
    back = InlineKeyboardButton(text="Назад", callback_data="announcement")
    mar = InlineKeyboardMarkup().add(back)
    await bot.edit_message_text("Актуалізовано", callback_query.from_user.id, callback_query.message.message_id,
                                reply_markup=mar)


@dp.callback_query_handler(cb_inline.filter(action="deactualization"))
async def deactualization(callback_query: types.CallbackQuery, callback_data):
    fire_base = firestore.client()

    collection_ref = fire_base.collection('WebFormTwo').document(str(callback_query.from_user.id))

    collection_watch = collection_ref.on_snapshot(on_snapshot)

    collection_ref.update({str(callback_data['data']): firestore.DELETE_FIELD})

    await bot.send_message(callback_query.from_user.id, "Оголошення видалено")


@dp.callback_query_handler(text='not_enough_coins')
async def without_coins(callback_query: types.CallbackQuery):
    back = InlineKeyboardButton(text="Назад", callback_data=cb_inline.new(action="start", data="delete"))
    mar = InlineKeyboardMarkup().add(back)
    await bot.edit_message_text("Не вистачає монет на подачу оголошення", callback_query.from_user.id,
                                callback_query.message.message_id, reply_markup=mar)


@dp.message_handler(commands=['balance'])
@dp.callback_query_handler(text='wallet')
async def wallet(callback_query: types.CallbackQuery, command: types.BotCommand = None):
    global count_of_coins
    count_of_coins = 0
    control_table = db.Table("control_data", metadata, autoload_with=engine)
    selection_query = select(control_table).where(control_table.c.user_id == callback_query.from_user.id)
    selection_result = connection.execute(selection_query)
    for row in selection_result.fetchall():
        count_of_coins += row[-1]
    help = InlineKeyboardButton(text="Звернутися в підтримку", callback_data="help")
    back = InlineKeyboardButton(text="Назад", callback_data=cb_inline.new(action="start", data="delete"))
    mar = InlineKeyboardMarkup(row_width=2).add(help, back)
    if command and command.command == 'balance':
        await bot.send_message(callback_query.from_user.id, f"Ви маєте: {count_of_coins} монет")
    else:
        await bot.edit_message_text(f"Ви маєте: {count_of_coins} монет", callback_query.from_user.id,
                                    callback_query.message.message_id, reply_markup=mar)
    count_of_coins = 0


@dp.callback_query_handler(text="edit")
async def update(callback_query: types.CallbackQuery):
    actualize = InlineKeyboardButton(text="Актуалізувати", callback_data="actualize_set")
    update = InlineKeyboardButton(text="Редагувати", callback_data="update")
    delete = InlineKeyboardButton(text="Видалити", callback_data="delete")
    back = InlineKeyboardButton(text="Назад🔙", callback_data="back")
    mar = InlineKeyboardMarkup(row_width=2).add(actualize, update, delete, back)
    await bot.edit_message_text(chat_id=callback_query.from_user.id, message_id=callback_query.message.message_id,
                                text="Редагувати", reply_markup=mar)


@dp.message_handler(commands=['support'])
@dp.callback_query_handler(text='help')
async def support(callback_query: types.CallbackQuery, command: types.BotCommand = None):
    await bot.send_message(callback_query.from_user.id,
                           f"Якщо у вас виникли питання, на які не зміг відповісти бот, напишіть нам, будь ласка: @eBAZAadmin")


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


def filters(doc, long, lat, floor, area, price, city_name, role, option, street, metro, room,
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

                    def convert_usd_to_uah(amount):
                        try:
                            response = requests.get('https://api.exchangerate-api.com/v4/latest/USD')
                            data = response.json()
                            exchange_rate = data['rates']['UAH']
                            uah_amount = amount * exchange_rate
                            return uah_amount
                        except (requests.exceptions.RequestException, KeyError):
                            return None

                    price = convert_usd_to_uah(int(price))
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


@dp.callback_query_handler(cb_inline.filter(action='show_not_checked'))
@dp.callback_query_handler(cb_inline.filter(action='more'))
@dp.message_handler(content_types=['web_app_data'])
async def web_app(message: types.Message, callback_data=None):
    if callback_data is None:
        callback_data = {'data': ''}
    if callback_data['data'] == 'for_ann' or str(message.web_app_data.data) == 'completed':
        # add_new_user('first', message.from_user.id)
        global current_row, temp, not_checked, current_num_row
        rieltor_table = db.Table("rieltor_data", metadata, autoload_with=engine)
        select_query = db.select(rieltor_table)
        selection_result = connection.execute(select_query)
        doc = check_id_form1(message.from_user.id)

        breaking = False
        rows = selection_result.fetchall()
        last_row = rows[-1]

        for check_row in rows:
            if check_row == current_row:
                for row_num in range(0, len(rows)):
                    if current_num_row == row_num:
                        current_num_row = row_num + 1
                        row = rows[row_num]
                        not_checked = len(rows) - temp
                        current_row = row

                        images = base64.b64decode(row[-6].encode())
                        images = zlib.decompress(images).decode()
                        images = json.loads(images)
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
                        if filters(doc=doc, long=row[-5], lat=row[-4], floor=row[7],
                                   area=row[8], price=row[5], city_name=row[2], role=row[-7],
                                   option=row[-2], street=row[4], metro=metro, room=row[6],
                                   new_building=new_building, commission=commission, land_area=row[9],
                                   landmark=landmark, city=row[2]):
                            if temp % 6 != 0:
                                for image in images:
                                    if count < len(images) and count < 10:
                                        if current_row != last_row:
                                            details = InlineKeyboardButton(text="Детальніше",
                                                                           callback_data=cb_inline.new(action="details",
                                                                                                       data=row[-3]))
                                            # error = InlineKeyboardButton(text="Помилка/Поскаржитись", callback_data="error")
                                            change = InlineKeyboardButton(text="Змінити пошук", callback_data="change")
                                            stop = InlineKeyboardButton(text="Зупинити пошук", callback_data="stop")
                                            share = InlineKeyboardButton(text="Розповісти про бот",
                                                                         callback_data="share")
                                            phone_num = InlineKeyboardButton(text="Показати номер телефону",
                                                                             callback_data=cb_inline.new(
                                                                                 action="phone_num_web",
                                                                                 data=row[-3]))
                                            more = InlineKeyboardButton(text="Показати ще",
                                                                        callback_data=cb_inline.new(action="more",
                                                                                                    data='for_ann'))
                                            mar = InlineKeyboardMarkup(row_width=2).add(details, phone_num, change,
                                                                                        stop,
                                                                                        share,
                                                                                        more)
                                            media.attach_photo(types.InputMediaPhoto(image))
                                        else:
                                            announcements = check_data_from_user(message.from_user.id)
                                            control_table = db.Table('control_data', metadata, autoload_with=engine)
                                            selection_query = select(control_table).where(
                                                control_table.c.user_id == message.from_user.id)
                                            selection_res = connection.execute(selection_query)
                                            user = ""
                                            for control_element in selection_res.fetchall():
                                                if control_element[2]:
                                                    user = control_element
                                            for announcement in announcements:

                                                media = types.MediaGroup()
                                                if temp % 6 != 0:
                                                    for bot_image in announcement['photoUrl']:
                                                        details = InlineKeyboardButton(text="Детальніше",
                                                                                       callback_data=cb_inline.new(
                                                                                           action="details_bot",
                                                                                           data=announcement[
                                                                                               'announcementID']))
                                                        error = InlineKeyboardButton(text="Помилка/Поскаржитись",
                                                                                     callback_data=cb_inline.new(
                                                                                         action="error",
                                                                                         data=
                                                                                         announcement[
                                                                                             'announcementID']))
                                                        change = InlineKeyboardButton(text="Змінити пошук",
                                                                                      callback_data="change")
                                                        stop = InlineKeyboardButton(text="Зупинити пошук",
                                                                                    callback_data="stop")
                                                        share = InlineKeyboardButton(text="Розповісти про бот",
                                                                                     callback_data="share")
                                                        phone_num = InlineKeyboardButton(text="Показати номер телефону",
                                                                                         callback_data=cb_inline.new(
                                                                                             action="phone_num_web",
                                                                                             data=[announcement['GEO'][
                                                                                                       'complex'][0],
                                                                                                   user[2]]))
                                                        more = InlineKeyboardButton(text="Показати ще",
                                                                                    callback_data=cb_inline.new(
                                                                                        action="more", data='for_ann'))
                                                        mar = InlineKeyboardMarkup(row_width=2).add(details, phone_num,
                                                                                                    error,
                                                                                                    change, stop, share,
                                                                                                    more)
                                                        media.attach_photo(types.InputMediaPhoto(bot_image['url']))
                                                    temp += 1
                                                    await bot.send_media_group(message.from_user.id, media=media)
                                                    await bot.send_message(message.from_user.id,
                                                                           f"📌ID:{announcement['anouncementID']}\n"
                                                                           f"📍Розташування: {announcements['GEO']['currentCity']} {announcement['GEO']['streets']}\n"
                                                                           f"🏢{announcement['GEO']['complex']}\n"
                                                                           f"📫{announcement['GEO']['googleAdress'][1]['long_name']}, {announcement['GEO']['googleAdress'][0]['long_name']}\n"
                                                                           f"🏢{announcement['input']['areaFloor'][0]} з {announcement['input']['areaFloorInHouse'][0]}\n"
                                                                           f"📈Площа: {announcement['input']['areaTotal'][0]} м²\n"
                                                                           f"🛏{announcement['buttons']['numbRooms'][0]} кімнат\n"
                                                                           f"💰Ціна: {announcement['input']['cost'][0]}\n"
                                                                           f"👥{announcement['buttons']['role'][0]}",
                                                                           reply_markup=mar)
                                                else:
                                                    breaking = True
                                                    temp += 1
                                                    break


                                    elif count == len(images) or count == 10:
                                        if current_row != last_row:
                                            temp += 1
                                            await bot.send_media_group(message.from_user.id, media=media)
                                            await bot.send_message(message.from_user.id, f"📌ID:{row[-3]}\n"
                                                                                         f"📍Розташування: {row[3]}\n"
                                                                                         f"🏢{new_building}\n"
                                                                                         f"📫{row[4]}\n"
                                                                                         f"🏢{row[7]}\n"
                                                                                         f"📈Площа: {row[8]}\n"
                                                                                         f"🛏{row[6]}\n"
                                                                                         f"💰Ціна:{row[5]}\n"
                                                                                         f"👥{row[-7]}",
                                                                   reply_markup=mar)
                                        else:
                                            break
                                    elif count > len(images) or count > 10:
                                        break
                                    count += 1
                            else:
                                breaking = True
                                temp += 1
                                break
            elif breaking:
                break
    else:
        # add_new_user('second', message.from_user.id)
        check_id_form2(message.from_user.id)
        global count_of_coins
        count_of_coins -= 10
        print(count_of_coins)
        control_table = db.Table("control_data", metadata, autoload_with=engine)
        update_query = db.update(control_table).where(
            control_table.c.user_id == message.from_user.id and control_table.c.coins >= 10).values(
            coins=control_table.c.coins - 10)
        connection.execute(update_query)
        connection.commit()
        back = InlineKeyboardButton('Повернутися до меню', callback_data='start')
        mar = InlineKeyboardMarkup().add(back)
        selection_query = select(control_table).where(control_table.c.user_id == message.from_user.id)
        selection_result = connection.execute(selection_query)
        for row in selection_result:
            if row[-1]:
                print(row)
        await bot.send_message(message.from_user.id, "Оголошення успішно створено!", reply_markup=mar)


@dp.callback_query_handler(text="stop")
async def stop_search(callback_query: types.CallbackQuery):
    global temp, current_row, not_checked
    temp = 1
    current_row = ()
    not_checked = 0
    agreement = InlineKeyboardButton("Зупинити", callback_data="search")
    mar = InlineKeyboardMarkup().add(agreement)
    await bot.send_message(callback_query.from_user.id, "Ви дійсно хочете зупинити пошук?", reply_markup=mar)


@dp.callback_query_handler(text="change")
async def change_search(callback_query: types.CallbackQuery):
    global temp, current_row, not_checked
    temp = 1
    current_row = ()
    not_checked = 0
    agreement = KeyboardButton("", web_app=WebAppInfo(
        url=f"https://testwebform142125.000webhostapp.com/FormSecond/idUser/{callback_query.from_user.id}"))
    mar = ReplyKeyboardMarkup().add(agreement)
    await bot.send_message(callback_query.from_user.id, "Ви дійсно хочете змінити пошук?", reply_markup=mar)


@dp.callback_query_handler(cb_inline.filter(action="details_bot"))
@dp.callback_query_handler(cb_inline.filter(action="details"))
async def details_view(callback_query: types.CallbackQuery, callback_data):
    fav = InlineKeyboardButton(text="Додати в обране",
                               callback_data=cb_inline.new(action="add_fav", data=callback_data['data']))
    rieltor_table = db.Table("rieltor_data", metadata, autoload_with=engine)
    rieltor_query = select(rieltor_table)
    rieltor_res = connection.execute(rieltor_query)
    rieltor_elements = rieltor_res.fetchall()
    rieltor_element = ()
    for element in rieltor_elements:
        if element[-3] == callback_data['data']:
            rieltor_element = element
            break
    new_building = ''
    announcements = check_id_form2(callback_query.from_user.id)
    if rieltor_element:
        markers = json.loads(rieltor_element[-8])
        if 'newhouse' in markers:
            new_building = markers['newhouse']
    else:
        for announcement in announcements:
            if str(announcement['announcementID']) == str(callback_data['data']):
                if announcement['GEO']['complex']:
                    new_building = announcement['GEO']['complex']
    print(new_building)
    res_complex = InlineKeyboardButton(text="Квартири в цьому ЖК",
                                       callback_data=cb_inline.new(action="res_complex", data=new_building))
    complaints = InlineKeyboardButton(text="Скарги", callback_data="complaints")
    back = InlineKeyboardButton(text="Назад🔙", callback_data=cb_inline.new(action="back_text_ann",
                                                                           data=callback_data['data']))
    mar = InlineKeyboardMarkup(row_width=2).add(fav, res_complex, complaints, back)
    await bot.edit_message_text(chat_id=callback_query.from_user.id, message_id=callback_query.message.message_id,
                                text="Детальніше", reply_markup=mar)


@dp.callback_query_handler(cb_inline.filter(action="phone_num_web"))
async def phone_num_web(callback_query: types.CallbackQuery, callback_data):
    rieltor_table = db.Table('rieltor_data', metadata, autoload_with=engine)
    rieltor_query = select(rieltor_table)
    rieltor_result = connection.execute(rieltor_query)
    rieltor_elements = rieltor_result.fetchall()
    rieltor_element = ()
    for element in rieltor_elements:
        if element[-3] == callback_data['data']:
            rieltor_element = element
            break
    new_building = ''
    announcements = check_id_form2(callback_query.from_user.id)
    if rieltor_element:
        markers = json.loads(rieltor_element[-8])
        if 'newhouse' in markers:
            new_building = markers['newhouse']
    else:
        for announcement in announcements:
            if str(announcement['annoncementID']) == str(callback_data['data']):
                if announcement['GEO']['complex']:
                    new_building = announcement['GEO']['complex']
    control_table = db.Table('control_data', metadata, autoload_with=engine)
    selection_query = select(control_table).where(
        control_table.c.user_id == callback_query.from_user.id)
    selection_res = connection.execute(selection_query)
    user = ()
    for control_element in selection_res.fetchall():
        if control_element[2]:
            user = control_element
    details = InlineKeyboardButton(text="Детальніше",
                                   callback_data=cb_inline.new(action="details", data=callback_data['data']))
    error = InlineKeyboardButton(text="Помилка/Поскаржитись",
                                 callback_data=cb_inline.new(action="error", data=callback_data['data']))
    change = InlineKeyboardButton(text="Змінити пошук", callback_data="change")
    stop = InlineKeyboardButton(text="Зупинити пошук", callback_data="stop")
    share = InlineKeyboardButton(text="Розповісти про бот", callback_data="share")
    # phone_num = InlineKeyboardButton(text="Показати номер телефону")
    more = InlineKeyboardButton(text="Показати ще",
                                callback_data=cb_inline.new(action="more", data='for_ann'))
    back = InlineKeyboardButton(text="Назад🔙",
                                callback_data=cb_inline.new(action="back_text_ann", data=callback_data['data']))
    mar = InlineKeyboardMarkup(row_width=2).add(details, error, change, stop, share,
                                                more, back)
    await bot.edit_message_text(rieltor_element[-1], callback_query.from_user.id,
                                callback_query.message.message_id, reply_markup=mar)


@dp.callback_query_handler(cb_inline.filter(action="back_text_ann"))
async def return_ann_text(callback_query: types.CallbackQuery, callback_data):
    rieltor_table = db.Table("rieltor_data", metadata, autoload_with=engine)
    rieltor_query = select(rieltor_table)
    rieltor_result = connection.execute(rieltor_query)
    rows = rieltor_result.fetchall()
    row = ()
    for rieltor_row in rows:
        if rieltor_row[-3] == callback_data['data']:
            row = rieltor_row
    new_building = ''
    print(row)
    markers = json.loads(row[-8])
    if 'newhouse' in markers:
        new_building = markers['newhouse']
    announcements = check_id_form2(callback_query.from_user.id)
    control_table = db.Table("control_data", metadata, autoload_with=engine)
    control_query = select(control_table).where(str(control_table.c.user_id) == str(callback_query.from_user.id))
    control_res = connection.execute(control_query)
    user = ()
    rieltor_id = ''
    for user_row in control_res.fetchall():
        if user_row[2]:
            user = user_row
            break
    print(announcements)
    for announcement in announcements:
        if str(announcement['announcementID']) == str(callback_data['data']):
            rieltor_id = announcement['announcementID']
            details = InlineKeyboardButton(text="Детальніше",
                                           callback_data=cb_inline.new(
                                               action="details_bot",
                                               data=announcement['announcementID']))
            error = InlineKeyboardButton(text="Помилка/Поскаржитись",
                                         callback_data=cb_inline.new(action="error",
                                                                     data=
                                                                     announcement[
                                                                         'announcementID']))
            change = InlineKeyboardButton(text="Змінити пошук",
                                          callback_data="change")
            stop = InlineKeyboardButton(text="Зупинити пошук",
                                        callback_data="stop")
            share = InlineKeyboardButton(text="Розповісти про бот",
                                         callback_data="share")
            phone_num = InlineKeyboardButton(text="Показати номер телефону",
                                             callback_data=cb_inline.new(
                                                 action="phone_num_web",
                                                 data=announcement['announcementID']))
            more = InlineKeyboardButton(text="Показати ще",
                                        callback_data=cb_inline.new(
                                            action="more", data='for_ann'))
            mar = InlineKeyboardMarkup(row_width=2).add(details, phone_num, error,
                                                        change, stop, share,
                                                        more)
            await bot.edit_message_text(chat_id=callback_query.from_user.id,
                                        message_id=callback_query.message.message_id,
                                        text=f"📌ID:{announcement['anouncementID']}\n"
                                             f"📍Розташування: {announcements['GEO']['currentCity']} {announcement['GEO']['streets']}\n"
                                             f"🏢{announcement['GEO']['complex']}\n"
                                             f"📫{announcement['GEO']['googleAdress'][1]['long_name']}, {announcement['GEO']['googleAdress'][0]['long_name']}\n"
                                             f"🏢{announcement['input']['areaFloor'][0]} з {announcement['input']['areaFloorInHouse'][0]}\n"
                                             f"📈Площа: {announcement['input']['areaTotal'][0]} м²\n"
                                             f"🛏{announcement['buttons']['numbRooms'][0]} кімнат\n"
                                             f"💰Ціна: {announcement['input']['cost'][0]}\n"
                                             f"👥{announcement['buttons']['role'][0]}",
                                        reply_markup=mar)
        else:
            details = InlineKeyboardButton(text="Детальніше",
                                           callback_data=cb_inline.new(action="details",
                                                                       data=row[-3]))
            # error = InlineKeyboardButton(text="Помилка/Поскаржитись", callback_data="error")
            change = InlineKeyboardButton(text="Змінити пошук", callback_data="change")
            stop = InlineKeyboardButton(text="Зупинити пошук", callback_data="stop")
            share = InlineKeyboardButton(text="Розповісти про бот", callback_data="share")
            phone_num = InlineKeyboardButton(text="Показати номер телефону",
                                             callback_data=cb_inline.new(
                                                 action="phone_num_web",
                                                 data=row[-3]))
            more = InlineKeyboardButton(text="Показати ще",
                                        callback_data=cb_inline.new(action="more",
                                                                    data='for_ann'))
            mar = InlineKeyboardMarkup(row_width=2).add(details, phone_num, change, stop,
                                                        share,
                                                        more)
            await bot.edit_message_text(chat_id=callback_query.from_user.id,
                                        message_id=callback_query.message.message_id,
                                        text=f"📌ID:{row[-3]}\n"
                                             f"📍Розташування: {row[3]}\n"
                                             f"🏢{new_building}\n"
                                             f"📫{row[4]}\n"
                                             f"🏢{row[7]}\n"
                                             f"📈Площа: {row[8]}\n"
                                             f"🛏{row[6]}\n"
                                             f"💰Ціна:{row[5]}\n"
                                             f"👥{row[-7]}", reply_markup=mar)
    if announcements == []:
        details = InlineKeyboardButton(text="Детальніше",
                                       callback_data=cb_inline.new(action="details",
                                                                   data=row[-3]))
        # error = InlineKeyboardButton(text="Помилка/Поскаржитись", callback_data="error")
        change = InlineKeyboardButton(text="Змінити пошук", callback_data="change")
        stop = InlineKeyboardButton(text="Зупинити пошук", callback_data="stop")
        share = InlineKeyboardButton(text="Розповісти про бот", callback_data="share")
        phone_num = InlineKeyboardButton(text="Показати номер телефону",
                                         callback_data=cb_inline.new(
                                             action="phone_num_web",
                                             data=row[-3]))
        more = InlineKeyboardButton(text="Показати ще",
                                    callback_data=cb_inline.new(action="more",
                                                                data='for_ann'))
        mar = InlineKeyboardMarkup(row_width=2).add(details, phone_num, change, stop,
                                                    share,
                                                    more)
        await bot.edit_message_text(chat_id=callback_query.from_user.id, message_id=callback_query.message.message_id,
                                    text=f"📌ID:{row[-3]}\n"
                                         f"📍Розташування: {row[3]}\n"
                                         f"🏢{new_building}\n"
                                         f"📫{row[4]}\n"
                                         f"🏢{row[7]}\n"
                                         f"📈Площа: {row[8]}\n"
                                         f"🛏{row[6]}\n"
                                         f"💰Ціна:{row[5]}\n"
                                         f"👥{row[-7]}", reply_markup=mar)


@dp.message_handler(commands=['share_bot'])
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


@dp.message_handler(commands=['favorites'])
@dp.callback_query_handler(text='favorite')
async def show_favorite(callback_query: types.CallbackQuery):
    global media_id
    control_table = db.Table('control_data', metadata, autoload_with=engine)
    rieltor_table = db.Table('rieltor_data', metadata, autoload_with=engine)
    control_selection = select(control_table).where(control_table.c.user_id == callback_query.from_user.id)
    control_selection_result = connection.execute(control_selection)
    control_elements = control_selection_result.fetchall()
    last_element = control_elements[-1]
    if control_elements:
        for control_element in control_elements:
            if control_element[3]:
                rieltor_selection = select(rieltor_table).where(rieltor_table.c.rieltor_id == control_element[3])
                rieltor_selection_result = connection.execute(rieltor_selection)
                row = rieltor_selection_result.fetchone()
                images = base64.b64decode(row[-6].encode())
                images = zlib.decompress(images).decode()
                images = json.loads(images)
                markers = json.loads(row[6])
                count = 0
                media = types.MediaGroup()
                markers = json.loads(row[-8])
                new_building = ''
                if 'newhouse' in markers:
                    new_building = markers['newhouse']
                for image in images:
                    if count < 10:
                        if control_element == last_element:
                            details = InlineKeyboardButton(text="Детальніше",
                                                           callback_data=cb_inline.new(action="details_in_fav",
                                                                                       data=new_building))
                            error = InlineKeyboardButton(text="Помилка/Поскаржитись", callback_data="error")
                            phone_num = InlineKeyboardButton(text="Показати номер телефону",
                                                             callback_data=cb_inline(action="phone_num_fav",
                                                                                     data=row[-3]))
                            share = InlineKeyboardButton(text="Розповісти про бот", callback_data="share")
                            mar = InlineKeyboardMarkup(row_width=1).add(details, error, phone_num, share)
                            media.attach_photo(types.InputMediaPhoto(image))
                        else:
                            announcements = check_id_form2(callback_query.from_user.id)
                            control_table = db.Table("control_data", metadata, autoload_with=engine)
                            selection_query = select(control_table).where(
                                control_table.c.user_id == callback_query.from_user.id)
                            selection_res = connection.execute(selection_query)
                            user = {}
                            ann_ids = []
                            for row in selection_res.fetchall():
                                if row[2]:
                                    user = row
                                if row[3]:
                                    ann_ids.append(row[3])
                            for announcement in announcements:
                                if announcement['announcementID'] in ann_ids:
                                    media = types.MediaGroup()
                                    for bot_image in announcement['photoUrl']:
                                        details = InlineKeyboardButton(text="Детальніше",
                                                                       callback_data=cb_inline.new(
                                                                           action="details_in_fav",
                                                                           data=announcement['announcementID']))
                                        error = InlineKeyboardButton(text="Помилка/Поскаржитись",
                                                                     callback_data=cb_inline.new(action="error",
                                                                                                 data=announcement[
                                                                                                     'announcementID']))
                                        phone_num = InlineKeyboardButton(text="Показати номер телефону",
                                                                         callback_data=cb_inline(action="phone_num_fav",
                                                                                                 data=announcement[
                                                                                                     'anouncementID']))
                                        share = InlineKeyboardButton(text="Розповісти про бот", callback_data="share")
                                        mar = InlineKeyboardMarkup(row_width=1).add(details, error, phone_num, share)
                                        media.attach_photo(types.InputMediaPhoto(bot_image['url']))
                                        await bot.send_media_group(callback_query.from_user.id, media=media)
                                        await bot.send_message(callback_query.from_user.id,
                                                               f"📌ID:{announcement['anouncementID']}\n"
                                                               f"📍Розташування: {announcements['GEO']['currentCity']} {announcement['GEO']['streets']}\n"
                                                               f"🏢{announcement['GEO']['complex']}\n"
                                                               f"📫{announcement['GEO']['googleAdress'][1]['long_name']}, {announcement['GEO']['googleAdress'][0]['long_name']}\n"
                                                               f"🏢{announcement['input']['areaFloor'][0]} з {announcement['input']['areaFloorInHouse'][0]}\n"
                                                               f"📈Площа: {announcement['input']['areaTotal'][0]} м²\n"
                                                               f"🛏{announcement['buttons']['numbRooms'][0]} кімнат\n"
                                                               f"💰Ціна: {announcement['input']['cost'][0]}\n"
                                                               f"👥{announcement['buttons']['role'][0]}",
                                                               reply_markup=mar)
                    elif count == 10:
                        if control_element != last_element:
                            await bot.send_media_group(callback_query.from_user.id, media=media)
                            await bot.send_message(callback_query.from_user.id, f"📌ID:{row[-3]}\n"
                                                                                f"📍Розташування: {row[1].upper()},"
                                                                                f"🏢{new_building}\n"
                                                                                f"📫 {row[2]}, {row[3]}\n"
                                                                                f"🏢{row[4]}\n"
                                                                                f"📈Площа: {row[5]}\n"
                                                                                f"🛏{row[3]}\n"
                                                                                f"💰Ціна:{row[2]}\n"
                                                                                f"👥{row[7]}", reply_markup=mar)
                        else:
                            break
                    elif count > 10:
                        break
                    count += 1
    else:
        await bot.send_message(callback_query.from_user.id, "Ви ще не додали оголошень до обраного")


@dp.callback_query_handler(cb_inline.filter(action="phone_num_fav"))
async def phone_num_fav(callback_query: types.CallbackQuery, callback_data):
    rieltor_table = db.Table('rieltor_data', metadata, autoload_with=engine)
    rieltor_query = select(rieltor_table)
    rieltor_result = connection.execute(rieltor_query)
    rieltor_elements = rieltor_result.fetchall()
    rieltor_element = ()
    for element in rieltor_elements:
        if element[-3] == callback_data['data']:
            rieltor_element = element
            break
    new_building = ''
    announcements = check_id_form2(callback_query.from_user.id)
    if rieltor_element:
        markers = json.loads(rieltor_element[-8])
        if 'newhouse' in markers:
            new_building = markers['newhouse']
    else:
        for announcement in announcements:
            if str(announcement['annoncementID']) == str(callback_data['data']):
                if announcement['GEO']['complex']:
                    new_building = announcement['GEO']['complex']
    control_table = db.Table('control_data', metadata, autoload_with=engine)
    selection_query = select(control_table).where(
        control_table.c.user_id == callback_query.from_user.id)
    selection_res = connection.execute(selection_query)
    user = ()
    for control_element in selection_res.fetchall():
        if control_element[2]:
            user = control_element
    details = InlineKeyboardButton(text="Детальніше",
                                   callback_data=cb_inline.new(action="details", data=callback_data['data']))
    error = InlineKeyboardButton(text="Помилка/Поскаржитись",
                                 callback_data=cb_inline.new(action="error", data=callback_data['data']))
    change = InlineKeyboardButton(text="Змінити пошук", callback_data="change")
    stop = InlineKeyboardButton(text="Зупинити пошук", callback_data="stop")
    share = InlineKeyboardButton(text="Розповісти про бот", callback_data="share")
    # phone_num = InlineKeyboardButton(text="Показати номер телефону")
    more = InlineKeyboardButton(text="Показати ще",
                                callback_data=cb_inline.new(action="more", data='for_ann'))
    back = InlineKeyboardButton(text="Назад🔙",
                                callback_data=cb_inline.new(action="back_text_ann", data=callback_data['data']))
    mar = InlineKeyboardMarkup(row_width=2).add(details, error, change, stop, share,
                                                more, back)
    await bot.edit_message_text(rieltor_element[-1], callback_query.from_user.id,
                                callback_query.message.message_id, reply_markup=mar)


@dp.callback_query_handler(cb_inline.filter(action="back_text_fav"))
async def return_fav_text(callback_query: types.CallbackQuery, callback_data):
    rieltor_table = db.Table("rieltor_data", metadata, autoload_with=engine)
    rieltor_query = select(rieltor_table).where(str(rieltor_table.c.rieltor_id) == (callback_data['data']))
    rieltor_result = connection.execute(rieltor_query)
    row = rieltor_result.fetchone()
    markers = json.loads(row[-8])
    new_building = ''
    if 'newhouse' in markers:
        new_building = markers['newhouse']
    announcements = check_id_form2(callback_query.from_user.id)
    control_table = db.Table("control_data", metadata, autoload_with=engine)
    control_query = select(control_table).where(str(control_table.c.user_id) == str(callback_query.from_user.id))
    control_res = connection.execute(control_query)
    user = ()
    for user_row in control_res.fetchall():
        if user_row[2]:
            user = user_row
            break
    for announcement in announcements:
        if announcement['announcementID'] == callback_data['data']:
            details = InlineKeyboardButton(text="Детальніше",
                                           callback_data=cb_inline.new(
                                               action="details_bot",
                                               data=announcement['announcementID']))
            error = InlineKeyboardButton(text="Помилка/Поскаржитись",
                                         callback_data=cb_inline.new(action="error",
                                                                     data=announcement['announcementID']))
            change = InlineKeyboardButton(text="Змінити пошук",
                                          callback_data="change")
            stop = InlineKeyboardButton(text="Зупинити пошук",
                                        callback_data="stop")
            share = InlineKeyboardButton(text="Розповісти про бот",
                                         callback_data="share")
            phone_num = InlineKeyboardButton(text="Показати номер телефону",
                                             callback_data=cb_inline.new(
                                                 action="phone_num_web",
                                                 data=announcement['announcementID']))
            more = InlineKeyboardButton(text="Показати ще",
                                        callback_data=cb_inline.new(
                                            action="more", data='for_ann'))
            mar = InlineKeyboardMarkup(row_width=2).add(details, phone_num, error,
                                                        change, stop, share,
                                                        more)
            await bot.send_message(callback_query.from_user.id, f"📌ID:{announcement['anouncementID']}\n"
                                                                f"📍Розташування: {announcements['GEO']['currentCity']} {announcement['GEO']['streets']}\n"
                                                                f"🏢{announcement['GEO']['complex']}\n"
                                                                f"📫{announcement['GEO']['googleAdress'][1]['long_name']}, {announcement['GEO']['googleAdress'][0]['long_name']}\n"
                                                                f"🏢{announcement['input']['areaFloor'][0]} з {announcement['input']['areaFloorInHouse'][0]}\n"
                                                                f"📈Площа: {announcement['input']['areaTotal'][0]} м²\n"
                                                                f"🛏{announcement['buttons']['numbRooms'][0]} кімнат\n"
                                                                f"💰Ціна: {announcement['input']['cost'][0]}\n"
                                                                f"👥{announcement['buttons']['role'][0]}",
                                   reply_markup=mar)
        else:
            details = InlineKeyboardButton(text="Детальніше",
                                           callback_data=cb_inline.new(action="details",
                                                                       data=row[-3]))
            # error = InlineKeyboardButton(text="Помилка/Поскаржитись", callback_data="error")
            change = InlineKeyboardButton(text="Змінити пошук", callback_data="change")
            stop = InlineKeyboardButton(text="Зупинити пошук", callback_data="stop")
            share = InlineKeyboardButton(text="Розповісти про бот", callback_data="share")
            phone_num = InlineKeyboardButton(text="Показати номер телефону",
                                             callback_data=cb_inline.new(
                                                 action="phone_num_web",
                                                 data=row[-3]))
            more = InlineKeyboardButton(text="Показати ще",
                                        callback_data=cb_inline.new(action="more",
                                                                    data='for_ann'))
            mar = InlineKeyboardMarkup(row_width=2).add(details, phone_num, change, stop,
                                                        share,
                                                        more)
            await bot.send_message(callback_query.from_user.id, f"📌ID:{row[-3]}\n"
                                                                f"📍Розташування: {row[3]}\n"
                                                                f"🏢{new_building}\n"
                                                                f"📫{row[4]}\n"
                                                                f"🏢{row[7]}\n"
                                                                f"📈Площа: {row[8]}\n"
                                                                f"🛏{row[6]}\n"
                                                                f"💰Ціна:{row[5]}\n"
                                                                f"👥{row[-7]}", reply_markup=mar)
    if announcements == []:
        details = InlineKeyboardButton(text="Детальніше",
                                       callback_data=cb_inline.new(action="details",
                                                                   data=row[-3]))
        # error = InlineKeyboardButton(text="Помилка/Поскаржитись", callback_data="error")
        change = InlineKeyboardButton(text="Змінити пошук", callback_data="change")
        stop = InlineKeyboardButton(text="Зупинити пошук", callback_data="stop")
        share = InlineKeyboardButton(text="Розповісти про бот", callback_data="share")
        phone_num = InlineKeyboardButton(text="Показати номер телефону",
                                         callback_data=cb_inline.new(
                                             action="phone_num_web",
                                             data=row[-3]))
        more = InlineKeyboardButton(text="Показати ще",
                                    callback_data=cb_inline.new(action="more",
                                                                data='for_ann'))
        mar = InlineKeyboardMarkup(row_width=2).add(details, phone_num, change, stop,
                                                    share,
                                                    more)
        await bot.send_message(callback_query.from_user.id, f"📌ID:{row[-3]}\n"
                                                            f"📍Розташування: {row[3]}\n"
                                                            f"🏢{new_building}\n"
                                                            f"📫{row[4]}\n"
                                                            f"🏢{row[7]}\n"
                                                            f"📈Площа: {row[8]}\n"
                                                            f"🛏{row[6]}\n"
                                                            f"💰Ціна:{row[5]}\n"
                                                            f"👥{row[-7]}", reply_markup=mar)


@dp.callback_query_handler(cb_inline.filter(action="details_in_fav"))
async def details_in_fav(callback_query: types.CallbackQuery, callback_data):
    rieltor_table = db.Table("rieltor_data", metadata, autoload_with=engine)
    rieltor_query = select(rieltor_table).where(str(rieltor_table.c.rieltor_id) == str(callback_data['data']))
    rieltor_result = connection.execute(rieltor_query)
    rieltor_element = rieltor_result.fetchone()
    new_building = ''
    announcements = check_id_form2(callback_query.from_user.id)
    if rieltor_element:
        markers = json.loads(rieltor_element[-8])
        if 'newhouse' in markers:
            new_building = markers['newhouse']
    else:
        for announcement in announcements:
            if str(announcement['annoncementID']) == str(callback_data['data']):
                if announcement['GEO']['complex']:
                    new_building = announcement['GEO']['complex']
    fav = InlineKeyboardButton(text="Видалити з обране", callback_data=cb_inline.new(action="del_fav", data=
    callback_data['data']))
    res_complex = InlineKeyboardButton(text="Квартири в цьому ЖК",
                                       callback_data=cb_inline.new(action="res_complex", data=new_building))
    complaints = InlineKeyboardButton(text="Скарги", callback_data="complaints_show")
    back = InlineKeyboardButton(text="Назад🔙", callback_data=cb_inline.new(action="back", data=
    callback_data['data']))
    mar = InlineKeyboardMarkup(row_width=2).add(fav, res_complex, complaints, back)
    await bot.edit_message_text(chat_id=callback_query.from_user.id, message_id=callback_query.message.message_id,
                                text="Детальніше", reply_markup=mar)


@dp.callback_query_handler(cb_inline.filter(action="complaints_show"))
async def show_complaints(callback_query: types.CallbackQuery, callback_data):
    control_table = db.Table("control_data", metadata, autoload_with=engine)
    selection_query = select(control_table).where(str(control_table.c.announcement_id) == str(callback_data['data']))
    selection_result = connection.execute(selection_query)
    rows = selection_result.fetchall()
    control_table = db.Table('control_data', metadata, autoload_with=engine)
    selection_query = select(control_table).where(
        control_table.c.user_id == callback_query.from_user.id)
    selection_res = connection.execute(selection_query)
    for control_element in selection_res.fetchall():
        if control_element[2]:
            user = control_element
    for row in rows:
        if row[3]:
            await bot.send_message(callback_query.from_user.id, f"{callback_data['data']}\n"
                                                                f"{row[4]}\n"
                                                                f"від {callback_query.from_user.full_name} {user[2]}")


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


@dp.callback_query_handler(cb_inline.filter(action="error"))
async def complaints_view(callback_query: types.CallbackQuery, callback_data):
    mess1 = InlineKeyboardButton(text="Неактуально/Фейк", callback_data=cb_inline.new(action="complaint", data=[
        callback_data['data'], "Неактуально/Фейк"]))
    mess2 = InlineKeyboardButton(text="Невідповідні фото", callback_data=cb_inline.new(action="complaint", data=[
        callback_data['data'], "Невідповідні фото"]))
    mess3 = InlineKeyboardButton(text="Невірні (поверх,площа або ціна)",
                                 callback_data=cb_inline.new(action="complaint", data=[
                                     callback_data['data'], "Невірний опис"]))
    mess4 = InlineKeyboardButton(text="Це мій ексклюзив", callback_data=cb_inline.new(action="complaint", data=[
        callback_data['data'], "Це мій ексклюзив"]))
    mess5 = InlineKeyboardButton(text="Підозрілий об`єкт", callback_data=cb_inline.new(action="complaint", data=[
        callback_data['data'], "Підозрілий об`єкт"]))
    back = InlineKeyboardButton(text="Назад🔙", callback_data=cb_inline.new(action="back", data=
    callback_data['data']))
    mar = InlineKeyboardMarkup(row_width=3).add(mess1, mess2, mess3, mess4, mess5, back)
    await bot.edit_message_text(chat_id=callback_query.from_user.id, message_id=callback_query.message.message_id,
                                text="Помилка/Поскаржитись", reply_markup=mar)


@dp.callback_query_handler(cb_inline.filter(action='complaint'))
async def send_complaint(callback_query: types.CallbackQuery, callback_data):
    announcement_id = callback_data['data'][0]
    complaint = callback_data['data'][1]
    control_table = db.Table('control_data', metadata, autoload_with=engine)
    insertion_query = control_table.insert().values(user_id=callback_query.from_user.id,
                                                    complaint=complaint,
                                                    announcement_id=announcement_id)
    connection.execute(insertion_query)
    connection.commit()
    await bot.send_message(callback_query.from_user.id,
                           text=f"Оголошення {announcement_id}, скарга {' '.join(complaint)} успішно відправлена!")


@dp.callback_query_handler(cb_inline.filter(action='res_complex'))
async def all_flats_in_complex(callback_query: types.CallbackQuery, callback_data):
    rieltor_table = db.Table('rieltor_data', metadata, autoload_with=engine)
    selection_query = select(rieltor_table)
    selection_result = connection.execute(selection_query)
    rows = selection_result.fetchall()
    last_row = rows[-1]
    for row in rows:
        markers = json.loads(row[-8])
        if 'newhouse' in markers:
            if callback_data['data'] == markers['newhouse']:
                images = base64.b64decode(row[-6].encode())
                images = zlib.decompress(images).decode()
                images = json.loads(images)
                count = 0
                media = types.MediaGroup()
                share = InlineKeyboardButton(text="Розповісти про бот", callback_data="share")
                details = InlineKeyboardButton(text="Детальніше",
                                               callback_data=cb_inline.new(action="details_in_complex",
                                                                           data=callback_data['data']))
                phone_num = InlineKeyboardButton(text="Показати номер телефону",
                                                 callback_data=cb_inline.new(action="phone_num_complex",
                                                                             data=row[-3]))
                error = InlineKeyboardButton(text="Помилка/Поскаржитись", callback_data="error")
                mar = InlineKeyboardMarkup(row_width=1).add(details, error, phone_num, share)
                for image in images:
                    if count < len(images) and count < 9:
                        if row != last_row:
                            media.attach_photo(types.InputMediaPhoto(image))
                        else:
                            announcements = check_id_form2(callback_query.from_user.id)
                            control_table = db.Table("control_data", metadata, autoload_with=engine)
                            selection_query = select(control_table).where(
                                control_table.c.user_id == callback_query.from_user.id)
                            selection_res = connection.execute(selection_query)
                            for control_element in selection_res.fetchall():
                                if control_element[2]:
                                    user = control_element
                            for announcement in announcements:
                                if callback_data['data'] in announcement['GEO']['complex']:
                                    media = types.MediaGroup()
                                    for bot_image in announcement['photoUrl']:
                                        details = InlineKeyboardButton(text="Детальніше",
                                                                       callback_data=cb_inline.new(
                                                                           action="details_in_fav",
                                                                           data=[announcement[
                                                                                     'announcementID'],
                                                                                 announcement[
                                                                                     'GEO'][
                                                                                     'complex'][
                                                                                     0]]))
                                        error = InlineKeyboardButton(text="Помилка/Поскаржитись",
                                                                     callback_data=cb_inline.new(action="error",
                                                                                                 data=
                                                                                                 announcement[
                                                                                                     'announcementID']))
                                        phone_num = InlineKeyboardButton(text="Показати номер телефону",
                                                                         callback_data=cb_inline(
                                                                             action="phone_num_complex",
                                                                             data=announcement['anouncementID']))
                                        share = InlineKeyboardButton(text="Розповісти про бот", callback_data="share")
                                        mar = InlineKeyboardMarkup(row_width=1).add(details, error, phone_num, share)
                                        media.attach_photo(types.InputMediaPhoto(bot_image['url']))
                                        await bot.send_media_group(callback_query.from_user.id, media=media)
                                        await bot.send_message(callback_query.from_user.id,
                                                               f"📌ID:{announcement['anouncementID']}\n"
                                                               f"📍Розташування: {announcements['GEO']['currentCity']} {announcement['GEO']['streets']}\n"
                                                               f"🏢{announcement['GEO']['complex']}\n"
                                                               f"📫{announcement['GEO']['googleAdress'][1]['long_name']}, {announcement['GEO']['googleAdress'][0]['long_name']}\n"
                                                               f"🏢{announcement['input']['areaFloor'][0]} з {announcement['input']['areaFloorInHouse'][0]}\n"
                                                               f"📈Площа: {announcement['input']['areaTotal'][0]} м²\n"
                                                               f"🛏{announcement['buttons']['numbRooms'][0]} кімнат\n"
                                                               f"💰Ціна: {announcement['input']['cost'][0]}\n"
                                                               f"👥{announcement['buttons']['role'][0]}",
                                                               reply_markup=mar)
                    elif count == len(images) or count == 9:
                        if row != last_row:
                            markers = json.loads(row[-8])
                            new_building = ''
                            if 'newhouse' in markers:
                                new_building = markers['newhouse']
                            print("images count - " + str(len(images)))
                            await bot.send_media_group(callback_query.from_user.id, media=media)
                            await bot.send_message(callback_query.from_user.id, f"📌ID:{row[-3]}\n"
                                                                                f"📍Розташування: {row[3]}\n"
                                                                                f"🏢{new_building}\n"
                                                                                f"📫{row[4]}\n"
                                                                                f"🏢{row[7]}\n"
                                                                                f"📈Площа: {row[8]}\n"
                                                                                f"🛏{row[6]}\n"
                                                                                f"💰Ціна:{row[5]}\n"
                                                                                f"👥{row[-7]}", reply_markup=mar)
                        else:
                            break
                    elif count > len(images) or count > 9:
                        break
                    count += 1


@dp.callback_query_handler(cb_inline.filter(action="phone_num_complex"))
async def phone_num_complex(callback_query: types.CallbackQuery, callback_data):
    rieltor_table = db.Table('rieltor_data', metadata, autoload_with=engine)
    rieltor_query = select(rieltor_table)
    rieltor_result = connection.execute(rieltor_query)
    rieltor_elements = rieltor_result.fetchall()
    rieltor_element = ()
    for element in rieltor_elements:
        if element[-3] == callback_data['data']:
            rieltor_element = element
            break
    new_building = ''
    announcements = check_id_form2(callback_query.from_user.id)
    if rieltor_element:
        markers = json.loads(rieltor_element[-8])
        if 'newhouse' in markers:
            new_building = markers['newhouse']
    else:
        for announcement in announcements:
            if str(announcement['annoncementID']) == str(callback_data['data']):
                if announcement['GEO']['complex']:
                    new_building = announcement['GEO']['complex']
    control_table = db.Table('control_data', metadata, autoload_with=engine)
    selection_query = select(control_table).where(
        control_table.c.user_id == callback_query.from_user.id)
    selection_res = connection.execute(selection_query)
    user = ()
    for control_element in selection_res.fetchall():
        if control_element[2]:
            user = control_element
    details = InlineKeyboardButton(text="Детальніше",
                                   callback_data=cb_inline.new(action="details", data=callback_data['data']))
    error = InlineKeyboardButton(text="Помилка/Поскаржитись",
                                 callback_data=cb_inline.new(action="error", data=callback_data['data']))
    change = InlineKeyboardButton(text="Змінити пошук", callback_data="change")
    stop = InlineKeyboardButton(text="Зупинити пошук", callback_data="stop")
    share = InlineKeyboardButton(text="Розповісти про бот", callback_data="share")
    # phone_num = InlineKeyboardButton(text="Показати номер телефону")
    more = InlineKeyboardButton(text="Показати ще",
                                callback_data=cb_inline.new(action="more", data='for_ann'))
    back = InlineKeyboardButton(text="Назад🔙",
                                callback_data=cb_inline.new(action="back_text_ann", data=callback_data['data']))
    mar = InlineKeyboardMarkup(row_width=2).add(details, error, change, stop, share,
                                                more, back)
    await bot.edit_message_text(rieltor_element[-1], callback_query.from_user.id,
                                callback_query.message.message_id, reply_markup=mar)


@dp.callback_query_handler(cb_inline.filter(action="details_in_complex"))
async def details_in_complex(callback_query: types.CallbackQuery, callback_data):
    fav = InlineKeyboardButton(text="Додати в обране", callback_data=cb_inline.new(action="add_fav", data=
    callback_data['data']))
    complaints = InlineKeyboardButton(text="Скарги", callback_data="complaints_show")
    back = InlineKeyboardButton(text="Назад🔙",
                                callback_data=cb_inline.new(action="back_text_ann", data=callback_data['data']))
    mar = InlineKeyboardMarkup(row_width=2).add(fav, complaints, back)
    await bot.edit_message_text(chat_id=callback_query.from_user.id, message_id=callback_query.message.message_id,
                                text="Детальніше", reply_markup=mar)


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


if __name__ == "__main__":
    cred = credentials.Certificate("aleksandr-c0286-firebase-adminsdk-4k3sz-ebc5beaae1.json")
    firebase_admin.initialize_app(cred)
    open_rieltor_data()
    create_db_control()
    executor.start_polling(dp, skip_updates=True)
