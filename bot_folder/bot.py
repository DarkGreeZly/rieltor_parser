from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from aiogram.utils.callback_data import CallbackData
import sqlalchemy as db
from sqlalchemy import select, column
from sqlalchemy.sql.expression import exists
import asyncio
import aiohttp
import json
import zlib
import ast
import base64
import phonenumbers
from config import metadata, create_connection, TOKEN, cb_inline, cred, count_of_coins, count_complaints, \
                    current_time, current_row, current_num_row, not_checked, temp, media_id, favorites, rows, start_message1, start_message2, phone_number
from settings import on_snapshot, check_id_form2, check_id_form1, check_data_from_user, filters, open_rieltor_data, create_db_control
import firebase_admin
from firebase_admin import credentials, firestore
import datetime as dt

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

@dp.message_handler(commands=['start'])
async def command_start(message: types.Message):
    async with connection.begin() as transaction:
        control_table = db.Table('control_data', metadata, autoload=True)
        selection_query = select(control_table).where(control_table.c.user_id == message.from_user.id)
        selection_query = exists(selection_query).select()
        selection_result = await connection.execute(selection_query)
        # print(selection_result.fetchone()[0])
        if await selection_result.fetchone()[0] == False:
            if str(message.text[7:]) != "":
                insertion_query = db.insert(control_table).values(user_id=message.from_user.id,
                                                                  referral=str(message.text[7:]),
                                                                  coins=30)
                await connection.execute(insertion_query)
                await transaction.commit()
                insertion_query_referral = db.insert(control_table).values(user_id=str(message.text[7:]),
                                                                           coins=5)
                await connection.execute(insertion_query_referral)
                await transaction.commit()
            else:
                insertion_query = db.insert(control_table).values(user_id=message.from_user.id, referral="None",
                                                                  coins=30)
                await connection.execute(insertion_query)
                transaction.commit()

    await bot.send_message(message.from_user.id, start_message1, parse_mode='HTML')
    await asyncio.sleep(2)
    await bot.send_message(message.from_user.id, start_message2)
    await asyncio.sleep(2)
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
    async with connection.begin():
        control_table = db.Table("control_data", metadata, autoload=True)
        selection_query = select(control_table).where(control_table.c.user_id == callback_query.from_user.id)
        selection_result = await connection.execute(selection_query)
        search = InlineKeyboardButton(text="Пошук", callback_data="search")
        favorites = 0
        count_complaints = 0
        for user in await selection_result.fetchall():
            if user[3]:
                async with connection.begin():
                    rieltor_table = db.Table("rieltor_data", metadata, autoload=True)
                    rieltor_query = select(rieltor_table).where(rieltor_table.c.rieltor_id == user[3])
                    rieltor_result = await connection.execute(rieltor_query)
                    res = await rieltor_result.fetchone()
                    if res and res[-3]:
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
    async with connection.begin():
        control_table = db.Table("control_data", metadata, autoload=True)
        selection_query = select(control_table).where(control_table.c.user_id == callback_query.from_user.id)
        selection_result = await connection.execute(selection_query)
        favorites = 0
        count_complaints = 0
        for user in await selection_result.fetchall():
            if user[3]:
                async with connection.begin():
                    rieltor_table = db.Table("rieltor_data", metadata, autoload=True)
                    rieltor_query = select(rieltor_table).where(rieltor_table.c.rieltor_id == user[3])
                    rieltor_result = await connection.execute(rieltor_query)
                    res = await rieltor_result.fetchone()
                    if res and res[-3]:
                        favorites += 1
            if user[4]:
                count_complaints += 1
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
    async with connection.begin():
        control_table = db.Table("control_data", metadata, autoload=True)
        selection_query = select(control_table).where(control_table.c.user_id == callback_query.from_user.id)
        selection_result = await connection.execute(selection_query)
        rows = await selection_result.fetchall()
        last_row = rows[-1]
        count_complaints = 0
        for row in rows:
            if row[4] and row != last_row:
                await bot.send_message(callback_query.from_user.id, f"{row[5]}\n"
                                                                    f"{row[4]}\n"
                                                                    f"від {callback_query.from_user.full_name} {row[2] if row[2] else ''}")
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
                                                                        f"Ⓜ {' '.join(doc['GEO']['metroStation']) if doc['GEO']['metroStation'] else ''}\n"
                                                                        f"📫{' '.join(doc['GEO']['streets'])}\n"
                                                                        f"🏢{'-'.join([str(i) for i in doc['input']['areaFloor']])} з {'-'.join([str(i) for i in doc['input']['areaFloorInHouse']])}\n"
                                                                        f"📈Площа: {'-'.join([str(i) for i in doc['input']['areaTotal']])}\n"
                                                                        f"🛏Кількість кімнат: {' '.join([str(i) for i in doc['buttons']['numbRooms']])}\n"
                                                                        f"💰Ціна:{'-'.join([str(i) for i in doc['input']['cost']])} грн\n"
                                                                        f"👥{doc['buttons']['role'][0]}", reply_markup=mar)

                elif doc['buttons']['section'] == ['Орендувати'] and callback_data['data'] == "rent_in":
                    # media = types.MediaGroup()
                    # for image in doc['photoUrl']:
                    #     media.attach_photo(types.InputMediaPhoto(image['url']))
                    # await bot.send_media_group(callback_query.from_user.id, media=media)
                    await bot.send_message(callback_query.from_user.id, f"📌ID:{doc['userID']}\n"
                                                                        f"📍Розташування: {doc['GEO']['currentCity']}\n"
                                                                        f"Ⓜ {' '.join(doc['GEO']['metroStation']) if doc['GEO']['metroStation'] else ''}\n"
                                                                        f"📫{' '.join(doc['GEO']['streets'])}\n"
                                                                        f"🏢{'-'.join([str(i) for i in doc['input']['areaFloor']])} з {'-'.join([str(i) for i in doc['input']['areaFloorInHouse']])}\n"
                                                                        f"📈Площа: {'-'.join([str(i) for i in doc['input']['areaTotal']])}\n"
                                                                        f"🛏Кількість кімнат: {' '.join([str(i) for i in doc['buttons']['numbRooms']])}\n"
                                                                        f"💰Ціна:{'-'.join([str(i) for i in doc['input']['cost']])} грн\n"
                                                                        f"👥{doc['buttons']['role'][0]}", reply_markup=mar)


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
    async with connection.begin():
        control_table = db.Table("control_data", metadata, autoload=True)
        selection_query = select(control_table).where(control_table.c.user_id == callback_query.from_user.id)
        selection_result = await connection.execute(selection_query)
        for row in await selection_result.fetchall():
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


@dp.callback_query_handler(cb_inline.filter(action='show_not_checked'))
@dp.callback_query_handler(cb_inline.filter(action='more'))
@dp.message_handler(content_types=['web_app_data'])
async def web_app(message: types.Message, callback_data=None):
    if callback_data is None:
        callback_data = {'data': ''}
    if callback_data['data'] == 'for_ann' or str(message.web_app_data.data) == 'completed':
        # add_new_user('first', message.from_user.id)
        global current_row, temp, not_checked, current_num_row, rows
        async with connection.begin():
            rieltor_table = db.Table("rieltor_data", metadata, autoload=True)
            select_query = db.select(rieltor_table)
            selection_result = await connection.execute(select_query)
            doc = check_id_form1(message.from_user.id)

            breaking = False
            rows = await selection_result.fetchall()
            last_row = rows[-1]

            for check_row in rows:
                if current_row == ():
                    current_row = check_row
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
                            if await filters(doc=doc, long=row[-5], lat=row[-4], floor=row[7],
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
                                                error = InlineKeyboardButton(text="Помилка/Поскаржитись", callback_data=cb_inline.new(action="error", data=row[-3]))
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
                                                mar = InlineKeyboardMarkup(row_width=2).add(details, error, phone_num, change,
                                                                                            stop,
                                                                                            share,
                                                                                            more)
                                                media.attach_photo(types.InputMediaPhoto(image))
                                            else:
                                                announcements = check_data_from_user(message.from_user.id)
                                                async with connection.begin():
                                                    control_table = db.Table('control_data', metadata, autoload=True)
                                                    selection_query = select(control_table).where(
                                                        control_table.c.user_id == message.from_user.id)
                                                    selection_res = await connection.execute(selection_query)
                                                    user = ""
                                                    for control_element in await selection_res.fetchall():
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
                if breaking:
                    break

    else:
        # add_new_user('second', message.from_user.id)
        check_id_form2(message.from_user.id)
        global count_of_coins
        count_of_coins -= 10
        print(count_of_coins)
        async with connection.begin() as transaction:
            control_table = db.Table("control_data", metadata, autoload=True)
            update_query = db.update(control_table).where(
                control_table.c.user_id == message.from_user.id and control_table.c.coins >= 10).values(
                coins=control_table.c.coins - 10)
            await connection.execute(update_query)
            await transaction.commit()
            back = InlineKeyboardButton('Повернутися до меню', callback_data='start')
            mar = InlineKeyboardMarkup().add(back)
            async with connection.begin():
                selection_query = select(control_table).where(control_table.c.user_id == message.from_user.id)
                selection_result = await connection.execute(selection_query)
                for row in selection_result.fetchall():
                    if row[-1]:
                        print(row)
            await bot.send_message(message.from_user.id, "Оголошення успішно створено!", reply_markup=mar)


@dp.callback_query_handler(text="stop")
async def stop_search(callback_query: types.CallbackQuery):
    global temp, current_row, not_checked, current_num_row
    temp = 1
    current_row = ()
    current_num_row = 0
    not_checked = 0
    agreement = InlineKeyboardButton("Зупинити", callback_data="search")
    mar = InlineKeyboardMarkup().add(agreement)
    await bot.send_message(callback_query.from_user.id, "Ви дійсно хочете зупинити пошук?", reply_markup=mar)


@dp.callback_query_handler(text="change")
async def change_search(callback_query: types.CallbackQuery):
    global temp, current_row, not_checked, current_num_row
    temp = 1
    current_row = ()
    current_num_row = 0
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
    async with connection.begin():
        rieltor_table = db.Table("rieltor_data", metadata, autoload=True)
        rieltor_query = select(rieltor_table)
        rieltor_res = await connection.execute(rieltor_query)
        rieltor_elements = await rieltor_res.fetchall()
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
    complaints = InlineKeyboardButton(text="Скарги", callback_data=cb_inline.new(action="complaints_show", data=callback_data['data']))
    back = InlineKeyboardButton(text="Назад🔙", callback_data=cb_inline.new(action="back_text_ann",
                                                                           data=callback_data['data']))
    mar = InlineKeyboardMarkup(row_width=2).add(fav, res_complex, complaints, back)
    await bot.edit_message_text(chat_id=callback_query.from_user.id, message_id=callback_query.message.message_id,
                                text="Детальніше", reply_markup=mar)


@dp.callback_query_handler(cb_inline.filter(action="phone_num_web"))
async def phone_num_web(callback_query: types.CallbackQuery, callback_data):
    async with connection.begin():
        rieltor_table = db.Table('rieltor_data', metadata, autoload=True)
        rieltor_query = select(rieltor_table)
        rieltor_result = await connection.execute(rieltor_query)
        rieltor_elements = await rieltor_result.fetchall()
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
    async with connection.begin():
        control_table = db.Table('control_data', metadata, autoload=True)
        selection_query = select(control_table).where(
            control_table.c.user_id == callback_query.from_user.id)
        selection_res = await connection.execute(selection_query)
        user = ()
        for control_element in await selection_res.fetchall():
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
    async with connection.begin():
        rieltor_table = db.Table("rieltor_data", metadata, autoload=True)
        rieltor_query = select(rieltor_table)
        rieltor_result = await connection.execute(rieltor_query)
        rows = await rieltor_result.fetchall()
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
    async with connection.begin():
        control_table = db.Table("control_data", metadata, autoload=True)
        control_query = select(control_table).where(str(control_table.c.user_id) == str(callback_query.from_user.id))
        control_res = await connection.execute(control_query)
        user = ()
        rieltor_id = ''
        for user_row in await control_res.fetchall():
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
            error = InlineKeyboardButton(text="Помилка/Поскаржитись", callback_data=cb_inline.new(action="error", data=row[-3]))
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
            mar = InlineKeyboardMarkup(row_width=2).add(details, error, phone_num, change, stop,
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
        error = InlineKeyboardButton(text="Помилка/Поскаржитись", callback_data=cb_inline.new(action="error", data=row[-3]))
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
        mar = InlineKeyboardMarkup(row_width=2).add(details, error, phone_num, change, stop,
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
    async with connection.begin():
        control_table = db.Table("control_data", metadata, autoload=True)
        selection_query = select(control_table).where(control_table.c.referral == callback_query.from_user.id)
        selection_result = await connection.execute(selection_query)
        await bot.send_message(chat_id=callback_query.from_user.id, text=f"Це твоє реферальне посилання.\n"
                                                                         f"https://t.me/eBAZA_estate_bot?start={callback_query.from_user.id}\n"
                                                                         f"Кількість рефералів: {len(await selection_result.fetchall())}")


@dp.callback_query_handler(cb_inline.filter(action="add_fav"))
async def add_fav(callback_query: types.CallbackQuery, callback_data):
    async with connection.begin() as transaction:
        control_table = db.Table('control_data', metadata, autoload=True)
        insertion_query = control_table.insert().values(user_id=callback_query.from_user.id, favorite=callback_data['data'])
        await connection.execute(insertion_query)
        await transaction.commit()
    mess = await bot.send_message(callback_query.from_user.id, f"Оголошення {callback_data['data']} додане до Обране")
    await asyncio.sleep(20)
    await bot.delete_message(callback_query.from_user.id, mess.message_id)


@dp.message_handler(commands=['favorites'])
@dp.callback_query_handler(text='favorite')
async def show_favorite(callback_query: types.CallbackQuery):
    global media_id
    async with connection.begin():
        control_table = db.Table('control_data', metadata, autoload=True)
        rieltor_table = db.Table('rieltor_data', metadata, autoload=True)
        control_selection = select(control_table).where(control_table.c.user_id == callback_query.from_user.id)
        control_selection_result = await connection.execute(control_selection)
        control_elements = await control_selection_result.fetchall()
        control_elements_count = len(control_elements)
        anns_count = len(check_id_form2(callback_query.from_user.id))
        count_of_favs = 0
        count_of_anns = 0
    if control_elements:
        for control_element in control_elements:
            if control_element[3]:
                rieltor_selection = select(rieltor_table)
                rieltor_selection_result = connection.execute(rieltor_selection)
                rows = rieltor_selection_result.fetchall()
                row = ()
                for element in rows:
                    if element[-3] == control_element[3]:
                        row = element
                        break
                    else:
                        row = False
                if row == False:
                    continue
                images = base64.b64decode(row[-6].encode())
                images = zlib.decompress(images).decode()
                images = json.loads(images)
                count = 0
                media = types.MediaGroup()
                markers = json.loads(row[-8])
                new_building = ''
                if 'newhouse' in markers:
                    new_building = markers['newhouse']
                for image in images:
                    if count < 10:
                        if count_of_favs < control_elements_count:
                            details = InlineKeyboardButton(text="Детальніше",
                                                           callback_data=cb_inline.new(action="details_in_fav",
                                                                                       data=new_building))
                            error = InlineKeyboardButton(text="Помилка/Поскаржитись", callback_data=cb_inline.new(action="error", data=row[-3]))
                            phone_num = InlineKeyboardButton(text="Показати номер телефону",
                                                             callback_data=cb_inline.new(action="phone_num_fav",
                                                                                     data=row[-3]))
                            share = InlineKeyboardButton(text="Розповісти про бот", callback_data="share")
                            mar = InlineKeyboardMarkup(row_width=1).add(details, error, phone_num, share)
                            media.attach_photo(types.InputMediaPhoto(image))
                        elif count_of_anns < anns_count:
                            announcements = check_id_form2(callback_query.from_user.id)
                            async with connection.begin():
                                control_table = db.Table("control_data", metadata, autoload=True)
                                selection_query = select(control_table).where(
                                    control_table.c.user_id == callback_query.from_user.id)
                                selection_res = await connection.execute(selection_query)
                                user = {}
                                ann_ids = []
                                for row in await selection_res.fetchall():
                                    if row[2]:
                                        user = row
                                    if row[3]:
                                        ann_ids.append(row[3])
                            if announcements != []:
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
                        if count_of_favs <= control_elements_count:
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
    async with connection.begin():
        rieltor_table = db.Table('rieltor_data', metadata, autoload=True)
        rieltor_query = select(rieltor_table)
        rieltor_result = await connection.execute(rieltor_query)
        rieltor_elements = await rieltor_result.fetchall()
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
    async with connection.begin():
        control_table = db.Table('control_data', metadata, autoload=True)
        selection_query = select(control_table).where(
            control_table.c.user_id == callback_query.from_user.id)
        selection_res = await connection.execute(selection_query)
        user = ()
        for control_element in await selection_res.fetchall():
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
    async with connection.begin():
        rieltor_table = db.Table("rieltor_data", metadata, autoload=True)
        rieltor_query = select(rieltor_table).where(str(rieltor_table.c.rieltor_id) == (callback_data['data']))
        rieltor_result = await connection.execute(rieltor_query)
        row = await rieltor_result.fetchone()
        markers = json.loads(row[-8])
        new_building = ''
        if 'newhouse' in markers:
            new_building = markers['newhouse']
    announcements = check_id_form2(callback_query.from_user.id)
    async with connection.begin():
        control_table = db.Table("control_data", metadata, autoload=True)
        control_query = select(control_table).where(str(control_table.c.user_id) == str(callback_query.from_user.id))
        control_res = await connection.execute(control_query)
        user = ()
        for user_row in await control_res.fetchall():
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
            error = InlineKeyboardButton(text="Помилка/Поскаржитись", callback_data=cb_inline.new(action="error", data=row[-3]))
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
            mar = InlineKeyboardMarkup(row_width=2).add(details, error, phone_num, change, stop,
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
        error = InlineKeyboardButton(text="Помилка/Поскаржитись", callback_data=cb_inline.new(action="error", data=row[-3]))
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
        mar = InlineKeyboardMarkup(row_width=2).add(details, error, phone_num, change, stop,
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
    async with connection.begin():
        rieltor_table = db.Table("rieltor_data", metadata, autoload=True)
        rieltor_query = select(rieltor_table).where(str(rieltor_table.c.rieltor_id) == str(callback_data['data']))
        rieltor_result = await connection.execute(rieltor_query)
        rieltor_element = await rieltor_result.fetchone()
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
    complaints = InlineKeyboardButton(text="Скарги", callback_data=cb_inline.new(action="complaints_show", data=callback_data['data']))
    back = InlineKeyboardButton(text="Назад🔙", callback_data=cb_inline.new(action="back_text_ann", data=
    callback_data['data']))
    mar = InlineKeyboardMarkup(row_width=2).add(fav, res_complex, complaints, back)
    await bot.edit_message_text(chat_id=callback_query.from_user.id, message_id=callback_query.message.message_id,
                                text="Детальніше", reply_markup=mar)


@dp.callback_query_handler(cb_inline.filter(action="complaints_show"))
async def show_complaints(callback_query: types.CallbackQuery, callback_data):
    async with connection.begin():
        control_table = db.Table("control_data", metadata, autoload=True)
        selection_query = select(control_table)
        selection_result = await connection.execute(selection_query)
        rows_list = await selection_result.fetchall()
        rows = []
        for row_list in rows_list:
            if row_list[-4] and str(row_list[-4]) == str(callback_data['data']):
                rows.append(row_list)
    async with connection.begin():
        control_table = db.Table('control_data', metadata, autoload=True)
        selection_query = select(control_table).where(
            control_table.c.user_id == callback_query.from_user.id)
        selection_res = await connection.execute(selection_query)
        user = ()
        for control_element in await selection_res.fetchall():
            if control_element[2]:
                user = control_element
    for row in rows:
        if row[4]:
            await bot.send_message(callback_query.from_user.id, f"{callback_data['data']}\n"
                                                                f"{row[4]}\n"
                                                                f"від {callback_query.from_user.full_name} {user[2] if user != () else ''}")


@dp.callback_query_handler(cb_inline.filter(action="del_fav"))
async def del_fav(callback_query: types.CallbackQuery, callback_data):
    for media_key in list(media_id.keys()):
        if media_key == callback_data['data']:
            async with connection.begin() as transaction:
                control_table = db.Table("control_data", metadata, autoload=True)
                del_query = db.delete(control_table).where(control_table.c.favorite == media_key)
                await connection.execute(del_query)
                await transaction.commit()
    mes = await bot.send_message(callback_query.from_user.id, "Оголошення видалено з Обране")
    await asyncio.sleep(10)
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
    back = InlineKeyboardButton(text="Назад🔙", callback_data=cb_inline.new(action="back_text_ann", data=callback_data['data']))
    mar = InlineKeyboardMarkup(row_width=3).add(mess1, mess2, mess3, mess4, mess5, back)
    await bot.edit_message_text(chat_id=callback_query.from_user.id, message_id=callback_query.message.message_id,
                                text="Помилка/Поскаржитись", reply_markup=mar)


@dp.callback_query_handler(cb_inline.filter(action='complaint'))
async def send_complaint(callback_query: types.CallbackQuery, callback_data):
    serialized_data = ast.literal_eval(callback_data['data'])
    announcement_id = serialized_data[0]
    complaint = serialized_data[1]
    async with connection.begin() as transaction:
        control_table = db.Table('control_data', metadata, autoload=True)
        insertion_query = control_table.insert().values(user_id=callback_query.from_user.id,
                                                        phone_number=phone_number,
                                                        complaint=complaint,
                                                        announcement_id=announcement_id)
        await connection.execute(insertion_query)
        await transaction.commit()
    await bot.send_message(callback_query.from_user.id,
                           text=f"Оголошення {announcement_id}, скарга {complaint} успішно відправлена!")


@dp.callback_query_handler(cb_inline.filter(action='res_complex'))
async def all_flats_in_complex(callback_query: types.CallbackQuery, callback_data):
    async with connection.begin():
        rieltor_table = db.Table('rieltor_data', metadata, autoload=True)
        selection_query = select(rieltor_table)
        selection_result = await connection.execute(selection_query)
        rows = await selection_result.fetchall()
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
                error = InlineKeyboardButton(text="Помилка/Поскаржитись", callback_data=cb_inline.new(action="error", data=row[-3]))
                mar = InlineKeyboardMarkup(row_width=1).add(details, error, phone_num, share)
                for image in images:
                    if count < len(images) and count < 9:
                        if row != last_row:
                            media.attach_photo(types.InputMediaPhoto(image))
                        else:
                            announcements = check_id_form2(callback_query.from_user.id)
                            async with connection.begin():
                                control_table = db.Table("control_data", metadata, autoload=True)
                                selection_query = select(control_table).where(
                                    control_table.c.user_id == callback_query.from_user.id)
                                selection_res = await connection.execute(selection_query)
                                for control_element in await selection_res.fetchall():
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
    async with connection.begin():
        rieltor_table = db.Table('rieltor_data', metadata, autoload=True)
        rieltor_query = select(rieltor_table)
        rieltor_result = await connection.execute(rieltor_query)
        rieltor_elements = await rieltor_result.fetchall()
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
    async with connection.begin():
        control_table = db.Table('control_data', metadata, autoload=True)
        selection_query = select(control_table).where(
            control_table.c.user_id == callback_query.from_user.id)
        selection_res = await connection.execute(selection_query)
        user = ()
        for control_element in await selection_res.fetchall():
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
    complaints = InlineKeyboardButton(text="Скарги", callback_data=cb_inline.new(action="complaints_show", data=callback_data['data']))
    back = InlineKeyboardButton(text="Назад🔙",
                                callback_data=cb_inline.new(action="back_text_ann", data=callback_data['data']))
    mar = InlineKeyboardMarkup(row_width=2).add(fav, complaints, back)
    await bot.edit_message_text(chat_id=callback_query.from_user.id, message_id=callback_query.message.message_id,
                                text="Детальніше", reply_markup=mar)


async def main():
    global connection
    connection = await create_connection()
    firebase_admin.initialize_app(cred)
    await open_rieltor_data()
    create_db_control()
    executor.start_polling(dp, skip_updates=True)


if __name__ == "__main__":
    asyncio.run(main())