from firebase_admin import credentials, firestore
import sqlalchemy as db
from aiogram.utils.callback_data import CallbackData

TOKEN = "6247426236:AAEQKdagFgu6Xe8f9L_Yb_cPWmFvuP8DJsA"

engine = db.create_engine("mysql+pymysql://yarikOdmen:developer70@localhost:3306/eBazaDB")
try:
    cred = credentials.Certificate("aleksandr-c0286-firebase-adminsdk-4k3sz-ebc5beaae1.json")
except Exception:
    pass
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
rows = []

start_message1 = "🏡 Вітаю! Я — єБАЗА нерухомості бот.\n\n"
"👋🏻 <b>З моєю допомогою ти зможеш:</b>\n\n"
"▫️ знаходити варіанти квартир, будинків за параметрами чи по карті;\n"
"▫️ знаходити покупців чи орендарів на свою нерухомість;\n"
"▫️ швидко та оперативно отримувати інформацію про нові об'єкти;\n"
"▫️ одночасно можеш додати до трьох оголошень в кожну рубрику,"
" а якщо буде потрібно більше оголошень — ділись посиланням на бот "
"з друзями і отримуй додаткові оголошення."

start_message2 = "❗️ Будь-ласка, дотримуйся правил!\n"
"Заборонено розміщувати “фейкові” оголошення.\n"
"Якщо продаж/оренда твого об'єкта вже неактуальна — не забудь "
"відправити в архів.\n"
"Якщо ти ріелтор — розміщуй лише ті оголошення, де в тебе є "
"договір з власником.\n"
"За порушення правил — можливий бан!"
