import subprocess
import datetime
import time


while True:
    now = datetime.datetime.now()
    if now.hour == 1 and now.minute == 0:
        subprocess.call(['python', 'bot.py'])
        time.sleep((24 * 60 * 60) - 10)