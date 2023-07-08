import datetime
import runpy
import time


runpy.run_path(path_name="bot.py")
while True:
    now = datetime.datetime.now()
    if now.hour == 1 and now.minute == 0:
        runpy.run_path(path_name="bot.py")
        time.sleep((24 * 60 * 60) - 10)