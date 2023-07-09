import schedule
import subprocess

def restart_bot():
    subprocess.call(["python", "bot.py"])

# Schedule bot restart every day at 01:00
schedule.every().day.at("01:00").do(restart_bot)

while True:
    schedule.run_pending()