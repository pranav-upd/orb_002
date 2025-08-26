import csv
import time
import pytz
import shutil
import logging
import traceback
from dotenv import load_dotenv
from datetime import datetime
import math
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.signals.sg_intraday_accuracy import (
    SgIntradayStockAccuracyRepository,
)

# ---------------- Load Env ---------------- #
load_dotenv()

# ---------------- Setup Logging ---------------- #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("intra-alerts")
INTRADAY_SCREENER_EMAIL = os.getenv("INTRADAY_SCREENER_EMAIL")
INTRADAY_SCREENER_PWD = os.getenv("INTRADAY_SCREENER_PWD")


# --------------------------------------------
def get_screener_run_id():
    ist_zone = pytz.timezone("Asia/Kolkata")
    time_now = datetime.now()
    ist_now = time_now.astimezone(ist_zone)

    # Round down to nearest 10th minute
    rounded_minute = math.floor(ist_now.minute / 10) * 10
    screener_run_time_dt = ist_now.replace(minute=rounded_minute, second=0, microsecond=0)
    screener_run_time = screener_run_time_dt.strftime("%Y-%m-%d")

    return screener_run_time


def read_csv_and_delete(download_dir, file_name, logger):
    csv_file = os.path.join(download_dir, file_name)
    logger.info(f"Reading {csv_file}..")
    csv_data = []
    try:
        with open(csv_file, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)
            for row in reader:
                csv_data.append(row)
        logger.info("Reading completed")
        logger.info(f"Deleting {csv_file}..")
        os.remove(csv_file)
    except FileNotFoundError:
        logger.error(f"Error: File not found at {file_name}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

    return csv_data


def wait_for_file(dir, filename, logger):
    try:
        time.sleep(10)
        path = os.path.join(dir, filename)
        if os.path.exists(path):
            logger.info("Successfully found CSV file")
            return True
        else:
            raise FileNotFoundError
    except Exception as e:
        logger.error(f"Error: {e}")
        raise Exception


# --------------------------------------------
def write_to_db(data_towrite, logger, db_session):
    run_dt = get_screener_run_id()
    sg_intraday = SgIntradayStockAccuracyRepository(db_session=db_session)
    rows_to_insert = []
    try:
        for i, data_i in enumerate(data_towrite[1:], 1):
            run_id = str(abs(hash(str(i))))
            row_dict = {
                "screener_run_id": run_id,
                "screener_date": run_dt,
                "screener_type": "Intraday Accuracy",
                "screener": data_i[0],
                "stock_name": data_i[0],
                "trade_type": "BUY" if float(data_i[1].split("\n")[1].split()[1][1:-2])>0 else "SELL",
                "ltp": data_i[1].split("\n")[0],
                "volume": data_i[2],
                "deviation_from_pivots": data_i[3],
                "sector": data_i[4],
                "percentage_change": float(data_i[1].split("\n")[1].split()[1][1:-2]),
                "change": float(data_i[1].split("\n")[1].split()[0])
            }
            rows_to_insert.append(row_dict)

        if rows_to_insert:
            sg_intraday.bulk_insert(rows_to_insert)
            logger.info("Logged all rows successfully")
        else:
            logger.info("No rows to log.")
    except Exception as e:
        logger.error(f"Error: {e}")
        raise Exception


# ---------------- Scraper ---------------- #
def get_intraday_accuracy(logger, db_session):
    logger.info("🚀 Launching browser...")

    download_dir = os.getcwd()
    file_name = "Intraday 100% Accuracy.csv"

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 20)

    try:
        # --- Login ---
        driver.get("https://intradayscreener.com/login")
        logger.info("🌐 Opened login page.")

        email_input = wait.until(EC.presence_of_element_located((By.XPATH, '//input[@type="email"]')))
        email_input.clear()
        email_input.send_keys(INTRADAY_SCREENER_EMAIL)

        password_input = wait.until(EC.presence_of_element_located((By.XPATH, '//input[@type="password"]')))
        password_input.clear()
        password_input.send_keys(INTRADAY_SCREENER_PWD)

        signin_btn = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[contains(@class,"login-btn")]')))
        driver.execute_script("arguments[0].scrollIntoView(true);", signin_btn)
        driver.execute_script("arguments[0].click();", signin_btn)
        logger.info("🔐 Login submitted.")
        time.sleep(3)

        # --- Navigate to Accuracy page ---
        driver.get("https://intradayscreener.com/scan/1111/Intraday_100%25_Accuracy")
        logger.info("📌 Opened Intraday Stock Accuracy page.")
        time.sleep(3)

        # --- Click CSV ---
        csv_element = wait.until(EC.element_to_be_clickable((By.XPATH, "//*[contains(text(),'CSV')]")))
        driver.execute_script("arguments[0].click();", csv_element)
        logger.info("📥 CSV download clicked.")

        # --- Wait for file ---
        if not wait_for_file(download_dir, file_name, logger):
            raise TimeoutError("CSV file not downloaded.")
        logger.info("✅ CSV downloaded successfully.")

    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        traceback.print_exc()

    finally:
        driver.quit()
        logger.info("🧹 Browser closed.")
        data_final = read_csv_and_delete(download_dir, file_name, logger)
        write_to_db(data_final,logger, db_session=db_session)


# ---------------- Runner ---------------- #
