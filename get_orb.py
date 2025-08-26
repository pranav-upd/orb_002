from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
import time
import os
from dotenv import load_dotenv
import logging

from algo_scripts.algotrade.scripts.trade_utils.time_manager import (
    get_current_ist_time_as_str,
    get_screener_run_id,
    get_ist_time
)

from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.signals.sg_orb_screener import SgOrbRepository

load_dotenv()

INTRADAY_SCREENER_EMAIL = os.getenv("INTRADAY_SCREENER_EMAIL")
INTRADAY_SCREENER_PWD = os.getenv("INTRADAY_SCREENER_PWD")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("GET_ORB_SCREENER")

def get_intraday_screener_orb_bis(db_session):
    date_time = get_ist_time()[1]
    run_id = get_screener_run_id()
    options = Options()
    options.add_argument("--headless=new")   # headless mode
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=options)
    driver.set_window_size(1920, 1080)
    driver.get("https://intradayscreener.com/login")
    records_list = []

    try:
        # --- Step 1: Login ---
        email_field = WebDriverWait(driver, 30).until(
            EC.visibility_of_element_located(
                (
                    By.XPATH,
                    "/html/body/app-root/div/app-login-layout/div/app-signin/div/div[1]/div/div[2]/div/div/div/div/div/div/form/div[1]/input",
                )
            )
        )
        email_field.send_keys(INTRADAY_SCREENER_EMAIL)

        password_field = driver.find_element(
            By.XPATH,
            "/html/body/app-root/div/app-login-layout/div/app-signin/div/div[1]/div/div[2]/div/div/div/div/div/div/form/div[2]/div/input",
        )
        password_field.send_keys(INTRADAY_SCREENER_PWD)

        login_button = driver.find_element(
            By.XPATH,
            "/html/body/app-root/div/app-login-layout/div/app-signin/div/div[1]/div/div[2]/div/div/div/div/div/div/form/button",
        )
        login_button.click()

        # Close popup if it exists
        try:
            popup_chart_label = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable(
                    (By.XPATH, '//*[@id="whatsnewModal"]/div/div/div[1]/button/span')
                )
            )
            popup_chart_label.click()
        except TimeoutException:
            logger.error("Unable to Retrieve Element: Chart Pop-up")

        # --- Step 2: Navigate to ORB ---
        intraday_menu = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.ID, "intradayNavbarDropdown"))
        )
        intraday_menu.click()

        orb_link = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable(
                (By.XPATH, '//*[@id="navbarSupportedContent"]/li[2]/div/a[8]')
            )
        )
        orb_link.click()
        time.sleep(2)

        tab_xpaths = [
            ('//*[@id="pills-home-15min"]', "ORB+PRB 15"),
            ('//*[@id="pills-home-15minp"]', "ORB 15"),
            ('//*[@id="pills-home-30min"]', "ORB+PRB 30"),
            ('//*[@id="pills-home-30minp"]', "ORB 30"),
            ('//*[@id="pills-home-45min"]', "ORB+PRB 45"),
            ('//*[@id="pills-home-45minp"]', "ORB 45"),
            ('//*[@id="pills-home-60min"]', "ORB+PRB 60"),
            ('//*[@id="pills-home-60minp"]', "ORB 60"),
        ]

        all_records = [
            "symbol",
            "ltp",
            "orb_price",
            "orb_time",
            "deviation",
            "range",
            "run_id",
            "last_updated",
            "stock_type",
            "strategy",
            "trade_type",
            "time_range_orb",
            "is_prb_present",
        ]

        symbols = set()

        for tab_xpath, option in tab_xpaths:
            tab_element = WebDriverWait(driver, 30).until(
                EC.element_to_be_clickable((By.XPATH, tab_xpath))
            )
            driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center'});", tab_element
            )
            driver.execute_script("arguments[0].click();", tab_element)
            time.sleep(2)

            # --- Loop through paginated table ---
            while True:
                rows = WebDriverWait(driver, 30).until(
                    EC.presence_of_all_elements_located((By.TAG_NAME, "mat-row"))
                )

                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "mat-cell")
                    row_data = [cell.text for cell in cells]

                    if not row_data or row_data[0] in symbols:
                        continue

                    symbols.add(row_data[0])
                    row_data += [
                        run_id,
                        date_time,
                        "CASH",
                        option,
                        "INTRADAY",
                        option[-2:],
                        True if "PRB" in option else False,
                    ]
                    records_list.append(
                        {all_records[x]: row_data[x] for x in range(len(row_data))}
                    )

                try:
                    next_btn = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located(
                            (
                                By.XPATH,
                                '//button[contains(@class, "mat-mdc-paginator-navigation-next")]',
                            )
                        )
                    )

                    if next_btn.get_attribute("disabled") or "disabled" in next_btn.get_attribute("class"):
                        logger.info("Reached last page.")
                        break

                    driver.execute_script(
                        "arguments[0].scrollIntoView({block: 'center'});", next_btn
                    )
                    driver.execute_script("arguments[0].click();", next_btn)

                    # ✅ wait for table reload (first row goes stale)
                    WebDriverWait(driver, 10).until(EC.staleness_of(rows[0]))
                    time.sleep(1)

                except TimeoutException:
                    logger.error("No paginator button found.")
                    break

    except TimeoutException:
        logger.error("An element failed to load in the expected time.")

    finally:
        run_completed_time = get_current_ist_time_as_str()
        logger.info("Run Completed " + run_completed_time)
        driver.quit()
        repo = SgOrbRepository(db_session)
        for record in records_list:
            repo.insert(record)
        logger.info("Inserted all the records successfully!")


if __name__ == "__main__":
    pass
    #get_intraday_screener_orb_bis(db_session)
