import sqlite3
import validators
import requests
import time
import pandas as pd
import random
import pdfkit

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from requests.exceptions import SSLError
from bs4 import BeautifulSoup
from print_module import Print

config = pdfkit.configuration(
    wkhtmltopdf=r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe"
)


class BcptVcbsService:

    REPORT_TYPES = [
        # "Company Research",
        "Sector Reports",
        "Market Commentary",
        "Futures",
        "Economics",
        "Bond Report",
    ]

    REPORT_CODES = [ #"BCDN",
                    "BCN",
                    "BCTT", 
                    "BCCKPS", 
                    "BCVM", 
                    "BCTP"]

    LINK_VI_PAGE = "https://vcbs.com.vn/trung-tam-phan-tich"
    LINK_EN_PAGE = "https://vcbs.com.vn/en/analysis-center"

    LINKS_VI = [
        "https://vcbs.com.vn/api/v1/ttpt-reports?category_code=BCDN&locale=vi",  # Company Research
        "https://vcbs.com.vn/api/v1/ttpt-reports?category_code=BCN&locale=vi",  # Sector Reports
        "https://vcbs.com.vn/api/v1/ttpt-reports?category_code=BCTT&locale=vi",  # Market Commentary
        "https://vcbs.com.vn/api/v1/ttpt-reports?category_code=BCCKPS&locale=vi",  # Futures
        "https://vcbs.com.vn/api/v1/ttpt-reports?category_code=BCVM&locale=vi",  # Economics
        "https://vcbs.com.vn/api/v1/ttpt-reports?category_code=BCTP&locale=vi",  # Bond Report
    ]

    LINKS_EN = [
        "https://vcbs.com.vn/api/v1/ttpt-reports?category_code=BCDN&locale=en",  # Company Research
        "https://vcbs.com.vn/api/v1/ttpt-reports?category_code=BCN&locale=en",  # Sector Reports
        "https://vcbs.com.vn/api/v1/ttpt-reports?category_code=BCTT&locale=en",  # Market Commentary
        "https://vcbs.com.vn/api/v1/ttpt-reports?category_code=BCCKPS&locale=en",  # Futures
        "https://vcbs.com.vn/api/v1/ttpt-reports?category_code=BCVM&locale=en",  # Economics
        "https://vcbs.com.vn/api/v1/ttpt-reports?category_code=BCTP&locale=en",  # Bond Report
    ]

    LANGUAGE_LIST = ["VI", "EN"]

    BASE_URL = "https://vcbs.com.vn"

    @staticmethod
    def save_alternate_pdf(content, headline):
        """Convert PDF file"""
        print("Saving alternate PDF...")
        for img_tag in content.find_all("img"):
            img_tag.decompose()

        content_str = str(content)
        html_str = (
            """<html>
                    <head>
                    <title>Title of the document</title>
                    <style>
                        html * {
                        font-family: Arial, Helvetica, sans-serif;
                        }
                    </style>
                    </head>
                    <body>"""
            + content_str.replace("font-size: 14pt;", "font-size: 12pt;")
            + """</body>
                </html>"""
        )

        r = pdfkit.PDFKit(
            html_str,
            options={
                "encoding": "UTF-8",
                "enable-local-file-access": None,
                "margin-left": "40mm",
                "margin-right": "40mm",
                "quiet": "",
            },
            configuration=config,
            type_="string",
        )
        pdf = r.to_pdf()
        with open("./bcpt_pdf/vcbs/metadata.pdf", "wb") as f:
            # with open(f"./bcpt_pdf/vcbs/{headline}.pdf", "wb") as f:
            f.write(pdf)
        Print.success("PDF downloaded successfully!")

    @staticmethod
    def download_pdf(cls, download_link, content, headline):
        # Check valid url
        if validators.url(download_link):
            response = requests.get(download_link)
            if response.status_code == 200:
                # with open(f"./bcpt_pdf/vcbs/{headline}.pdf", "wb") as f:
                with open(f"./bcpt_pdf/vcbs/metadata.pdf", "wb") as f:
                    f.write(response.content)
                Print.success("PDF downloaded successfully!")
            else:
                Print.warning("PDF Expired!")
                (
                    cls.save_alternate_pdf(content, headline)
                    if content
                    else print("No content")
                )
        else:
            Print.warning("Invalid URL!")
            (
                cls.save_alternate_pdf(content, headline)
                if content
                else print("No content")
            )

    @staticmethod
    def insert_data(cursor, data, conn, retries=3):
        """Insert data into the SQLite database with retries."""
        date_str = pd.Timestamp(data["date"]).strftime("%Y-%m-%d %H:%M:%S")
        insert_query = f"""
            INSERT INTO reports (source, ticker, date, reportType, recommendation, headline, content, analyst, language, linkWeb, linkDrive)
            VALUES ('{data["source"]}', '{data["ticker"]}', '{date_str}', '{data["reportType"]}', '{data["recommendation"]}', '{data["headline"]}', '{data["content"]}', '{data["analyst"]}', '{data["language"]}', '{data["linkWeb"]}', '{data["linkDrive"]}')
        """
        while retries > 0:
            try:
                cursor.execute(insert_query)
                conn.commit()
                Print.success("Data inserted SQLite3 successfully!")
                break
            except Exception as e:
                Print.error(f"Error inserting {data['headline']}: {e}")
                retries -= 1
                time.sleep(random.randint(1, 3))

    @classmethod
    def crawl_bcpt_vcbs(cls, cursor, conn):
        """Main method to crawl reports and insert data into the database."""

        options = webdriver.ChromeOptions()
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        options.add_argument("--log-level=3")  # Suppress logs
        options.add_argument("--disable-logging")  # Disable logging
        options.add_argument("--silent")  # Silent mode
        options.add_argument("headless")
        options.add_argument("window-size=1920x1080")
        options.add_argument("disable-gpu")
        service = Service(executable_path=ChromeDriverManager().install())
        driver = webdriver.Chrome(options=options, service=service)
        # driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(30)
        print("Logging in VCBS...")
        driver.get(
            "https://trading.vcbs.com.vn/SSOServer/Account/Login?returnUrl=https%3a%2f%2ftrading.vcbs.com.vn%2fSSOServer%2fOAuth%2fAuth%3fresponse_type%3dcode%26client_id%3dvcbswebsite%26scope%3dOnline-Read%2bOnline-Write%26redirect_uri%3dhttps%3a%252F%252Fvcbs.com.vn%252Flogin-sso"
        )

        account_input = driver.find_element(By.CSS_SELECTOR, "input[id='Username']")
        account_input.clear()
        account_input.send_keys("009C271960")

        password_input = driver.find_element(By.CSS_SELECTOR, "input[id='Password']")
        password_input.clear()
        password_input.send_keys("Asd123456!")

        login_button = driver.find_element(By.CSS_SELECTOR, ".login_button")
        login_button.click()

        # Wait for the div.o-notify_wrapper to be present
        try:
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "div.o-notify_wrapper")
                )
            )
            Print.success("Login VCBS successfully!")

            driver.get("https://vcbs.com.vn/trung-tam-phan-tich")

            for lang in cls.LANGUAGE_LIST:
                for idx, report_type in enumerate(cls.REPORT_TYPES):
                    link = cls.LINKS_VI[idx] if lang == "VI" else cls.LINKS_EN[idx]
                    link_page = cls.LINK_VI_PAGE if lang == "VI" else cls.LINK_EN_PAGE
                    report_code = cls.REPORT_CODES[idx]

                    try:
                        res_json = requests.get(link).json()
                        page_num = res_json["meta"]["totalPages"]
                        print(f"Crawling {lang} {report_type} reports")

                        """Navigate to each report page"""
                        page_url = f"{link_page}?code={report_code}"
                        driver.get(page_url)
                        time.sleep(random.randint(3, 5))

                        """Iterate through each page"""
                        for page in range(page_num):
                            print("--------------------")
                            print(f"Crawling page {page + 1} of {page_num}...")

                            """Navigating to the next page"""
                            try:
                                next_page_button = WebDriverWait(driver, 30).until(
                                    EC.element_to_be_clickable(
                                        (By.XPATH, f"//a[text()='{page + 1}']")
                                    )
                                )
                                next_page_button.click()
                                time.sleep(random.randint(2, 4))
                                print(f"Navigate to the page {page + 1}...")

                                reports_link = f"{link}&page={page + 1}"
                                data_raw_list = requests.get(reports_link).json()["data"]
                                time.sleep(1)

                                """Open new tab for each report"""
                                print("Opening new tab...")
                                driver.execute_script("window.open('');")
                                driver.switch_to.window(driver.window_handles[1])

                                """Get data of each report"""
                                for idx, data_raw in enumerate(data_raw_list):
                                    print(f'Crawling {data_raw["name"]}...')

                                    """Get linkWeb"""
                                    download_url = f"https://vcbs.com.vn/bao-cao-phan-tich/{data_raw['id']}?login=true"
                                    try:
                                        driver.get(download_url)
                                        WebDriverWait(driver, 30).until(
                                            EC.url_changes(driver.current_url)
                                        )
                                        linkWeb = driver.current_url
                                        time.sleep(random.randint(2, 4))

                                        ticker = (
                                            data_raw["stockSymbol"]
                                            if report_type == "Company Research"
                                            else None
                                        )
                                        date = (
                                            pd.to_datetime(data_raw["createdAt"])
                                            .tz_localize(None)
                                            .strftime("%Y-%m-%d %H:%M:%S")
                                        )
                                        # recommendation = data_raw["name"].lower() if report_type == "Company Research" else None
                                        headline = data_raw["name"]
                                        content_html = BeautifulSoup(
                                            data_raw["description"], "html.parser"
                                        )
                                        content = content_html.get_text()

                                        data = {
                                            "source": "vcbs",
                                            "ticker": ticker,
                                            "date": date,
                                            "reportType": report_type,
                                            "recommendation": None,
                                            "headline": headline,
                                            "content": content,
                                            "analyst": None,
                                            "language": lang,
                                            "linkWeb": linkWeb,
                                            "linkDrive": None,
                                        }

                                        """Insert and download PDF"""
                                        cls.insert_data(cursor, data, conn)
                                        cls.download_pdf(
                                            cls, linkWeb, content_html, headline
                                        )

                                    except SSLError as ssl_err:
                                        Print.error(f"SSLError encountered: {ssl_err}")
                                        time.sleep(random.randint(2, 4))
                                        continue  # Skip this report and move to the next one

                                    except TimeoutException:
                                        Print.error(
                                            f"Timeout exception for {data_raw['name']}"
                                        )
                                        time.sleep(random.randint(2, 4))
                                        continue

                                """Close the tab"""
                                print("Closing tab...")
                                driver.close()
                                driver.switch_to.window(driver.window_handles[0])

                            except TimeoutException:
                                print(
                                    f"Timeout exception for page {page + 1} of {page_num}")
                                continue

                    except Exception as e:
                        print(f"Error: {e}")

        except TimeoutException:
            Print.error("Login failed")

        driver.quit()
        Print.success("Done VCBS!")


# Connect to the SQLite
conn = sqlite3.connect("reports.db")
cursor = conn.cursor()

bcpt_service = BcptVcbsService()
bcpt_service.crawl_bcpt_vcbs(cursor, conn)

conn.close()
