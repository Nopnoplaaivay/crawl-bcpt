import os
import requests
import time
import pandas as pd
import random
import os
import re
import pdfkit
import sqlite3
import validators

config = pdfkit.configuration(
    wkhtmltopdf=r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe"
)

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup


class BcptVndService:
    REPORT_TYPES = [
        # "Company Research",
        # "Sector Reports",
        # "Economics",
        "Strategy",
        "Forex Report",
        "Real Estate Report",
        "Bond Report",
    ]

    LINKS_VI = [
        # "https://www.vndirect.com.vn/category/bao-cao-phan-tich-dn/", # Company Research
        # "https://www.vndirect.com.vn/category/bao-cao-nganh/", # Sector Reports
        # "https://www.vndirect.com.vn/category/bao-cao-vi-mo-vi-chuyen-de-su-kien/", # Economics
        "https://www.vndirect.com.vn/category/bao-cao-chien-luoc/",  # Strategy
        "https://www.vndirect.com.vn/category/thi-truong-tien-te/",  # Forex Report
        "https://www.vndirect.com.vn/category/bao-cao-nganh/thi-truong-bat-dong-san/",  # Real Estate Report
        "https://www.vndirect.com.vn/category/bao-cao-trai-phieu/",  # Bond Report
    ]

    @staticmethod
    def save_alternate_pdf(driver):
        """Convert PDF file"""
        soup = BeautifulSoup(driver.page_source, "html.parser")
        content = soup.find("section")
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

        print("Saving PDF...")
        pdf = r.to_pdf()
        with open("./bcpt_pdf/vnd/metadata.pdf", "wb") as f:
            f.write(pdf)
        print("PDF saved!")

    @staticmethod
    def download_pdf(cls, single_content, driver):
        print("Downloading PDF...")
        download_element = single_content.find_elements(By.CSS_SELECTOR, "a")
        if download_element:
            download_url = download_element[-1].get_attribute("href")
            # Check valid url
            if validators.url(download_url):
                response = requests.get(download_url)
                if response.status_code == 200:
                    with open("./bcpt_pdf/vnd/metadata.pdf", "wb") as f:
                        f.write(response.content)
                else:
                    print("PDF Expired")
                    cls.save_alternate_pdf(driver)
            else:
                print("Invalid URL")
                cls.save_alternate_pdf(driver)
        else:
            print("No URL provided")
            cls.save_alternate_pdf(driver)

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
                print("Data inserted successfully!")
                break
            except Exception as e:
                print(f"Error: {e}")
                retries -= 1
                time.sleep(2)

    @classmethod
    def crawl_bcpt_vnd(cls, cursor, conn):
        """Setup Chrome driver"""
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
        driver.set_page_load_timeout(30)

        """Iterate through each report type"""
        for idx, link in enumerate(cls.LINKS_VI):
            driver.get(link)
            time.sleep(2)

            """Get page numbers"""
            page_numbers = driver.find_elements(By.CSS_SELECTOR, ".page-numbers")
            page_numbers = [p.text for p in page_numbers]
            page_num = max([int(p) for p in page_numbers if p.isdigit()], default=1)

            # for page in range(58,59):
            for page in range(page_num):
                """Move to page"""
                print(f"Crawling page {page + 1} of {page_num}...")
                page_url = f"{link}page/{page}/"
                driver.get(page_url)
                contents = driver.find_elements(
                    By.CSS_SELECTOR, ".news-item .news-infor [href]"
                )

                for c in contents:
                    """Move to 2nd tab"""
                    link_page = c.get_attribute("href")
                    driver.execute_script(
                        "window.open(arguments[0], '_blank');", link_page
                    )
                    driver.switch_to.window(driver.window_handles[-1])
                    time.sleep(random.randint(1, 2))

                    single_content = driver.find_element(
                        By.CSS_SELECTOR, ".single-content.content-text"
                    )
                    content = single_content.text.strip().replace("\n", " ")
                    date_raw = driver.find_element(By.CSS_SELECTOR, ".fontita.font16")
                    date_time = pd.to_datetime(
                        date_raw.text.split(" ")[0], format="%d/%m/%Y"
                    )
                    date = date_time.strftime("%Y-%m-%d %H:%M:%S")
                    headline = driver.find_element(
                        By.CSS_SELECTOR, ".section-title.font700.font35"
                    ).text.strip()
                    report_type = cls.REPORT_TYPES[idx]

                    # Get ticker and recommendation
                    print(f"Crawling {report_type}...")
                    if report_type == "Company Research":
                        match = re.search(r"^[A-Z0-9]{1,5}", headline)
                        ticker = match.group(0) if match else headline.split(" ")[0]
                        print(f"Ticker: {ticker}")
                        try:
                            table = single_content.find_element(
                                By.CSS_SELECTOR, "table"
                            )
                            rows = table.find_elements(By.CSS_SELECTOR, "tr")
                            headers = rows[0].find_elements(By.CSS_SELECTOR, "td")
                            data = rows[1].find_elements(By.CSS_SELECTOR, "td")
                            khuyen_nghi_col_index = next(
                                (
                                    i
                                    for i, h in enumerate(headers)
                                    if "Khuyến nghị" in h.text
                                ),
                                None,
                            )
                            recommendation = data[khuyen_nghi_col_index].text
                        except:
                            recommendation = None
                            print("No recommendation found")
                    else:
                        ticker = None
                        recommendation = None

                    # Create metadata
                    data = {
                        "source": "vnd",
                        "ticker": ticker,
                        "date": date,
                        "reportType": report_type,
                        "recommendation": recommendation,
                        "headline": headline,
                        "content": content,
                        "analyst": None,
                        "language": "VI",
                        "linkWeb": link_page,
                        "linkDrive": None,
                    }

                    # Download PDF file
                    cls.download_pdf(cls, single_content, driver)

                    # Insert data into SQLite
                    cls.insert_data(cursor, data, conn)

                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])

        driver.quit()
        print("Crawling VND Reports completed!")


# Connect to the SQLite
conn = sqlite3.connect("reports.db", timeout=10)
cursor = conn.cursor()

vnd_service = BcptVndService()
vnd_service.crawl_bcpt_vnd(cursor, conn)

conn.close()
