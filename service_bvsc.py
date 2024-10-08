import os
import time
import random
import requests
import math
import re
import pandas as pd
import sqlite3
from bs4 import BeautifulSoup as bs


class BcptBscService:
    REPORT_TYPES = [
        "Company Research",
        'Sector Reports',
        'Market Commentary',
        'Strategy',
        'Economics',
        'Bond Report'
    ]

    LINKS_VI = [
        "https://bvsc.com.vn/baocaophantich/99277?trang=1&sotin=12&culture=vi",
        'https://bvsc.com.vn/baocaophantich/99278?trang=1&sotin=12&culture=vi',
        'https://bvsc.com.vn/baocaophantich/6390?trang=1&sotin=12&culture=vi',
        'https://bvsc.com.vn/baocaophantich/6391?trang=1&sotin=12&culture=vi',
        'https://bvsc.com.vn/baocaophantich/6392?trang=1&sotin=12&culture=vi',
        'https://bvsc.com.vn/baocaophantich/99276?trang=1&sotin=12&culture=vi'
    ]

    BASE_URL = "https://bvsc.com.vn"

    @staticmethod
    def download_pdf(url, file_name):
        """Download PDF file"""
        file_name = re.sub(r'[<>:"/\\|?*\']', "", file_name).replace(" ", "_")
        if not file_name.endswith(".pdf"):
            file_name += ".pdf"

        file_path = os.path.join("./bcpt_pdf/bvsc/", file_name)

        with open(file_path, "wb") as file:
            response = requests.get(url, timeout=10)
            file.write(response.content)

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
                print(f"Error inserting {data['headline']}: {e}")
                retries -= 1
                time.sleep(2)

    @classmethod
    def crawl_bcpt_bsc(cls, cursor, conn):
        """Main method to crawl reports and insert data into the database."""
        for idx, link in enumerate(cls.LINKS_VI):
            time.sleep(1.5)
            res_json = requests.get(link).json()
            total_records = res_json["totalRecords"]
            page_num = math.ceil(total_records / 12)
            report_type = cls.REPORT_TYPES[idx]

            for page in range(page_num):
                print(f"Crawling page {page + 1} of {page_num}...")
                reports_link = link.replace("trang=1", f"trang={page + 1}")
                time.sleep(1.5)

                data_raw_list = requests.get(reports_link).json()["items"]
                for data_raw in data_raw_list:
                    time.sleep(random.uniform(1.5, 2))
                    print(f'Crawling {data_raw["name"]}...')

                    # Create metadata
                    data = {
                        "source": "bvs",
                        "ticker": (
                            data_raw["maCK"]
                            if report_type == "Company Research"
                            else None
                        ),
                        "date": pd.Timestamp(data_raw["ngayHienThi"]).tz_localize(None),
                        "reportType": report_type,
                        "recommendation": (
                            data_raw["name"].lower()
                            if report_type == "Company Research"
                            else None
                        ),
                        "headline": data_raw["name"],
                        "content": bs(
                            data_raw["description"], "html.parser"
                        ).get_text(),
                        "analyst": None,
                        "language": "VI",
                        "linkWeb": cls.BASE_URL + data_raw["url"],
                        "linkDrive": None,
                    }

                    # Download PDF file
                    download_link = f"{cls.BASE_URL}/download_attachment/{data_raw['fileInfo']['id']}?_={data_raw['id']}"
                    file_name = f"{data['headline']}"
                    cls.download_pdf(download_link, file_name)

                    # Insert data into the reports table
                    cls.insert_data(cursor, data, conn)


# Connect to the SQLite
conn = sqlite3.connect("reports.db")
cursor = conn.cursor()

bcpt_service = BcptBscService()
bcpt_service.crawl_bcpt_bsc(cursor, conn)

conn.close()
