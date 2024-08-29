import os
import time
import random
import requests
import math
import re
import pandas as pd
import sqlite3
import validators
import pdfkit

config = pdfkit.configuration(
    wkhtmltopdf=r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe"
)

from datetime import datetime
from bs4 import BeautifulSoup


class BcptVscsService:
    REPORT_TYPES = [
        "Company Research",
        "Sector Reports",
        "Market Commentary",
        "Economics",
        "Strategy",
        "Fixed Income",
    ]

    BASE_URL = "https://www.vietcap.com.vn"
    API_URL = "https://www.vietcap.com.vn/api/cms-service/v1/page/analysis?is-all=false&page=0&size=10&direction=DESC&sortBy=date"

    PAGE_IDS_VI = [144, 143, 141, 146, 145, 147]
    PAGE_IDS_EN = [230, 232, 229, 228, 224, 227]
    LANGUAGE_LIST = ["VI", "EN"]

    @staticmethod
    def save_alternate_pdf(content):
        """Convert PDF file"""
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
        with open("./bcpt_pdf/vcsc/metadata.pdf", "wb") as f:
            f.write(pdf)
        print("PDF saved!")

    @staticmethod
    def download_pdf(cls, download_link, content):
        print("Downloading PDF...")
        # Check valid url
        if validators.url(download_link):
            response = requests.get(download_link)
            if response.status_code == 200:
                with open("./bcpt_pdf/vcsc/metadata.pdf", "wb") as f:
                    f.write(response.content)
            else:
                print("PDF Expired")
                cls.save_alternate_pdf(content) if content else print("No content")
        else:
            print("Invalid URL")
            cls.save_alternate_pdf(content) if content else print("No content")

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
                time.sleep(random.randint(1, 3))

    @staticmethod
    def get_data(cls, lang, page_id, report_type, cursor, conn):
        """Get page numbers"""
        lang_idx = 1 if lang == "VI" else 2
        api_url = f"{cls.API_URL}&language={lang_idx}&page-ids={page_id}"
        res_json = requests.get(api_url).json()
        page_num = res_json["data"]["pagingGeneralResponses"]["totalPages"]

        '''Iterate through pages and get data'''
        for page in range(page_num):
            print(f"Crawling page {page + 1} of {page_num}...")
            url = api_url.replace(f"page=0", f"page={page}")
            res_json = requests.get(url).json()
            content = res_json["data"]["pagingGeneralResponses"]["content"]
            for data_raw in content:
                # data = cls.tranform_data(cls.BASE_URL, stock, lang, report_type)
                """Transform raw data to the desired format."""
                content = (
                    BeautifulSoup(data_raw["detail"], "html.parser")
                    if data_raw["detail"]
                    else None
                )
                detail = content.get_text() if content else None
                date = datetime.strptime(
                    data_raw["date"], "%Y-%m-%dT%H:%M:%S"
                ).strftime("%Y-%m-%d %H:%M:%S")
                ticker = (
                    data_raw["companyInfo"]["code"]
                    if report_type == "Company Research"
                    else None
                )

                reccomendation = (
                    data_raw["name"] if report_type == "Company Research" else None
                )
                headline = data_raw["name"]
                linkWeb = f"{cls.BASE_URL}/{lang.lower()}/{data_raw['link']}"

                data = {
                    "source": "vcs",
                    "ticker": ticker,
                    "date": date,
                    "reportType": report_type,
                    "recommendation": reccomendation,
                    "headline": headline,
                    "content": detail,
                    "analyst": None,
                    "language": lang,
                    "linkWeb": linkWeb,
                    "linkDrive": None,
                }

                '''Download and insert data'''
                # cls.download_pdf(cls, data_raw["file"], content)
                cls.insert_data(cursor, data, conn)
                print(f"Crawling {data['headline']}...")
                time.sleep(random.randint(1, 2))

    @classmethod
    def crawl_bcpt_vscs(cls, cursor, conn):
        """Main method to crawl reports and insert data into the database."""
        for lang in cls.LANGUAGE_LIST:
            if lang == "VI":
                for idx, page_id in enumerate(cls.PAGE_IDS_VI):
                    report_type = cls.REPORT_TYPES[idx]

                    '''Download and insert data'''
                    cls.get_data(cls, lang, page_id, report_type, cursor, conn)
            else:
                for idx, page_id in enumerate(cls.PAGE_IDS_EN):
                    report_type = cls.REPORT_TYPES[idx]

                    '''Download and insert data'''
                    cls.get_data(cls, lang, page_id, report_type, cursor, conn)


# Connect to the SQLite
conn = sqlite3.connect("reports.db")
cursor = conn.cursor()

bcpt_service = BcptVscsService()
bcpt_service.crawl_bcpt_vscs(cursor, conn)

conn.close()
