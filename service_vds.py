import time
import requests
import re
import sqlite3
import validators
import io
import pdfplumber
import random
import pandas as pd

from dateutil import parser
import pdfkit

config = pdfkit.configuration(
    wkhtmltopdf=r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe"
)

class BcptVdsService:

    REPORT_TYPE = [
        "Company Research",
        "Flashnote",
        "Strategy",
        "Strategy",
        "Theme",
        "Market Commentary",
        "Brokerage Corner",
        "Investment Opportunities",
    ]

    REPORT_GROUP_ID = [5, 6, 3, 4, 7, 1, 2, 99]

    LANGUAGE = ["VI", "EN"]

    API_URL = "https://vdsc.com.vn/data/api/app/management-report/public-paged"

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
    def download_pdf(download_link, get_text=False, download=True):
        # Check valid url
        if validators.url(download_link):
            response = requests.get(download_link)
            if response.status_code == 200:
                if download:
                    with open("./bcpt_pdf/vds/metadata.pdf", "wb") as f:
                        f.write(response.content)
                file_downloaded = io.BytesIO(response.content)
                try:
                    with pdfplumber.open(file_downloaded) as pdf:
                        text = pdf.pages[0].extract_text()
                        if get_text:
                            return " ".join(text.split("\n")[:10])
                except Exception as e:
                    print(f"Error processing PDF: {e}")
                    return None
            else:
                print("PDF Expired")
                return None
        else:
            print("Invalid URL")
            return None

    @staticmethod
    def transform_data(cls, lang, item, report_type):
        """Transform raw data to the desired format."""
        if lang == "VI":
            headline = item["title"] if item["title"] else None
            link_web = (
                f"https://www.vdsc.com.vn/data/api/app/file-storage/{item['file']}"
                if item["file"]
                else None
            )
        else:
            headline = item["titleEn"] if item["titleEn"] else None
            link_web = (
                f"https://www.vdsc.com.vn/data/api/app/file-storage/{item['fileEn']}"
                if item["fileEn"]
                else None
            )
        date = parser.parse(item["publishDate"]).strftime("%Y-%m-%d %H:%M:%S")

        """Get ticker and recommendation"""
        if report_type == "Company Research":
            match = re.findall(r"\b[A-Z0-9]{3}\b", headline)
            ticker = match[0] if match else None
            recomendation = (
                cls.download_pdf(link_web, get_text=True, download=False)
                if link_web
                else None
            )
        else:
            ticker = None
            recomendation = None

        data = {
            "source": "vds",
            "ticker": ticker,
            "date": date,
            "reportType": report_type,
            "recommendation": recomendation,
            "headline": headline,
            "content": None,
            "analyst": None,
            "language": lang,
            "linkWeb": link_web,
            "linkDrive": None,
        }
        #
        return data

    @staticmethod
    def get_data(cls, lang, group_id, report_type, cursor, conn):
        """Get total records"""
        api_url = f"{cls.API_URL}?groupId={group_id}&language={lang.lower()}"
        res = requests.get(api_url).json()
        total_record = res["totalCount"]

        """Check number of records"""
        if total_record == 0:
            print(f"No data for {report_type} in {lang} language.")
            return

        if total_record > 1000:
            skip_count = 0
            while skip_count < total_record:
                url = f"{api_url}&sorting=publishDate%20desc&skipCount={skip_count}&maxResultCount=1000"
                res_json = requests.get(url).json()
                items = res_json["items"]
                skip_count += 1000
                for item in items:
                    """Tranform data"""
                    data = cls.transform_data(cls, lang, item, report_type)

                    """Download and insert data"""
                    # cls.download_pdf(data["linkWeb"], download=True)
                    # cls.insert_data(cursor, data, conn)

        else:
            time.sleep(1)
            url = f"{api_url}&sorting=publishDate%20desc&maxResultCount={total_record}"
            res_json = requests.get(url).json()
            items = res_json["items"]
            for item in items:
                """Tranform data"""
                data = cls.transform_data(cls, lang, item, report_type)

                """Download and insert data"""
                # cls.download_pdf(data["linkWeb"], download=True)
                # cls.insert_data(cursor, data, conn)

    @classmethod
    def crawl_bcpt_vds(cls, cursor, conn):

        for lang in cls.LANGUAGE:
            (
                print("Crawling VI reports...")
                if lang == "VI"
                else print("Crawling EN reports...")
            )
            for idx, group_id in enumerate(cls.REPORT_GROUP_ID):
                report_type = cls.REPORT_TYPE[idx]
                print(f"Crawling {lang} {report_type} ...")

                """Download and insert data"""
                cls.get_data(cls, lang, group_id, report_type, cursor, conn)

        print("Done VDS")


# Connect to the SQLite
conn = sqlite3.connect("reports.db")
cursor = conn.cursor()

bcpt_service = BcptVdsService()
bcpt_service.crawl_bcpt_vds(cursor, conn)

conn.close()
