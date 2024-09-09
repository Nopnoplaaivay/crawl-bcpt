import time
import requests
import re
import sqlite3
import random
import pandas as pd
import pdfkit
import base64

from print_module import Print
from bs4 import BeautifulSoup

config = pdfkit.configuration(
    wkhtmltopdf=r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe"
)


class BcptVdsAPService:

    REPORT_TYPE = ["Analyst Pinboard"]

    LANGUAGE = ["VI", "EN"]

    API_URL = "https://vdsc.com.vn/data/api/app/management-market-commentary/public-paged?sorting=publishDate%20desc"

    @staticmethod
    def insert_data(cursor, data, conn):
        """Insert data into the SQLite database with retries."""
        date_str = pd.Timestamp(data["date"]).strftime("%Y-%m-%d %H:%M:%S")
        insert_query = f"""
            INSERT INTO reports (source, ticker, date, reportType, recommendation, headline, content, analyst, language, linkWeb, linkDrive)
            VALUES ('{data["source"]}', '{data["ticker"]}', '{date_str}', '{data["reportType"]}', '{data["recommendation"]}', '{data["headline"]}', '{data["content"]}', '{data["analyst"]}', '{data["language"]}', '{data["linkWeb"]}', '{data["linkDrive"]}')
        """
        try:
            cursor.execute(insert_query)
            conn.commit()
            Print.success(f"Data inserted successfully")

        except Exception as e:
            Print.error(f"Error inserting {data['headline']}: {e}")

    @staticmethod
    def download_pdf(content):
        """Convert HTML to PDF"""
        base_url_vdsc = "https://vdsc.com.vn"

        for img_tag in content.find_all("img"):
            src = img_tag.get("src")
            if img_tag.has_attr("loading") and img_tag["loading"] == "lazy":
                del img_tag["loading"]
            
            if src.startswith("/data/api/app/file-storage"):
                img_url = f"{base_url_vdsc}{src}"
                Print.warning(f"Add base_url after data/api URL: {img_url}")
                img_tag["src"] = img_url

            elif src.startswith("./assets"):
                img_tag["src"] = f"{base_url_vdsc}{src[1:]}"
                Print.warning(f"Add base_url before ./asset URL: {img_tag['src']}")
            else:
                Print.warning(f"Already valid image URL: {img_tag['src']}")
           

        content_str = str(content)
        html_str = f"""
        <html>
            <head>
                <title></title>
                <meta charset="UTF-8">
                <style>
                    html * {{
                        font-family: Arial, Helvetica, sans-serif;
                    }}
                </style>
            </head>
            <body>
                {content_str.replace("font-size: 14pt;", "font-size: 12pt;")}
            </body>
        </html>
        """
        print(html_str)

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
        with open(f"./bcpt_pdf/vds_ap/metadata.pdf", "wb") as f:
            f.write(pdf)
        Print.success(f"PDF saved!")

    @classmethod
    def crawl_bcpt_vds_ap(cls, cursor, conn):

        for lang in cls.LANGUAGE:
            print("Crawling VI reports...") if lang == "VI" else print("Crawling EN reports...")

            report_type = cls.REPORT_TYPE[0]
            print(f"Crawling {lang} {report_type} ...")

            """Get total records"""
            api_url = f"{cls.API_URL}&language={lang.lower()}"
            res = requests.get(api_url).json()
            total_record = res["totalCount"]

            skip_count = 0
            while skip_count < total_record:
                url = f"{api_url}&skipCount={skip_count}&maxResultCount=20"
                res_json = requests.get(url).json()
                items = res_json["items"]
                skip_count += 20
                for item in items:
                    """Get ticker"""
                    tickers = (
                        ",".join([elem["name"] for elem in item["stockSymbol"]])
                        if item["stockSymbol"]
                        else None
                    )

                    """Transform raw data to the desired format"""
                    if lang == "VI":
                        headline = item["title"] if item["title"] else None
                        link_web = (
                            f"https://vdsc.com.vn/trung-tam-phan-tich/nhan-dinh-hang-ngay/{item['slug']}-d{item['id']}"
                            if item["slug"]
                            else None
                        )
                        content_html = (
                            BeautifulSoup(item["content"], "html.parser")
                            if item["content"]
                            else None
                        )
                        content = content_html.get_text() if content_html else None
                        analyst = item["author"] if item["author"] else None
                    else:
                        headline = item["titleEn"] if item["titleEn"] else None
                        link_web = (
                            f"https://vdsc.com.vn/en/research/daily-recommendations/{item['slugEn']}-d{item['id']}"
                            if item["slugEn"]
                            else None
                        )
                        content_html = (
                            BeautifulSoup(item["content"], "html.parser")
                            if item["contentEn"]
                            else None
                        )
                        content = content_html.get_text() if content_html else None
                        analyst = item["authorEn"] if item["authorEn"] else None

                    date = (
                        pd.to_datetime(item["publishDate"])
                        .tz_localize(None)
                        .strftime("%Y-%m-%d %H:%M:%S")
                    )

                    print(content_html)

                    data = {
                        "source": "vds",
                        "ticker": tickers,
                        "date": date,
                        "reportType": report_type,
                        "recommendation": None,
                        "headline": headline,
                        "content": content,
                        "analyst": analyst,
                        "language": lang,
                        "linkWeb": link_web,
                        "linkDrive": None,
                    }

                    """Download and insert data"""
                    time.sleep(random.randint(1, 2))
                    # cls.download_pdf(content_html)
                    # cls.insert_data(cursor, data, conn)

        print("Done VDS")


# Connect to the SQLite
conn = sqlite3.connect("reports.db")
cursor = conn.cursor()

bcpt_service = BcptVdsAPService()
bcpt_service.crawl_bcpt_vds_ap(cursor, conn)

conn.close()
