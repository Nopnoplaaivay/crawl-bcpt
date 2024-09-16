import requests
import pandas as pd
import sqlite3
import validators
import pdfkit
import logging

from print_module import Print
from bs4 import BeautifulSoup

config = pdfkit.configuration(
    wkhtmltopdf=r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe"
)

logging.basicConfig(
    filename="error_log.txt",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class BcptDscService:
    REPORT_TYPES = [
        "Company Research",
        "Strategy",
        "Sector Reports",
        "Commodities",
        "Economics",
        "ETFs",
        "Analyst Pinboard",
    ]

    LINKS_VI = [
        "https://www.dsc.com.vn/bao-cao-phan-tich/phan-tich-doanh-nghiep",
        "https://www.dsc.com.vn/bao-cao-phan-tich/bao-cao-chien-luoc-dau-tu",
        "https://www.dsc.com.vn/bao-cao-phan-tich/bao-cao-nganh",
        "https://www.dsc.com.vn/bao-cao-phan-tich/bao-cao-thi-truong-hang-hoa",
        "https://www.dsc.com.vn/bao-cao-phan-tich/bao-cao-vi-mo",
        "https://www.dsc.com.vn/bao-cao-phan-tich/bao-cao-etfs",
        "https://www.dsc.com.vn/bao-cao-phan-tich/goc-nhin-chuyen-gia",
    ]

    BASE_URL = "https://www.dsc.com.vn"
    FILE_BASE_URL = "https://extgw.dsc.com.vn/eback"

    LANGUAGE_LIST = ["VI", "EN"]

    header = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Cache-Control": "max-age=0",
        "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Microsoft Edge";v="128"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0",
    }

    @staticmethod
    def save_alternate_pdf(content):

        if not content:
            Print.error("No content to convert to PDF")
            return
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

        pdf = r.to_pdf()
        with open("./bcpt_pdf/vcsc/metadata.pdf", "wb") as f:
            f.write(pdf)
        Print.success("Alternate PDF saved successfully")

    @staticmethod
    def download_pdf(cls, download_link, content):
        print("Downloading PDF...")
        # Check valid url
        if validators.url(download_link):
            response = requests.get(download_link)
            if response.status_code == 200:
                with open("./bcpt_pdf/dsc/metadata.pdf", "wb") as f:
                    f.write(response.content)
                Print.success(f"Download PDF: {download_link} successfully")
            else:
                Print.error("PDF Expired")
                logging.error(f"DSC - PDF Expired: {download_link}")
                cls.save_alternate_pdf(content) if content else print("No content")
        else:
            Print.error("Invalid URL")
            logging.error(f"DSC - Invalid URL: {download_link}")
            cls.save_alternate_pdf(content) if content else print("No content")

    @staticmethod
    def insert_data(cursor, data, conn):
        """Insert data into the SQLite database with retries."""
        try:

            insert_query = """
                INSERT INTO reports (source, ticker, date, reportType, recommendation, headline, content, analyst, language, linkWeb, linkDrive)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            date_str = pd.Timestamp(data["date"]).strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute(
                insert_query,
                (
                    data["source"],
                    data["ticker"],
                    date_str,
                    data["reportType"],
                    data["recommendation"],
                    data["headline"],
                    data["content"],
                    data["analyst"],
                    data["language"],
                    data["linkWeb"],
                    data["linkDrive"],
                ),
            )

            conn.commit()
            Print.success(f"Data inserted successfully")

        except Exception as e:
            Print.error(f"Error inserting {data['headline']}: {e}")
            logging.error(f"VDS-AP - Error inserting {data['headline']}: {e}")

    @classmethod
    def crawl_bcpt_dsc(cls, cursor, conn):
        """Main method to crawl reports and insert data into the database."""
        for lang in cls.LANGUAGE_LIST:
            if lang == "VI":
                print("Crawling VI reports...")
                for idx, link in enumerate(cls.LINKS_VI):
                    report_type = cls.REPORT_TYPES[idx]
                    print(f"Crawling {report_type} reports...")

                    """Get preload url"""
                    try:
                        res_html = requests.get(link, headers=cls.header).text
                        soup = BeautifulSoup(res_html, "html.parser")
                        preload_url = (
                            soup.find(
                                "link",
                                {
                                    "rel": "preload",
                                    "as": "script",
                                    "href": lambda x: x
                                    and x.endswith("/_buildManifest.js"),
                                },
                            )
                            .get("href")
                            .replace("static", "data")
                        )
                        preload_url = "/".join(preload_url.split("/")[:-1])
                        slug = link.split("/")[-1]
                        api_url = f"{cls.BASE_URL}{preload_url}/bao-cao-phan-tich/{slug}.json?slug={slug}"

                        res = requests.get(api_url, headers=cls.header)
                        res_json = res.json()
                        page_num = res_json["pageProps"]["dataCategory"]["dataList"]["meta"]["pagination"]["pageCount"]

                        for page in range(page_num):
                            url = (
                                api_url.replace(".json", f"/{page + 1}.json")
                                + f"&slug={page + 1}"
                            )
                            res = requests.get(url, headers=cls.header)
                            res_json = res.json()
                            # page = res_json["pageProps"]["dataCategory"]["dataList"]["meta"]["pagination"]["page"]
                            Print.success(f"Crawling page {page + 1} of {page_num}...")

                            data = res_json["pageProps"]["dataCategory"]["dataList"][
                                "data"
                            ]
                            for data_raw in data:
                                data_raw = data_raw["attributes"]
                                ticker = None
                                recommendation = None

                                headline = data_raw["title"]
                                date = (
                                    pd.to_datetime(data_raw["public_at"])
                                    .tz_localize(None)
                                    .strftime("%Y-%m-%d %H:%M:%S")
                                )
                                # Check if description is html
                                if data_raw["description"]:
                                    html_tags = ['<html>', '<body>', '<div>', '<p>', '<a>', '<span>']
                                    if any(tag in data_raw["description"] for tag in html_tags):
                                        content_html = BeautifulSoup(data_raw["description"], "html.parser") 
                                        content = content_html.get_text() if content_html else None
                                    else:
                                        logging.error(f"DSC - Description is not html: {data_raw['description']}")
                                        content_html = None
                                        content = data_raw["description"]
                                else:
                                    logging.error(f"DSC - No description: {headline}")
                                    content = None
                                    content_html = None
                                analyst = None
                                linkWeb = (
                                    f"{cls.BASE_URL}/bao-cao-phan-tich/{data_raw['slug']}"
                                    if data_raw["slug"]
                                    else None
                                )

                                file_url = data_raw["file"]["data"][0]["attributes"]["url"]
                                download_link = f"{cls.FILE_BASE_URL}{file_url}"
                                print(f"Download link: {download_link}")

                                metadata = {
                                    "source": "dsc",
                                    "ticker": ticker,
                                    "date": date,
                                    "reportType": report_type,
                                    "recommendation": recommendation,
                                    "headline": headline,
                                    "content": content,
                                    "analyst": analyst,
                                    "language": lang,
                                    "linkWeb": linkWeb,
                                    "linkDrive": None,
                                }

                                '''Insert and download pdf'''
                                # cls.download_pdf(cls, download_link, content_html)
                                # cls.insert_data(cursor, metadata, conn)
                    except Exception as e:
                        print(f"Error getting preload url: {e}")
                        logging.error(f"DSC - Error getting preload url: {e}")

                    """Download and insert data"""
                    # cls.get_data(cls, lang, page_id, report_type, cursor, conn)
            else:
                print("EN Reports unavailable!")


# Connect to the SQLite
conn = sqlite3.connect("reports.db")
cursor = conn.cursor()

bcpt_service = BcptDscService()
bcpt_service.crawl_bcpt_dsc(cursor, conn)

conn.close()
