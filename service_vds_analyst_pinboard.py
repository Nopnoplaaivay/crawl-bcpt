import time
import requests
import sqlite3
import random
import pandas as pd
import pdfkit
import base64
import io
import logging
import os

from slugify import slugify
from PIL import Image
from print_module import Print
from bs4 import BeautifulSoup
# from weasyprint import HTML

config = pdfkit.configuration(
    wkhtmltopdf=r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe"
)

# os.add_dll_directory(r"C:\msys64\mingw64\bin")

# Configure logging
logging.basicConfig(
    filename='error_log.txt',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class BcptVdsAPService:

    REPORT_TYPE = ["Analyst Pinboard"]

    LANGUAGE = ["VI", 
                "EN"]

    API_URL = "https://vdsc.com.vn/data/api/app/management-market-commentary/public-paged?sorting=publishDate%20desc"

    @staticmethod
    def insert_data(cursor, data, conn):
        """Insert data into the SQLite database with retries."""
        try:

            insert_query = """
                INSERT INTO reports (source, ticker, date, reportType, recommendation, headline, content, analyst, language, linkWeb, linkDrive)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            date_str = pd.Timestamp(data["date"]).strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute(insert_query, (
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
                data["linkDrive"]
            ))

            # insert_query = f"""
            #     INSERT INTO reports (source, ticker, date, reportType, recommendation, headline, content, analyst, language, linkWeb, linkDrive)
            #     VALUES ('{data["source"]}', '{data["ticker"]}', '{date_str}', '{data["reportType"]}', '{data["recommendation"]}', '{data["headline"]}', '{data["content"]}', '{data["analyst"]}', '{data["language"]}', '{data["linkWeb"]}', '{data["linkDrive"]}')
            # """
            # cursor.execute(insert_query)
            conn.commit()
            Print.success(f"Data inserted successfully")

        except Exception as e:
            Print.error(f"Error inserting {data['headline']}: {e}")
            logging.error(f"VCBS - Error inserting {data['headline']}: {e}")

    @staticmethod
    def download_and_convert_image(url):
        """Fetches a WebP image, converts it to PNG, and returns Base64 encoding."""
        response = requests.get(url)
        response.raise_for_status() 
        webp_image_bytes = response.content
  
        with Image.open(io.BytesIO(webp_image_bytes)) as img:
            with io.BytesIO() as output:
                img.save(output, format="PNG")
                png_image_bytes = output.getvalue()

        base64_image = base64.b64encode(png_image_bytes).decode('utf-8')
        
        return base64_image

    @staticmethod
    def download_pdf(cls, content):
        """Convert HTML to PDF"""
        base_url_vdsc = "https://vdsc.com.vn"

        for img_tag in content.find_all("img"):
            src = img_tag.get("src")
            # print(f"Image URL: {src}")

            if src.startswith("./assets"):
                img_tag["src"] = f"{base_url_vdsc}{src[1:]}"
                # Print.warning(f"Add base_url before ./asset URL")
            elif src.startswith("c./assets"):
                img_tag.decompose()
            elif src.startswith("/data/api/app/file-storage/") or 'data:image' not in src:
                if not src.startswith('data:image'):
                    img_tag["src"] = f"{base_url_vdsc}{src}" if src.startswith("/data/api") else src
                    # Print.warning(f"Add base_url before ./data/api/app URL")
                    try:
                        base64_image = cls.download_and_convert_image(img_tag["src"])
                        img_tag["src"] = f"data:image/png;base64,{base64_image}"
                        # Print.success(f"Image converted to base64")
                    except Exception as e:
                        Print.error(f"Error converting webp to png: {e}")
                        logging.error(f"VCBS - Error converting image to base64: {e}")


        content_str = str(content)
        html_str = f"""
        <html>
            <head>
                <title></title>
                <meta charset="UTF-8">
                <style>
                    img {{
                        max-width: 100%;
                        height: auto;
                    }}
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
        # print(html_str)

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

        # HTML(string=html_str).write_pdf(f"./bcpt_pdf/vds_ap/test_output.pdf")
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
                            else f"https://vdsc.com.vn/trung-tam-phan-tich/nhan-dinh-hang-ngay/{slugify(headline)}-d{item['id']}"
                        )
                        '''Check link_web'''
                        try:
                            response = requests.get(link_web)
                            if response.status_code != 200:
                                link_web = None
                                Print.error(f"Link {link_web} is not valid")
                                logging.error(f"Link {link_web} is not valid")
                        except Exception as e:
                            link_web = None
                            Print.error(f"Error checking link {link_web}: {e}")
                            logging.error(f"Error checking link {link_web}: {e}")
                            continue

                        '''Get content'''
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
                            else f"https://vdsc.com.vn/en/research/daily-recommendations/{slugify(headline)}-d{item['id']}"
                        )
                        '''Check link_web'''
                        try:
                            response = requests.get(link_web)
                            if response.status_code != 200:
                                link_web = None
                                Print.error(f"Link {link_web} is not valid")
                                logging.error(f"Link {link_web} is not valid")
                        except Exception as e:
                            link_web = None
                            Print.error(f"Error checking link {link_web}: {e}")
                            logging.error(f"Error checking link {link_web}: {e}")
                            continue

                        '''Get content'''
                        content_html = (
                            BeautifulSoup(item["contentEn"], "html.parser")
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
                    print(f"Crawling {data['linkWeb']} ...")

                    """Download and insert data"""
                    time.sleep(random.randint(1, 2))
                    cls.download_pdf(cls, content_html)
                    cls.insert_data(cursor, data, conn)

        print("Done VDS")


# Connect to the SQLite
conn = sqlite3.connect("reports.db")
cursor = conn.cursor()

bcpt_service = BcptVdsAPService()
bcpt_service.crawl_bcpt_vds_ap(cursor, conn)

conn.close()
