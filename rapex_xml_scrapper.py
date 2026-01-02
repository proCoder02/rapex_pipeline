import requests
import xml.etree.ElementTree as ET
import csv
import time
import os
import pandas as pd
from sqlalchemy import create_engine

# ---------------- CONFIG ---------------- #
POSTGRES_URI = "postgresql://postgres:0326@localhost:5432/rapex"
TABLE_RAPEX_ALERT = "rapex_motor_vehicle_alerts"

WEEKLY_LIST_URL = (
    "https://ec.europa.eu/safety-gate-alerts/api/download/weeklyReport/list/xml/en"
)

OUTPUT_FILE = "motor_vehicle_alerts.csv"

NS = {
    "xsi": "http://www.w3.org/2001/XMLSchema-instance"
}
# ---------------------------------------- #


def fetch_xml(url):
    """Fetch and parse XML from URL"""
    response = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=30
    )
    response.raise_for_status()
    return ET.fromstring(response.content)


def extract_detail_urls(weekly_root):
    """Extract weekly detail report URLs"""
    urls = []
    for url_tag in weekly_root.findall(".//URL"):
        if url_tag.text:
            urls.append(url_tag.text.strip())
    return urls


def safe_text(node, tag):
    """Safely extract text from XML tag"""
    return (node.findtext(tag) or "").strip()


def extract_motor_vehicle_notifications(detail_root):
    """Extract Passenger Car Motor Vehicle alerts"""
    records = []

    for notif in detail_root.findall(".//notifications"):
        notif_type = notif.attrib.get(f"{{{NS['xsi']}}}type", "")

        # 1️⃣ Schema-level filter
        if notif_type != "webReportXmlWeeklyReportDetailMotorVehicleContentDto":
            continue

        category = safe_text(notif, "category")
        product = safe_text(notif, "product")

        # 2️⃣ Strict content-level filter
        if category != "Motor vehicles":
            continue
        if product != "Passenger car":
            continue

        record = {
            "case_number": safe_text(notif, "caseNumber"),
            "published_date": safe_text(notif, "publishedDate"),
            "report_number": safe_text(notif, "reportNumber"),
            "category": category,
            "product": product,
            "brand": safe_text(notif, "brand"),
            "name": safe_text(notif, "name"),
            "model": safe_text(notif, "type_numberOfModel"),
            "production_dates": safe_text(notif, "productionDates"),
            "ec_type_approval": safe_text(notif, "ecTypeApproval"),
            "company_recall_code": safe_text(notif, "companyRecallCode"),
            "risk_type": safe_text(notif, "riskType"),
            "danger": safe_text(notif, "danger"),
            "risk_description": safe_text(notif, "riskDescription"),
            "legal_provisions": safe_text(notif, "legalProvisions"),
            "measures": safe_text(notif, "measures"),
            "measures_economic_operators": safe_text(notif, "measuresEconomicOperators"),
            "description": safe_text(notif, "description"),
            "notifying_country": safe_text(notif, "notifyingCountry"),
            "country_of_origin": safe_text(notif, "countryOfOrigin"),
            "risk_level": safe_text(notif, "level"),
            "reference_url": safe_text(notif, "reference"),
            "recall_website": safe_text(notif, "recallWebsite"),
        }

        records.append(record)

    return records


def save_csv(records, output_file):
    """Save records to CSV"""
    if not records:
        print("No records to save.")
        return
        
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    print(f"[+] CSV saved → {output_file}")


def save_postgres(records, table_name):
    """Save records to PostgreSQL"""
    if not records:
        print("No records to save to database.")
        return
        
    engine = create_engine(POSTGRES_URI)
    df = pd.DataFrame(records)

    df.to_sql(
        table_name,
        engine,
        if_exists="append",   # do NOT destroy history
        index=False,
        method="multi"
    )

    print(f"[+] PostgreSQL table updated → {table_name}")


def main():
    print("Fetching weekly RAPEX report list...")
    weekly_root = fetch_xml(WEEKLY_LIST_URL)

    detail_urls = extract_detail_urls(weekly_root)
    print(f"Found {len(detail_urls)} weekly reports")

    all_records = []

    for idx, detail_url in enumerate(detail_urls, start=1):
        try:
            print(f"[{idx}/{len(detail_urls)}] Processing weekly report")
            detail_root = fetch_xml(detail_url)

            records = extract_motor_vehicle_notifications(detail_root)
            all_records.extend(records)

            time.sleep(0.5)  # polite crawling

        except Exception as e:
            print(f"Error processing {detail_url}: {e}")

    if not all_records:
        print("No Passenger car motor vehicle alerts found.")
        return

    output_path = os.path.join(os.getcwd(), OUTPUT_FILE)

    save_csv(all_records, output_path)
    save_postgres(all_records, TABLE_RAPEX_ALERT)

    print(f"[✓] Pipeline completed. Total records: {len(all_records)}")


if __name__ == "__main__":
    main()