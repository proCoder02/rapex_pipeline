# import requests
# from bs4 import BeautifulSoup
# import pandas as pd
# import re
# from urllib.parse import urljoin
# from sqlalchemy import create_engine
# from tqdm import tqdm

# # -------- CONFIG -------- #
# SITEMAP_URL = "https://www.euroncap.com/sitemap.xml"
# BASE_URL = "https://www.euroncap.com"
# POSTGRES_URI = "postgresql://username:password@localhost:5432/yourdb"
# CSV_OUTPUT = "euroncap_full_results.csv"
# TABLE_NAME = "euroncap_full_results"
# # ------------------------ #

# def fetch_sitemap(sitemap_url):
#     r = requests.get(sitemap_url)
#     r.raise_for_status()
#     return r.text

# def parse_sitemap(xml_text):
#     # fallback to html parser if lxml-xml isn’t available
#     soup = BeautifulSoup(xml_text, "html.parser")
#     return [loc.get_text() for loc in soup.find_all("loc") if "/en/results/" in loc.get_text()]

# def fetch_page(url):
#     r = requests.get(url)
#     r.raise_for_status()
#     return r.text

# def clean_text(text):
#     return text.strip().replace("\n", " ").replace("\r", "")

# def extract_numeric(text):
#     try:
#         return float(re.sub(r"[^\d.]", "", text))
#     except:
#         return text

# def parse_result_page(html, url):
#     soup = BeautifulSoup(html, "html.parser")
#     data = {"url": url}

#     # ———————— Metadata: title, model, publication year
#     title = soup.find("h1")
#     if title:
#         data["model_title"] = clean_text(title.get_text())

#     pub = soup.find(string=re.compile(r"Publication", re.IGNORECASE))
#     if pub:
#         year_match = re.search(r"(\d{4})", pub)
#         if year_match:
#             data["publication_year"] = int(year_match.group(1))

#     # ———————— Star ratings and % scores
#     for label in ["Adult", "Child", "Vulnerable", "Safety Assist"]:
#         tag = soup.find(string=re.compile(label, re.IGNORECASE))
#         if tag:
#             pct = re.search(r"(\d+)%", tag.parent.get_text(" ", strip=True))
#             data[f"{label.lower().replace(' ','_')}_pct"] = extract_numeric(pct.group(1)) if pct else None

#     # ———————— Points & section score breakdown
#     # e.g., Frontal Impact, Lateral, Rear, Rescue & Extrication...
#     categories = {
#         "Frontal Impact": "frontal_pts",
#         "Lateral Impact": "lateral_pts",
#         "Rear Impact": "rear_pts",
#         "Rescue and Extrication": "rescue_pts"
#     }
#     for label, key in categories.items():
#         tag = soup.find(string=re.compile(label, re.IGNORECASE))
#         if tag:
#             score_match = re.search(r"(\d+(\.\d+)?)\s*/\s*(\d+)", tag.parent.get_text())
#             if score_match:
#                 data[key] = float(score_match.group(1))

#     # ———————— Safety Equipment Table
#     # Loop through possible tables and capture any table-like data
#     for tbl in soup.find_all("table"):
#         headers = [clean_text(th.get_text()) for th in tbl.find_all("th")]
#         if headers:
#             rows = tbl.find_all("tr")
#             for r in rows:
#                 cells = [clean_text(td.get_text()) for td in r.find_all(["td"])]
#                 if len(cells) == len(headers):
#                     for h, c in zip(headers, cells):
#                         data[f"{h.lower().replace(' ','_')}"] = c

#     return data

# def save_csv(data, filename):
#     pd.DataFrame(data).to_csv(filename, index=False)
#     print(f"CSV saved → {filename}")

# def save_postgres(data, table, engine):
#     pd.DataFrame(data).to_sql(table, engine, if_exists="replace", index=False)
#     print(f"PostgreSQL saved → {table}")

# def main():
#     print("Fetching sitemap…")
#     xml = fetch_sitemap(SITEMAP_URL)
#     urls = parse_sitemap(xml)
#     print(f"Found {len(urls)} result URLs")

#     all_data = []
#     for url in tqdm(urls):
#         try:
#             html = fetch_page(url)
#             entry = parse_result_page(html, url)
#             all_data.append(entry)
#         except Exception as e:
#             print(f"Failed: {url} → {e}")

#     save_csv(all_data, CSV_OUTPUT)

#     # Uncomment to use PostgreSQL
#     # engine = create_engine(POSTGRES_URI)
#     # save_postgres(all_data, TABLE_NAME, engine)

# if __name__ == "__main__":
#     main()


# import requests
# from bs4 import BeautifulSoup
# import pandas as pd
# import re
# from sqlalchemy import create_engine
# from tqdm import tqdm

# # ================= CONFIG ================= #
# SITEMAP_URL = "https://www.euroncap.com/sitemap.xml"
# CSV_OUTPUT = "euroncap_motor_vehicles.csv"
# TABLE_NAME = "euroncap_motor_vehicles"

# POSTGRES_URI = "postgresql://postgres:0326@localhost:5432/rapex"
# HEADERS = {"User-Agent": "Mozilla/5.0"}
# # ========================================== #


# # ---------- Helpers ----------
# def clean(text):
#     return re.sub(r"\s+", " ", text.strip()) if text else None


# # ---------- Sitemap ----------
# def fetch_sitemap():
#     r = requests.get(SITEMAP_URL, headers=HEADERS)
#     r.raise_for_status()
#     return r.text


# def parse_sitemap(xml_text):
#     """
#     Extract only motor vehicle result URLs
#     Example:
#     https://www.euroncap.com/en/results/mercedes-benz/cla-class/58934
#     """
#     soup = BeautifulSoup(xml_text, "html.parser")

#     urls = []
#     for loc in soup.find_all("loc"):
#         url = loc.get_text(strip=True)
#         if re.match(r"https://www\.euroncap\.com/en/results/.+/\d+$", url):
#             urls.append(url)

#     return urls


# # ---------- Page Fetch ----------
# def fetch_page(url):
#     r = requests.get(url, headers=HEADERS)
#     r.raise_for_status()
#     return r.text


# # ---------- Page Parser ----------
# def parse_vehicle_page(html, url):
#     soup = BeautifulSoup(html, "html.parser")

#     data = {"url": url}

#     # ---- Vehicle title ----
#     title = soup.find("h1")
#     if title:
#         data["vehicle"] = clean(title.text)

#     # ---- Star rating ----
#     stars = soup.select_one(".rating__stars")
#     if stars:
#         data["star_rating"] = stars.get("data-stars")

#     # ---- Publication year ----
#     pub = soup.find(string=re.compile("Publication", re.I))
#     if pub:
#         year = re.search(r"\d{4}", pub)
#         if year:
#             data["year"] = int(year.group())

#     # ---- Percentage Scores ----
#     score_map = {
#         "Adult Occupant": "adult_occupant_pct",
#         "Child Occupant": "child_occupant_pct",
#         "Vulnerable Road Users": "vulnerable_road_users_pct",
#         "Safety Assist": "safety_assist_pct",
#     }

#     for label, key in score_map.items():
#         block = soup.find("h4", string=re.compile(label, re.I))
#         if block:
#             pct = re.search(r"(\d+)%", block.parent.get_text())
#             data[key] = int(pct.group(1)) if pct else None

#     # ---- Vehicle info tables (tabs content) ----
#     for table in soup.find_all("table"):
#         rows = table.find_all("tr")
#         for row in rows:
#             cells = row.find_all(["th", "td"])
#             if len(cells) == 2:
#                 k = clean(cells[0].text).lower().replace(" ", "_")
#                 v = clean(cells[1].text)
#                 if k and v:
#                     data[k] = v

#     return data


# # ---------- Storage ----------
# def save_csv(records):
#     df = pd.DataFrame(records)
#     df.to_csv(CSV_OUTPUT, index=False)
#     print(f"CSV saved → {CSV_OUTPUT}")


# def save_postgres(records):
#     engine = create_engine(POSTGRES_URI)
#     df = pd.DataFrame(records)
#     df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)
#     print(f"PostgreSQL table saved → {TABLE_NAME}")


# # ---------- Main ----------
# def main():
#     print("Fetching sitemap...")
#     xml = fetch_sitemap()

#     print("Parsing sitemap...")
#     urls = parse_sitemap(xml)
#     print(f"Found {len(urls)} motor vehicle pages")

#     results = []

#     for url in tqdm(urls):
#         try:
#             html = fetch_page(url)
#             data = parse_vehicle_page(html, url)
#             results.append(data)
#         except Exception as e:
#             print(f"Failed → {url} | {e}")

#     save_csv(results)

#     # Uncomment if PostgreSQL needed
#     save_postgres(results)


# if __name__ == "__main__":
#     main()


import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from sqlalchemy import create_engine
from tqdm import tqdm

# ================= CONFIG ================= #
SITEMAP_URL = "https://www.euroncap.com/sitemap.xml"
CSV_OUTPUT = "euroncap_motor_vehicles.csv"
TABLE_NAME = "euroncap_motor_vehicles"   # explicit schema

POSTGRES_URI = "postgresql://postgres:0326@localhost:5432/rapex"
HEADERS = {"User-Agent": "Mozilla/5.0"}
# ========================================== #


# ---------- Helpers ----------
def clean(text):
    if not text:
        return None
    text = re.sub(r"\s+", " ", text.strip())
    return text if text else None


def safe_key(text):
    """
    Normalize keys safely (prevents NoneType .lower crashes)
    """
    if not text:
        return None
    return text.strip().lower().replace(" ", "_")


# ---------- Sitemap ----------
def fetch_sitemap():
    r = requests.get(SITEMAP_URL, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.text


def parse_sitemap(xml_text):
    """
    Extract only motor vehicle result URLs
    Example:
    https://www.euroncap.com/en/results/mercedes-benz/cla-class/58934
    """
    soup = BeautifulSoup(xml_text, "xml")

    urls = []
    for loc in soup.find_all("loc"):
        url = loc.get_text(strip=True)
        if re.match(r"https://www\.euroncap\.com/en/results/.+/\d+$", url):
            urls.append(url)

    return urls


# ---------- Page Fetch ----------
def fetch_page(url):
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.text


# ---------- Page Parser ----------
def parse_vehicle_page(html, url):
    soup = BeautifulSoup(html, "html.parser")

    data = {"url": url}

    # ---- Vehicle title ----
    title = soup.find("h1")
    if title:
        data["vehicle"] = clean(title.get_text())

    # ---- Star rating ----
    stars = soup.select_one(".rating__stars")
    if stars and stars.get("data-stars"):
        data["star_rating"] = int(stars.get("data-stars"))

    # ---- Publication year ----
    pub = soup.find(string=re.compile("Publication", re.I))
    if pub:
        year = re.search(r"\d{4}", pub)
        if year:
            data["year"] = int(year.group())

    # ---- Percentage Scores ----
    score_map = {
        "Adult Occupant": "adult_occupant_pct",
        "Child Occupant": "child_occupant_pct",
        "Vulnerable Road Users": "vulnerable_road_users_pct",
        "Safety Assist": "safety_assist_pct",
    }

    for label, key in score_map.items():
        block = soup.find("h4", string=re.compile(label, re.I))
        if block and block.parent:
            pct = re.search(r"(\d+)%", block.parent.get_text(" ", strip=True))
            data[key] = int(pct.group(1)) if pct else None

    # ---- Vehicle info tables (tabs content) ----
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) != 2:
                continue

            raw_key = clean(cells[0].get_text())
            value = clean(cells[1].get_text())

            key = safe_key(raw_key)
            if not key or not value:
                continue

            data[key] = value

    return data


# ---------- Storage ----------
def save_csv(records):
    df = pd.DataFrame(records)
    df.to_csv(CSV_OUTPUT, index=False)
    print(f"CSV saved → {CSV_OUTPUT} ({len(df)} rows)")


def save_postgres(records):
    clean_records = [r for r in records if r and len(r) > 1]

    if not clean_records:
        raise ValueError("No valid records to save to PostgreSQL")

    df = pd.DataFrame(clean_records)

    print("Saving to PostgreSQL")
    print("Rows:", len(df))
    print("Columns:", list(df.columns))

    engine = create_engine(POSTGRES_URI)

    df.to_sql(
        TABLE_NAME,
        engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=100
    )

    print(f"PostgreSQL table saved → {TABLE_NAME}")


# ---------- Main ----------
def main():
    print("Fetching sitemap...")
    xml = fetch_sitemap()

    print("Parsing sitemap...")
    urls = parse_sitemap(xml)
    print(f"Found {len(urls)} motor vehicle pages")

    results = []

    for url in tqdm(urls):
        try:
            html = fetch_page(url)
            data = parse_vehicle_page(html, url)
            results.append(data)
        except Exception as e:
            print(f"Failed → {url} | {e}")

    # Filter bad rows early
    results = [r for r in results if r and len(r) > 1]

    print("Final valid records:", len(results))

    save_csv(results)
    save_postgres(results)


if __name__ == "__main__":
    main()
