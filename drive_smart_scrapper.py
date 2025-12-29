import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine

# -------- CONFIG -------- #
# PostgreSQL connection string
POSTGRES_URI = "postgresql://postgres:0326@localhost:5432/rapex"
# Table names
TABLE_CARS = "ncap_cars"
TABLE_EV = "ncap_electric_cars"
# CSV file outputs
CSV_CARS = "ncap_cars.csv"
CSV_EV = "ncap_electric_cars.csv"
# URLs to scrape
URL_CARS = "https://www.drivesmart.co.uk/NCAP-safety-ratings/NCAP-safety-ratings-for-cars.aspx"
URL_EV = "https://www.drivesmart.co.uk/NCAP-safety-ratings/NCAP-safety-ratings-for-electric-cars.aspx"
# ------------------------ #

def fetch_html(url):
    """Fetch the HTML content of a page."""
    response = requests.get(url)
    response.raise_for_status()
    return response.text

def parse_table(html):
    """Parse NCAP table HTML into a list of dicts."""
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("tr")

    data = []
    headers = []
    # Extract headers last (found by inspecting first <th> or row cells)
    for idx, row in enumerate(rows):
        cols = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]
        if not cols:
            continue

        # First meaningful row that contains header-looking data
        if idx == 0 or not headers:
            # Some pages use first <tr> as header-like
            # check if the row has known header values
            if len(cols) > 3 and "Manufacturer" in cols[0]:
                headers = [c.lower().replace(" ", "_") for c in cols]
                continue

        # If we have headers, map into dict
        if headers and len(cols) == len(headers):
            record = dict(zip(headers, cols))
            data.append(record)

    return data

def save_csv(data, filename):
    """Save dict list to CSV."""
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print(f"[+] Saved CSV: {filename}")

def save_postgres(data, table_name, engine):
#     """Insert data into PostgreSQL."""
    df = pd.DataFrame(data)
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"[+] Saved PostgreSQL table: {table_name}")

def main():
    # Fetch and parse pages
    html_cars = fetch_html(URL_CARS)
    html_ev = fetch_html(URL_EV)

    data_cars = parse_table(html_cars)
    data_ev = parse_table(html_ev)

    # Save to CSV
    save_csv(data_cars, CSV_CARS)
    save_csv(data_ev, CSV_EV)

    # Save to PostgreSQL
    engine = create_engine(POSTGRES_URI)
    save_postgres(data_cars, TABLE_CARS, engine)
    save_postgres(data_ev, TABLE_EV, engine)

if __name__ == "__main__":
    main()
