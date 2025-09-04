#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import pyodbc

import asyncio
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


# 1. Load CSV
df = pd.read_csv("D:/Jupyter/country_info_sample.csv")

# 2. Basic cleaning
df = df.drop_duplicates()  # remove duplicates
df = df.fillna({"Languages": "Unknown", "Currencies": "Unknown"})  # fill nulls
df["Country"] = df["Country"].str.strip()  # trim whitespace
df["Region"] = df["Region"].str.title()    # standardize case

# 3. Connect to SQL Server (adjust for your instance)
conn = pyodbc.connect(
    "Driver={SQL Server};"
    "Server=localhost\\SQLEXPRESS;"  # change if your server name is different
    "Database=DS_Portfolio;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()

# 4. Clear existing table (optional, for refresh)
cursor.execute("TRUNCATE TABLE Country_Info_sample;")
conn.commit()

# 5. Insert cleaned data into SQL
for index, row in df.iterrows():
    cursor.execute("""
        INSERT INTO Country_Info_sample (Country, Region, Languages, Currencies, Latitude, Longitude, Population, Area_km2)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, row.Country, row.Region, row.Languages, row.Currencies,
         row.Latitude, row.Longitude, int(row.Population), int(row.Area_km2))

conn.commit()
cursor.close()
conn.close()

print("✅ Data loaded successfully into SQL Server!")


# In[ ]:





# In[4]:


"""
ETL: .NET Reviews API (operational)  ->  SQL Server DS_Portfolio (analytics)

Features
- HTTP/HTTPS toggle via env (default HTTP:5000 to avoid cert issues)
- Creates DS_Portfolio.dbo.Sentiment_Reviews if missing
- Normalizes sentiment labels
- Dedup by ApiId (UNIQUE) so re-runs are safe
- Optional seeding of sample reviews into the API when source is empty
"""

import os
import sys
import time
from typing import List, Dict, Any

import requests
import pyodbc
import urllib3

# -------------------------
# Config (override via environment variables)
# -------------------------
API_BASE   = os.getenv("API_BASE", "http://localhost:5000")  # e.g., "http://localhost:5000" or "https://localhost:5001"
API_VERIFY = os.getenv("API_VERIFY", "false").lower() == "true"  # set to true only if your HTTPS cert is trusted
ALLOW_FALLBACK_TO_HTTP = os.getenv("API_FALLBACK_HTTP", "true").lower() == "true"  # try http if https fails

SQL_SERVER = os.getenv("SQL_SERVER", r"DESKTOP-70AOMBI\SQLEXPRESS")
SQL_DB     = os.getenv("SQL_DB", "DS_Portfolio")
SQL_TABLE  = os.getenv("SQL_TABLE", "dbo.Sentiment_Reviews")

SEED_IF_EMPTY = os.getenv("SEED_IF_EMPTY", "true").lower() == "true"  # seed a few reviews into API if it's empty

TIMEOUT_SECS = 30

# Silence HTTPS warnings in dev when verification is off
if not API_VERIFY:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# -------------------------
# HTTP helpers
# -------------------------
def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"Accept": "application/json"})
    return s

def get_json(session: requests.Session, url: str, verify: bool) -> Any:
    r = session.get(url, verify=verify, timeout=TIMEOUT_SECS)
    r.raise_for_status()
    ct = r.headers.get("Content-Type", "")
    if not ct.startswith("application/json"):
        raise RuntimeError(f"Expected JSON, got {ct}; body: {r.text[:200]}")
    return r.json()

def post_json(session: requests.Session, url: str, payload: Dict[str, Any], verify: bool) -> Any:
    r = session.post(url, json=payload, verify=verify, timeout=TIMEOUT_SECS)
    r.raise_for_status()
    ct = r.headers.get("Content-Type", "")
    if "application/json" in ct:
        return r.json()
    return r.text


# -------------------------
# Domain helpers
# -------------------------
def normalize_sentiment(val: str | None) -> str | None:
    if not val:
        return None
    v = val.strip().lower()
    if v.startswith("pos"):
        return "Positive"
    if v.startswith("neg"):
        return "Negative"
    if v.startswith("neu"):
        return "Neutral"
    return val.strip().title()  # fallback

def fetch_all_reviews(session: requests.Session, base: str, verify: bool) -> List[Dict[str, Any]]:
    url = f"{base.rstrip('/')}/api/reviews"
    return get_json(session, url, verify)


def seed_reviews_if_needed(session: requests.Session, base: str, verify: bool) -> int:
    """Insert a few demo reviews into the API if it's empty. Returns number seeded."""
    try:
        current = fetch_all_reviews(session, base, verify)
    except Exception as e:
        print(f"[seed] Could not fetch current reviews: {e}")
        return 0

    if current:
        return 0

    samples = [
        {"text": "Great burger! Crispy fries!", "sentiment": "Positive", "date": "2025-09-03T12:05:00Z"},
        {"text": "Service was slow and inattentive.", "sentiment": "Negative", "date": "2025-09-03T12:06:00Z"},
        {"text": "Burger was okay, nothing special.", "sentiment": "Neutral", "date": "2025-09-03T12:07:00Z"},
    ]
    seeded = 0
    for s in samples:
        try:
            post_json(session, f"{base.rstrip('/')}/api/reviews", s, verify)
            seeded += 1
            time.sleep(0.1)
        except Exception as e:
            print(f"[seed] Failed to POST sample review: {e}")

    return seeded


# -------------------------
# SQL helpers
# -------------------------
def sql_connect() -> pyodbc.Connection:
    conn = pyodbc.connect(
        "Driver={SQL Server};"
        f"Server={SQL_SERVER};"
        f"Database={SQL_DB};"
        "Trusted_Connection=yes;"
    )
    return conn

def ensure_table(conn: pyodbc.Connection):
    cur = conn.cursor()
    cur.execute(f"""
    IF OBJECT_ID('{SQL_TABLE}','U') IS NULL
    BEGIN
        CREATE TABLE {SQL_TABLE}(
          Id INT IDENTITY(1,1) PRIMARY KEY,
          ApiId INT NOT NULL UNIQUE,
          [Text] NVARCHAR(MAX) NOT NULL,
          Sentiment NVARCHAR(50) NULL,
          [Date] DATETIME2 NOT NULL,
          IngestedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
        CREATE INDEX IX_Sentiment_Reviews_Date ON {SQL_TABLE}([Date]);
        CREATE INDEX IX_Sentiment_Reviews_Sentiment ON {SQL_TABLE}(Sentiment);
    END
    """)
    conn.commit()
    cur.close()

def insert_curated(conn: pyodbc.Connection, items: List[Dict[str, Any]]) -> int:
    cur = conn.cursor()
    inserted = 0
    for rv in items:
        api_id    = rv.get("id")
        text      = (rv.get("text") or "").strip()
        sentiment = normalize_sentiment(rv.get("sentiment"))
        date_val  = rv.get("date")  # ISO 8601 string

        if api_id is None or not text or date_val is None:
            continue

        try:
            cur.execute(f"""
                INSERT INTO {SQL_TABLE} (ApiId, [Text], Sentiment, [Date])
                VALUES (?, ?, ?, ?)
            """, api_id, text, sentiment, date_val)
            inserted += 1
        except pyodbc.IntegrityError:
            # Duplicate ApiId → already ingested; skip
            pass
    conn.commit()
    cur.close()
    return inserted


# -------------------------
# Main
# -------------------------
def main():
    print(f"API_BASE={API_BASE}  |  API_VERIFY={API_VERIFY}  |  SQL={SQL_SERVER}/{SQL_DB} -> {SQL_TABLE}")

    session = get_session()

    # Try request with configured base; if https fails and fallback enabled, try http equivalent
    base = API_BASE
    try:
        if SEED_IF_EMPTY:
            seeded = seed_reviews_if_needed(session, base, API_VERIFY)
            if seeded:
                print(f"Seeded {seeded} demo reviews into API.")
        reviews = fetch_all_reviews(session, base, API_VERIFY)
    except requests.exceptions.SSLError as e:
        if base.startswith("https://") and ALLOW_FALLBACK_TO_HTTP:
            http_base = base.replace("https://", "http://")
            print(f"[warn] SSL error on HTTPS. Falling back to HTTP: {http_base}")
            base = http_base
            if SEED_IF_EMPTY:
                seeded = seed_reviews_if_needed(session, base, False)
                if seeded:
                    print(f"Seeded {seeded} demo reviews into API (HTTP).")
            reviews = fetch_all_reviews(session, base, False)
        else:
            raise
    except Exception as e:
        print(f"[error] Could not fetch reviews: {e}")
        sys.exit(1)

    print(f"Fetched {len(reviews)} reviews from {base}")

    # Connect to analytics DB and ensure curated table
    try:
        conn = sql_connect()
        ensure_table(conn)
        inserted = insert_curated(conn, reviews)
        conn.close()
    except Exception as e:
        print(f"[error] Database operation failed: {e}")
        sys.exit(2)

    print(f"✅ Inserted {inserted} new rows into {SQL_DB}.{SQL_TABLE}")

    # Show a tiny sample for visual confirmation
    try:
        conn = sql_connect()
        cur = conn.cursor()
        cur.execute(f"SELECT TOP 5 ApiId, LEFT([Text], 60) AS TextPreview, Sentiment, [Date] FROM {SQL_TABLE} ORDER BY [Date] DESC;")
        rows = cur.fetchall()
        conn.close()
        print("\nSample rows (latest 5):")
        for r in rows:
            print(f"- ApiId={r.ApiId} | {r.Sentiment or 'NULL':8} | {r.Date} | {r.TextPreview}")
    except Exception as e:
        print(f"[warn] Could not fetch sample rows: {e}")


if __name__ == "__main__":
    main()


# In[ ]:





# In[ ]:





# In[ ]:




