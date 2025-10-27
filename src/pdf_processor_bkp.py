#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pdf_processor.py
Jordan 2019 Standard Specifications → Excel and/or PostgreSQL.
Supports --pdf (local), --s3-uri (S3), or --url (HTTP/HTTPS).
"""

import argparse
import os
import re
from pathlib import Path
from typing import List, Optional, Dict
from urllib.parse import urlparse

import pandas as pd
from PyPDF2 import PdfReader
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import requests

# ---------- input source helpers (local/S3/URL) ----------
def _ensure_cache_dir() -> Path:
    d = Path(".cache/inputs"); d.mkdir(parents=True, exist_ok=True); return d

def download_url(url: str) -> Path:
    cache = _ensure_cache_dir()
    name = os.path.basename(urlparse(url).path) or "input.pdf"
    dst = cache / name
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dst, "wb") as f:
            for chunk in r.iter_content(8192):
                if chunk: f.write(chunk)
    return dst

def download_s3(s3_uri: str) -> Path:
    """Download a PDF from S3 using the current AWS credentials."""
    try:
        import boto3
    except ImportError as e:
        raise SystemExit("❌ To use S3, install boto3 (pip install boto3).") from e
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise SystemExit("Invalid S3 URI. Example: s3://bucket/path/file.pdf")
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    cache = _ensure_cache_dir()
    name = os.path.basename(key) or "input.pdf"
    dst = cache / name
    boto3.client("s3").download_file(bucket, key, str(dst))
    return dst

def resolve_input_source(pdf: Optional[str], s3_uri: Optional[str], url: Optional[str]) -> Path:
    """Return a local file path regardless of origin (local, S3, or URL)."""
    if s3_uri: return download_s3(s3_uri)
    if url:    return download_url(url)
    if pdf:
        p = Path(pdf)
        if not p.exists(): raise FileNotFoundError(f"PDF not found: {p}")
        return p
    raise SystemExit("❌ Provide a source: --pdf (local) or --s3-uri or --url")

# ---------- extraction/cleaning regexes ----------
SEC_RE     = re.compile(r"^\s*(\d{5})\s*-\s*(.+?)\s*$")
PLATE_RE   = re.compile(r"^\s*(\d{4}J)\s+(.+?)\s*$")
ARTICLE_RE = re.compile(r"^\s*Article\s+(\d+)\s+[–-]\s+(.+?)\s*$", re.IGNORECASE)

KNOWN_PLATE_DOMAINS = {
    "STREETS","LIGHTING","EROSION & SEDIMENT CONTROL","STORM SEWER","SANITARY SEWER","WATER","MISCELLANEOUS",
}

STOP = {"AND","&","OF","THE","A","AN","TO","FOR","IN","ON","WITH","BY","AT","FROM","OR","–","—","-","TITLE"}
PUNCT_RE = re.compile(r"[^\w\s]")
DASH_RE  = re.compile(r"[–—-]")

def clean_title(text: str) -> str:
    """Normalize a title: uppercase, remove symbols/stopwords, basic plural trimming."""
    if text is None: return ""
    s = str(text).upper()
    s = s.replace("&", " AND ")
    s = DASH_RE.sub(" ", s)
    s = PUNCT_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    toks = [t for t in s.split() if t and t not in STOP and not t.isdigit() and len(t) >= 3]
    toks = [re.sub(r"([A-Z]+)S\b", r"\1", t) for t in toks]
    return " ".join(toks)

def explode_tokens(series: pd.Series) -> pd.Series:
    """Tokenize cleaned titles for frequency stats."""
    tokens = series.fillna("").astype(str).str.split().explode()
    return tokens[tokens.str.len() >= 3]

def read_pdf_lines(pdf_path: Path) -> List[List[str]]:
    """Extract text lines per page from the PDF."""
    reader = PdfReader(str(pdf_path))
    pages = []
    for pg in reader.pages:
        text = (pg.extract_text() or "")
        lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.splitlines()]
        lines = [ln for ln in lines if ln]
        pages.append(lines)
    return pages

def extract_metadata(pages: List[List[str]], pdf_path: Path) -> Dict[str, str]:
    """Detect basic metadata: title, edition year, jurisdiction, base standard."""
    all_text = " ".join([" ".join(ls) for ls in pages])
    title = "Standard Specifications for Construction of Public Infrastructure" if \
        "Standard Specifications for Construction of Public Infrastructure" in all_text else "Standard Specifications (Jordan)"
    owner = "City of Jordan, MN"
    m_year = re.search(r"\b(20\d{2})\s+Edition\b", all_text)
    year = m_year.group(1) if m_year else "2019"
    std_base = "EJCDC C-700 (2013)"
    return {
        "document_id": f"Jordan_Standard_Specs_{year}",
        "title": title, "edition_year": year, "jurisdiction": owner,
        "source_path": str(pdf_path), "standard_base": std_base, "doc_type": "spec",
    }

def extract_structured(pages: List[List[str]]):
    """Parse sections (5-digit codes), detail plates (####J), and EJCDC articles."""
    sections, plates, articles = [], [], []
    in_plate_block = False; current_domain = None
    for lines in pages:
        for raw in lines:
            line = raw.strip()
            if "CITY OF JORDAN STANDARD DETAIL PLATES" in line.upper():
                in_plate_block = True; current_domain = None; continue
            if in_plate_block:
                if line.upper() in KNOWN_PLATE_DOMAINS:
                    current_domain = line.upper(); continue
                m_plate = PLATE_RE.match(line)
                if m_plate and current_domain:
                    plates.append({"code": m_plate.group(1), "title": m_plate.group(2), "domain": current_domain})
                    continue
            m_sec = SEC_RE.match(line)
            if m_sec:
                sections.append({"code": m_sec.group(1), "title": m_sec.group(2)}); continue
            m_art = ARTICLE_RE.match(line)
            if m_art:
                try: art_no = int(m_art.group(1))
                except: art_no = None
                articles.append({"article_no": art_no, "heading": m_art.group(2)})
                continue
    df_sections = pd.DataFrame(sections).drop_duplicates() if sections else pd.DataFrame(columns=["code","title"])
    df_plates   = pd.DataFrame(plates).drop_duplicates()   if plates   else pd.DataFrame(columns=["code","title","domain"])
    df_articles = pd.DataFrame(articles).drop_duplicates() if articles else pd.DataFrame(columns=["article_no","heading"])
    return df_sections, df_plates, df_articles

def make_line_items(df_sections: pd.DataFrame, df_plates: pd.DataFrame) -> pd.DataFrame:
    """Unify sections and detail plates into a single line_items table (for downstream)."""
    frames = []
    if not df_sections.empty:
        x = df_sections.copy()
        x["item_type"] = "spec_section"; x["description"]=None; x["quantity"]=None; x["unit_price"]=None; x["total"]=None
        frames.append(x[["code","title","item_type","description","quantity","unit_price","total"]])
    if not df_plates.empty:
        y = df_plates.copy()
        y["item_type"] = "detail_plate"; y["description"]=None; y["quantity"]=None; y["unit_price"]=None; y["total"]=None
        frames.append(y[["code","title","domain","item_type","description","quantity","unit_price","total"]])
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["code","title","item_type","description","quantity","unit_price","total"]
    )

def compute_domain_distribution(df_plates: pd.DataFrame, total_items: int) -> pd.DataFrame:
    """Compute category distribution over detail plates (counts and percentages)."""
    if df_plates.empty or "domain" not in df_plates.columns:
        return pd.DataFrame(columns=["domain","count","pct_of_plates","pct_of_total_items"])
    counts = df_plates["domain"].value_counts().rename_axis("domain").reset_index(name="count")
    total_plates = int(counts["count"].sum())
    counts["pct_of_plates"] = (counts["count"] / max(total_plates, 1) * 100).round(1)
    counts["pct_of_total_items"] = (counts["count"] / max(total_items, 1) * 100).round(1)
    return counts

def write_excel(out_xlsx: Path, sheets: Dict[str, pd.DataFrame]) -> None:
    """Write multiple DataFrames to an Excel workbook (one sheet per key)."""
    out_xlsx.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as xw:
        for name, df in sheets.items():
            if df is not None and not df.empty:
                df.to_excel(xw, index=False, sheet_name=name)

def write_postgres(pg_url: str, sheets: Dict[str, pd.DataFrame], schema: str, replace: bool):
    """Create tables (if needed) and load dataframes into PostgreSQL."""
    eng = create_engine(pg_url)
    with eng.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'))
        if replace:
            for t in ["documents","spec_sections","detail_plates","ejcdc_articles","line_items","line_item_tokens","domain_distribution"]:
                conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{t}" CASCADE;'))
        # DDLs (simplified)
        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS "{schema}"."documents"(
                document_id TEXT PRIMARY KEY, title TEXT, edition_year TEXT, jurisdiction TEXT,
                source_path TEXT, standard_base TEXT, doc_type TEXT
            );
        '''))
        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS "{schema}"."spec_sections"(
                code TEXT PRIMARY KEY, title TEXT
            );
        '''))
        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS "{schema}"."detail_plates"(
                code TEXT PRIMARY KEY, title TEXT, domain TEXT
            );
        '''))
        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS "{schema}"."ejcdc_articles"(
                article_no INTEGER, heading TEXT
            );
        '''))
        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS "{schema}"."line_items"(
                code TEXT, title TEXT, domain TEXT, item_type TEXT, description TEXT,
                quantity NUMERIC, unit_price NUMERIC, total NUMERIC, title_clean TEXT
            );
        '''))
        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS "{schema}"."line_item_tokens"(
                token TEXT, count INTEGER, pct_of_items NUMERIC
            );
        '''))
        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS "{schema}"."domain_distribution"(
                domain TEXT, count INTEGER, pct_of_plates NUMERIC, pct_of_total_items NUMERIC
            );
        '''))
        # Load each frame
        for name, table in [
            ("documents","documents"),
            ("spec_sections","spec_sections"),
            ("detail_plates","detail_plates"),
            ("ejcdc_articles","ejcdc_articles"),
            ("line_items","line_items"),
            ("line_item_tokens","line_item_tokens"),
            ("domain_distribution","domain_distribution"),
        ]:
            df = sheets.get(name)
            if df is not None and not df.empty:
                df.to_sql(table, conn, schema=schema, if_exists="append", index=False, method="multi", chunksize=1000)

def main():
    load_dotenv()

    ap = argparse.ArgumentParser(description="Jordan Specs → Excel and/or PostgreSQL (local/S3/URL).")
    srcgrp = ap.add_mutually_exclusive_group(required=True)
    srcgrp.add_argument("--pdf", help="Local PDF path")
    srcgrp.add_argument("--s3-uri", help="s3://bucket/path/file.pdf")
    srcgrp.add_argument("--url", help="https://host/path/file.pdf")
    ap.add_argument("--out-xlsx", help="Output Excel path")
    ap.add_argument("--pg-url", help="Postgres URL (or use PG_URL in .env)")
    ap.add_argument("--schema", default="public")
    ap.add_argument("--replace", action="store_true")
    args = ap.parse_args()

    pg_url = args.pg_url or os.getenv("PG_URL")
    pdf_path = resolve_input_source(args.pdf, args.s3_uri, args.url)

    # Extract
    pages = read_pdf_lines(pdf_path)
    meta = extract_metadata(pages, pdf_path)
    df_docs = pd.DataFrame([meta])
    df_sections, df_plates, df_articles = extract_structured(pages)
    df_items = make_line_items(df_sections, df_plates)

    # Transform
    df_items["title_clean"] = df_items["title"].apply(clean_title)
    tokens = explode_tokens(df_items["title_clean"]).value_counts().rename_axis("token").reset_index(name="count")
    total_items = max(len(df_items), 1)
    tokens["pct_of_items"] = (tokens["count"]/total_items*100).round(1)
    df_domain = compute_domain_distribution(df_plates, len(df_items))

    # Pack outputs
    sheets = {
        "documents": df_docs,
        "spec_sections": df_sections,
        "detail_plates": df_plates,
        "ejcdc_articles": df_articles,
        "line_items": df_items,
        "line_item_tokens": tokens,
        "domain_distribution": df_domain,
    }

    # Write Excel
    if args.out_xlsx:
        out = Path(args.out_xlsx); out.parent.mkdir(parents=True, exist_ok=True)
        write_excel(out, sheets)
        print(f"💾 Excel written to: {out.resolve()}")

    # Write PostgreSQL
    if pg_url:
        write_postgres(pg_url, sheets, schema=args.schema, replace=args.replace)
        print(f"🐘 PostgreSQL populated (schema={args.schema}).")

    if not args.out_xlsx and not pg_url:
        print("⚠️ No destination provided. Use --out-xlsx and/or --pg-url / PG_URL in .env.")

if __name__ == "__main__":
    main()
