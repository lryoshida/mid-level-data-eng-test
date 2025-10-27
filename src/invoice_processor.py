#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Invoice → item details into 'details_invoice' table (or Excel).
Columns: item, hsn_code, quantity, unit_price, total_price
Supports --pdf (local), --s3-uri (S3), or --url (HTTP/HTTPS).
"""

import argparse, os, re
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse

import pandas as pd
from PyPDF2 import PdfReader
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import requests

# ---------- input source (local/S3/URL) ----------
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
    """Download a PDF from S3 using current AWS credentials."""
    try: import boto3
    except ImportError as e: raise SystemExit("❌ To use S3, install boto3.") from e
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise SystemExit("Invalid S3 URI. Use: s3://bucket/path/file.pdf")
    bucket, key = parsed.netloc, parsed.path.lstrip("/")
    cache = _ensure_cache_dir()
    dst = cache / (os.path.basename(key) or "input.pdf")
    boto3.client("s3").download_file(bucket, key, str(dst))
    return dst

def resolve_input_source(pdf: Optional[str], s3_uri: Optional[str], url: Optional[str]) -> Path:
    """Return a local file path for local/S3/URL input."""
    if s3_uri: return download_s3(s3_uri)
    if url:    return download_url(url)
    if pdf:
        p = Path(pdf); 
        if not p.exists(): raise FileNotFoundError(f"PDF not found: {p}")
        return p
    raise SystemExit("❌ Provide at least one source: --pdf or --s3-uri or --url")

# ---------- parsing ----------
def pdf_to_lines(pdf_path: Path) -> List[str]:
    """Extract and normalize text lines from the PDF."""
    reader = PdfReader(str(pdf_path))
    lines: List[str] = []
    for p in reader.pages:
        txt = (p.extract_text() or "")
        for ln in txt.splitlines():
            ln = re.sub(r"\s+", " ", ln).strip()
            if ln: lines.append(ln)
    return lines

def extract_items_table(lines: List[str]) -> pd.DataFrame:
    """
    Extract only the invoice grid (no 'Desc' column):
      item | hsn_code | quantity | unit_price | total_price
    Rules:
      - accept a new row only if the block has HSN/HSDN or two $ prices
      - stop when reaching headers outside the grid (From/To/Date/Payment/Subtotal/Total/GST)
      - clean duplicated item names caused by PDF line breaks (e.g., 'versionWordpress', 'DeploymentServer')
    """
    header_idx = None
    for i, ln in enumerate(lines):
        if re.search(r"\bItem\s+Desc\b.*HSN\s*Code.*Quantity.*Unit\s+Price.*Total\s+Price", ln, re.I):
            header_idx = i; break
    if header_idx is None:
        return pd.DataFrame(columns=["item","hsn_code","quantity","unit_price","total_price"])

    def is_outside_table(s: str) -> bool:
        return bool(re.search(r"^(From|To)\s*:|^Date\s*:|^Payment details|^THANK YOU|^Subtotal|^Total Amount|^Total\b|^GST\b", s, re.I))

    rows: List[dict] = []
    i = header_idx + 1
    while i < len(lines):
        ln = lines[i]
        if is_outside_table(ln): break
        m_start = re.match(r"^\s*(\d+)\b", ln)
        if m_start:
            bucket = [ln[m_start.end():].strip()]
            j = i + 1
            while j < len(lines):
                if re.match(r"^\s*\d+\b", lines[j]) or is_outside_table(lines[j]): break
                bucket.append(lines[j]); j += 1
            blob = " ".join(bucket)
            prices = re.findall(r"\$\s*\d+(?:\.\d{1,2})?", blob)
            two_prices = len(prices) >= 2
            m_hsn = re.search(r"(?:Desc)?\s*(HSN|HSDN)\s*([A-Za-z0-9\-]+)", blob, re.I)
            has_hsn = bool(m_hsn)
            if not (two_prices or has_hsn): i += 1; continue
            if m_hsn:
                hsn_code = f"{m_hsn.group(1).upper()}{m_hsn.group(2)}"
                left = blob[:m_hsn.start()].strip()
                right = blob[m_hsn.end():].strip()
            else:
                hsn_code=None; left, right = blob, ""
            m_desc_kw = re.search(r"\bDesc\b", left, re.I)
            item = left[:m_desc_kw.start()].strip() if m_desc_kw else " ".join(left.split()[:2]) if len(left.split())>=2 else left
            qty = None
            for tok in (right or left).split():
                if re.fullmatch(r"\d+", tok):
                    qty = int(tok); break
            to_num = lambda s: float(re.sub(r"[^\d.]", "", s))
            unit_price  = to_num(prices[0]) if len(prices)>=1 else None
            total_price = to_num(prices[1]) if len(prices)>=2 else None
            rows.append({"item": item, "hsn_code": hsn_code, "quantity": qty, "unit_price": unit_price, "total_price": total_price})
            i = j; continue
        i += 1

    df = pd.DataFrame(rows, columns=["item","hsn_code","quantity","unit_price","total_price"])
    if not df.empty:
        df["quantity"]    = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
        df["unit_price"]  = pd.to_numeric(df["unit_price"], errors="coerce")
        df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce")

        def dedup_item(name: str) -> str:
            """Remove duplicated endings due to broken words across lines."""
            s = " ".join(str(name).split())
            toks = s.split()
            if len(toks)>=2 and toks[-1].lower()==toks[0].lower(): toks = toks[:-1]
            if len(toks)>=2 and toks[1].lower().endswith(toks[0].lower()) and len(toks[1])>len(toks[0])+1:
                toks[1]=toks[1][:-len(toks[0])]
            return " ".join(toks).strip()

        df["item"] = df["item"].astype(str).apply(dedup_item)
    return df

# ---------- outputs ----------
def write_excel(df: pd.DataFrame, out_xlsx: Path):
    """Write the items DataFrame to an Excel file."""
    out_xlsx.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as xw:
        df.to_excel(xw, index=False, sheet_name="items")

def write_postgres(df: pd.DataFrame, pg_url: str, schema: str, replace: bool):
    """Create details_invoice and append rows (optionally truncating first)."""
    eng = create_engine(pg_url)
    with eng.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'))
        if replace:
            conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."details_invoice" CASCADE;'))
        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS "{schema}"."details_invoice"(
                id BIGSERIAL PRIMARY KEY,
                item TEXT,
                hsn_code TEXT,
                quantity NUMERIC,
                unit_price NUMERIC,
                total_price NUMERIC
            );
        '''))
        if replace:
            conn.execute(text(f'TRUNCATE TABLE "{schema}"."details_invoice" RESTART IDENTITY;'))
        if not df.empty:
            df.to_sql("details_invoice", conn, schema=schema, if_exists="append", index=False, method="multi", chunksize=1000)

def main():
    load_dotenv()
    ap = argparse.ArgumentParser(description="Invoice → details in Postgres (or Excel).")
    srcgrp = ap.add_mutually_exclusive_group(required=True)
    srcgrp.add_argument("--pdf")
    srcgrp.add_argument("--s3-uri")
    srcgrp.add_argument("--url")
    ap.add_argument("--out-xlsx")
    ap.add_argument("--pg-url")
    ap.add_argument("--schema", default="public")
    ap.add_argument("--replace", action="store_true")
    args = ap.parse_args()

    src = resolve_input_source(args.pdf, args.s3_uri, args.url)
    df = extract_items_table(pdf_to_lines(src))

    if args.out_xlsx:
        out = Path(args.out_xlsx); write_excel(df, out); print(f"💾 Excel: {out.resolve()}")

    pg_url = args.pg_url or os.getenv("PG_URL")
    if pg_url:
        write_postgres(df, pg_url, args.schema, args.replace)
        print(f"🐘 Table '{args.schema}.details_invoice' updated.")

    if not args.out_xlsx and not pg_url:
        print("⚠️ No destination. Use --out-xlsx and/or --pg-url (or PG_URL in .env).")

if __name__ == "__main__":
    main()
