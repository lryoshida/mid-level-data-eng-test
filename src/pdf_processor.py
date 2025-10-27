#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pdf_processor.py
Jordan 2019 Standard Specifications → Excel e/ou PostgreSQL.
Suporta --pdf (local), --s3-uri (S3) ou --url (HTTP/HTTPS).

Saídas:
- Excel com múltiplas abas
- Tabelas PostgreSQL (schema configurável):
    documents (PK: document_id)
    spec_sections (PK: code)
    detail_plates (PK: code)
    ejcdc_articles
    line_items
    line_item_tokens
    domain_distribution
"""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
from typing import List, Optional, Dict, Tuple
from urllib.parse import urlparse

import pandas as pd
from PyPDF2 import PdfReader
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import requests

# ---------- helpers de entrada (local/S3/URL) ----------
def _ensure_cache_dir() -> Path:
    d = Path(".cache/inputs")
    d.mkdir(parents=True, exist_ok=True)
    return d

def download_url(url: str) -> Path:
    cache = _ensure_cache_dir()
    name = os.path.basename(urlparse(url).path) or "input.pdf"
    dst = cache / name
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dst, "wb") as f:
            for chunk in r.iter_content(8192):
                if chunk:
                    f.write(chunk)
    return dst

def download_s3(s3_uri: str) -> Path:
    """Baixa um PDF do S3 usando as credenciais atuais."""
    try:
        import boto3
    except ImportError as e:
        raise SystemExit("❌ Para usar S3, instale boto3 (pip install boto3).") from e
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise SystemExit("URI S3 inválida. Exemplo: s3://bucket/path/file.pdf")
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    cache = _ensure_cache_dir()
    name = os.path.basename(key) or "input.pdf"
    dst = cache / name
    boto3.client("s3").download_file(bucket, key, str(dst))
    return dst

def resolve_input_source(pdf: Optional[str], s3_uri: Optional[str], url: Optional[str]) -> Path:
    """Retorna um caminho local para o arquivo, independente da origem."""
    if s3_uri:
        return download_s3(s3_uri)
    if url:
        return download_url(url)
    if pdf:
        p = Path(pdf)
        if not p.exists():
            raise FileNotFoundError(f"PDF não encontrado: {p}")
        return p
    raise SystemExit("❌ Informe uma fonte: --pdf (local) ou --s3-uri ou --url")

# ---------- regex / limpeza ----------
SEC_RE     = re.compile(r"^\s*(\d{5})\s*-\s*(.+?)\s*$")
PLATE_RE   = re.compile(r"^\s*(\d{4}J)\s+(.+?)\s*$")
ARTICLE_RE = re.compile(r"^\s*Article\s+(\d+)\s+[–-]\s+(.+?)\s*$", re.IGNORECASE)

KNOWN_PLATE_DOMAINS = {
    "STREETS", "LIGHTING", "EROSION & SEDIMENT CONTROL", "STORM SEWER",
    "SANITARY SEWER", "WATER", "MISCELLANEOUS",
}

STOP = {"AND","&","OF","THE","A","AN","TO","FOR","IN","ON","WITH","BY","AT","FROM","OR","–","—","-","TITLE"}
PUNCT_RE = re.compile(r"[^\w\s]")
DASH_RE  = re.compile(r"[–—-]")

def clean_title(text: str) -> str:
    """Normaliza um título: UPPER, remove símbolos/stopwords, trim de plural simples."""
    if text is None:
        return ""
    s = str(text).upper()
    s = s.replace("&", " AND ")
    s = DASH_RE.sub(" ", s)
    s = PUNCT_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    toks = [t for t in s.split() if t and t not in STOP and not t.isdigit() and len(t) >= 3]
    toks = [re.sub(r"([A-Z]+)S\b", r"\1", t) for t in toks]
    return " ".join(toks)

def explode_tokens(series: pd.Series) -> pd.Series:
    """Tokeniza títulos limpos para estatísticas de frequência."""
    tokens = series.fillna("").astype(str).str.split().explode()
    return tokens[tokens.str.len() >= 3]

# ---------- leitura e parsing ----------
def read_pdf_lines(pdf_path: Path) -> List[List[str]]:
    """Extrai linhas de texto por página do PDF."""
    reader = PdfReader(str(pdf_path))
    pages: List[List[str]] = []
    for pg in reader.pages:
        text = (pg.extract_text() or "")
        lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.splitlines()]
        lines = [ln for ln in lines if ln]
        pages.append(lines)
    return pages

def extract_metadata(pages: List[List[str]], pdf_path: Path) -> Dict[str, str]:
    """Detecta metadados simples: título, ano, jurisdição, base do padrão."""
    all_text = " ".join([" ".join(ls) for ls in pages])
    title = "Standard Specifications for Construction of Public Infrastructure" \
        if "Standard Specifications for Construction of Public Infrastructure" in all_text \
        else "Standard Specifications (Jordan)"
    owner = "City of Jordan, MN"
    m_year = re.search(r"\b(20\d{2})\s+Edition\b", all_text)
    year = m_year.group(1) if m_year else "2019"
    std_base = "EJCDC C-700 (2013)"
    return {
        "document_id": f"Jordan_Standard_Specs_{year}",
        "title": title,
        "edition_year": year,
        "jurisdiction": owner,
        "source_path": str(pdf_path),
        "standard_base": std_base,
        "doc_type": "spec",
    }

def extract_structured(pages: List[List[str]]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Parseia:
      - Seções (códigos 5 dígitos)
      - Detail plates (####J + título, com domínio)
      - Artigos EJCDC (Article N – Heading)
    """
    sections, plates, articles = [], [], []
    in_plate_block = False
    current_domain: Optional[str] = None

    for lines in pages:
        for raw in lines:
            line = raw.strip()

            # Início do bloco de plates
            if "CITY OF JORDAN STANDARD DETAIL PLATES" in line.upper():
                in_plate_block = True
                current_domain = None
                continue

            # Domínios de plates (ex.: STREETS, LIGHTING, etc.)
            if in_plate_block:
                up = line.upper()
                if up in KNOWN_PLATE_DOMAINS:
                    current_domain = up
                    continue
                m_plate = PLATE_RE.match(line)
                if m_plate:
                    plates.append({
                        "code": m_plate.group(1),
                        "title": m_plate.group(2).strip(),
                        "domain": current_domain
                    })
                    continue

            # Seções (5 dígitos - título)
            m_sec = SEC_RE.match(line)
            if m_sec:
                sections.append({"code": m_sec.group(1), "title": m_sec.group(2).strip()})
                continue

            # Artigos EJCDC
            m_art = ARTICLE_RE.match(line)
            if m_art:
                try:
                    art_no = int(m_art.group(1))
                except Exception:
                    art_no = None
                articles.append({"article_no": art_no, "heading": m_art.group(2).strip()})
                continue

    df_sections = pd.DataFrame(sections, columns=["code", "title"]).drop_duplicates(subset=["code"])
    df_plates   = pd.DataFrame(plates,   columns=["code", "title", "domain"]).drop_duplicates(subset=["code"])
    df_articles = pd.DataFrame(articles, columns=["article_no", "heading"]).drop_duplicates()
    return df_sections, df_plates, df_articles

def make_line_items(df_sections: pd.DataFrame, df_plates: pd.DataFrame) -> pd.DataFrame:
    """
    Gera uma lista simples de 'line items' combinando seções e plates.
    (Exemplo: item_type indica origem; preços simulados = None)
    """
    items = []
    for _, r in df_sections.iterrows():
        items.append({
            "code": r["code"],
            "title": r["title"],
            "domain": None,
            "item_type": "spec_section",
            "description": None,
            "quantity": None,
            "unit_price": None,
            "total": None,
        })
    for _, r in df_plates.iterrows():
        items.append({
            "code": r["code"],
            "title": r["title"],
            "domain": r.get("domain"),
            "item_type": "detail_plate",
            "description": None,
            "quantity": None,
            "unit_price": None,
            "total": None,
        })
    return pd.DataFrame(items, columns=[
        "code", "title", "domain", "item_type", "description",
        "quantity", "unit_price", "total"
    ])

def compute_domain_distribution(df_plates: pd.DataFrame, total_items: int) -> pd.DataFrame:
    """Distribuição por domínio dos plates (count, %) + % dos itens totais."""
    if df_plates.empty:
        return pd.DataFrame(columns=["domain","count","pct_of_plates","pct_of_total_items"])
    domain_counts = df_plates["domain"].fillna("UNKNOWN").value_counts().rename_axis("domain").reset_index(name="count")
    sum_plates = int(domain_counts["count"].sum())
    domain_counts["pct_of_plates"] = (domain_counts["count"] / max(sum_plates, 1) * 100).round(1)
    domain_counts["pct_of_total_items"] = (domain_counts["count"] / max(total_items, 1) * 100).round(1)
    return domain_counts

# ---------- outputs ----------
def write_excel(out_xlsx: Path, sheets: Dict[str, pd.DataFrame]) -> None:
    out_xlsx.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as xw:
        for name, df in sheets.items():
            df = df.copy()
            # melhora visual: limita largura das colunas onde possível
            df.to_excel(xw, index=False, sheet_name=name[:31])
    print(f"💾 Excel written to: {out_xlsx.resolve()}")

def write_postgres(pg_url: str, sheets: Dict[str, pd.DataFrame], schema: str, replace: bool) -> None:
    """
    Escreve no PostgreSQL.
    - Se replace=True: DROP das tabelas e recriação.
    - UPSERT (DO NOTHING) para: documents, spec_sections, detail_plates.
    - to_sql(..., append) para: ejcdc_articles, line_items, line_item_tokens, domain_distribution.
    """
    eng = create_engine(pg_url)
    with eng.begin() as conn:
        # Schema
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'))

        # Limpa se replace
        if replace:
            for t in ["documents","spec_sections","detail_plates","ejcdc_articles",
                      "line_items","line_item_tokens","domain_distribution"]:
                conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{t}" CASCADE;'))

        # DDLs
        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS "{schema}"."documents"(
                document_id TEXT PRIMARY KEY,
                title TEXT, edition_year TEXT, jurisdiction TEXT,
                source_path TEXT, standard_base TEXT, doc_type TEXT
            );
        '''))

        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS "{schema}"."spec_sections"(
                code TEXT PRIMARY KEY,
                title TEXT
            );
        '''))

        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS "{schema}"."detail_plates"(
                code TEXT PRIMARY KEY,
                title TEXT,
                domain TEXT
            );
        '''))

        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS "{schema}"."ejcdc_articles"(
                article_no INTEGER,
                heading TEXT
            );
        '''))

        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS "{schema}"."line_items"(
                code TEXT,
                title TEXT,
                domain TEXT,
                item_type TEXT,
                description TEXT,
                quantity NUMERIC,
                unit_price NUMERIC,
                total NUMERIC,
                title_clean TEXT
            );
        '''))

        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS "{schema}"."line_item_tokens"(
                token TEXT,
                count INTEGER,
                pct_of_items NUMERIC
            );
        '''))

        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS "{schema}"."domain_distribution"(
                domain TEXT,
                count INTEGER,
                pct_of_plates NUMERIC,
                pct_of_total_items NUMERIC
            );
        '''))

        # -------- UPSERT (DO NOTHING) para as tabelas com PK --------
        docs = sheets.get("documents")
        if docs is not None and not docs.empty:
            payload = docs.to_dict(orient="records")
            conn.execute(text(f'''
                INSERT INTO "{schema}"."documents"
                    (document_id, title, edition_year, jurisdiction, source_path, standard_base, doc_type)
                VALUES
                    (:document_id, :title, :edition_year, :jurisdiction, :source_path, :standard_base, :doc_type)
                ON CONFLICT (document_id) DO NOTHING;
            '''), payload)

        secs = sheets.get("spec_sections")
        if secs is not None and not secs.empty:
            payload = secs.rename(columns={"code":"code", "title":"title"}).to_dict(orient="records")
            conn.execute(text(f'''
                INSERT INTO "{schema}"."spec_sections" (code, title)
                VALUES (:code, :title)
                ON CONFLICT (code) DO NOTHING;
            '''), payload)

        plates = sheets.get("detail_plates")
        if plates is not None and not plates.empty:
            payload = plates.rename(columns={"code":"code","title":"title","domain":"domain"}).to_dict(orient="records")
            conn.execute(text(f'''
                INSERT INTO "{schema}"."detail_plates" (code, title, domain)
                VALUES (:code, :title, :domain)
                ON CONFLICT (code) DO NOTHING;
            '''), payload)

        # -------- Demais tabelas: append normal --------
        for name, table in [
            ("ejcdc_articles","ejcdc_articles"),
            ("line_items","line_items"),
            ("line_item_tokens","line_item_tokens"),
            ("domain_distribution","domain_distribution"),
        ]:
            df = sheets.get(name)
            if df is not None and not df.empty:
                df.to_sql(table, conn, schema=schema, if_exists="append",
                          index=False, method="multi", chunksize=1000)

# ---------- main ----------
def main() -> None:
    load_dotenv()

    ap = argparse.ArgumentParser(description="Jordan Specs → Excel e/ou PostgreSQL (local/S3/URL).")
    srcgrp = ap.add_mutually_exclusive_group(required=True)
    srcgrp.add_argument("--pdf", help="Caminho local do PDF")
    srcgrp.add_argument("--s3-uri", help="s3://bucket/path/file.pdf")
    srcgrp.add_argument("--url", help="https://host/path/file.pdf")

    ap.add_argument("--out-xlsx", help="Caminho do Excel de saída")
    ap.add_argument("--pg-url", help="Postgres URL (ou use PG_URL no .env)")
    ap.add_argument("--schema", default="public", help="Schema alvo (default: public)")
    ap.add_argument("--replace", action="store_true", help="(Re)criar/limpar tabelas antes de inserir")
    args = ap.parse_args()

    # Conexão
    pg_url = args.pg_url or os.getenv("PG_URL")

    # Fonte
    pdf_path = resolve_input_source(args.pdf, args.s3_uri, args.url)

    # -------- Extract --------
    pages = read_pdf_lines(pdf_path)
    meta = extract_metadata(pages, pdf_path)
    df_docs = pd.DataFrame([meta])
    df_sections, df_plates, df_articles = extract_structured(pages)
    df_items = make_line_items(df_sections, df_plates)

    # -------- Transform --------
    df_items["title_clean"] = df_items["title"].apply(clean_title)
    tokens = explode_tokens(df_items["title_clean"]).value_counts().rename_axis("token").reset_index(name="count")
    total_items = max(len(df_items), 1)
    tokens["pct_of_items"] = (tokens["count"] / total_items * 100).round(1)
    df_domain = compute_domain_distribution(df_plates, len(df_items))

    # -------- Pack outputs --------
    sheets = {
        "documents": df_docs,
        "spec_sections": df_sections,
        "detail_plates": df_plates,
        "ejcdc_articles": df_articles,
        "line_items": df_items,
        "line_item_tokens": tokens,
        "domain_distribution": df_domain,
    }

    # Excel
    if args.out_xlsx:
        out = Path(args.out_xlsx)
        out.parent.mkdir(parents=True, exist_ok=True)
        write_excel(out, sheets)

    # PostgreSQL
    if pg_url:
        write_postgres(pg_url, sheets, schema=args.schema, replace=args.replace)
        print(f"🐘 PostgreSQL populated (schema={args.schema}).")

    if not args.out_xlsx and not pg_url:
        print("⚠️ Nenhum destino informado. Use --out-xlsx e/ou --pg-url / PG_URL no .env.")

if __name__ == "__main__":
    main()
