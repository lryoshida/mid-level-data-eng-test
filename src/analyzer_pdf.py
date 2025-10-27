#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyze_from_pg.py
Análise mínima lendo direto do PostgreSQL (sem excel).

Uso:
  # usando .env (PG_URL=postgresql+psycopg2://user:pass@host:5432/db)
  python src/analyze_from_pg.py --schema public

  # passando a URL na linha de comando
  python src/analyze_from_pg.py --pg-url "postgresql+psycopg2://postgres:senha@localhost:5432/postgres" --schema public

  # salvar CSVs simples
  python src/analyze_from_pg.py --schema public --save-csv
"""
import argparse
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


def main():
    load_dotenv()  # lê PG_URL se existir .env
    ap = argparse.ArgumentParser(description="Basic analysis directly from PostgreSQL.")
    ap.add_argument("--pg-url", type=str, help="URL do PostgreSQL (ex.: postgresql+psycopg2://user:pass@host:5432/db)")
    ap.add_argument("--schema", default="public", help="Schema do PostgreSQL (default: public)")
    ap.add_argument("--top-n", type=int, default=10, help="Top N tokens para títulos limpos.")
    ap.add_argument("--save-csv", action="store_true", help="Salvar CSVs com métricas (na pasta ./data/output/).")
    args = ap.parse_args()

    pg_url = args.pg_url or os.getenv("PG_URL")
    if not pg_url:
        raise SystemExit("❌ Defina --pg-url ou a variável de ambiente PG_URL (.env).")

    schema = args.schema
    out_dir = Path("data/output")
    out_dir.mkdir(parents=True, exist_ok=True)

    engine = create_engine(pg_url)

    # -------- métricas básicas --------
    with engine.connect() as conn:
        # contagens simples
        q_counts = text(f"""
            SELECT
              (SELECT COUNT(*) FROM "{schema}"."documents")      AS documents,
              (SELECT COUNT(*) FROM "{schema}"."spec_sections")  AS spec_sections,
              (SELECT COUNT(*) FROM "{schema}"."detail_plates")  AS detail_plates,
              (SELECT COUNT(*) FROM "{schema}"."line_items")     AS work_items
        """)
        counts = pd.read_sql(q_counts, conn)

        # distribuição por domínio dos plates (count, % of plates, % of total items)
        q_domain = text(f"""
            WITH c AS (
              SELECT domain, COUNT(*) AS count
              FROM "{schema}"."detail_plates"
              GROUP BY domain
            ), totals AS (
              SELECT
                (SELECT SUM(count) FROM c) AS total_plates,
                (SELECT COUNT(*) FROM "{schema}"."line_items") AS total_items
            )
            SELECT
              c.domain,
              c.count,
              ROUND(100.0 * c.count / NULLIF(t.total_plates,0), 1) AS pct_of_plates,
              ROUND(100.0 * c.count / NULLIF(t.total_items,0), 1)  AS pct_of_total_items
            FROM c CROSS JOIN totals t
            ORDER BY c.count DESC, c.domain
        """)
        domain_df = pd.read_sql(q_domain, conn)

        # top-N tokens a partir de title_clean
        # (split em espaço; ignora vazio e termos curtos)
        q_tokens = text(f"""
            WITH tokens AS (
              SELECT UPPER(tok) AS tok
              FROM "{schema}"."line_items" li
              CROSS JOIN LATERAL regexp_split_to_table(COALESCE(li.title_clean, ''), '\\s+') AS t(tok)
            )
            SELECT tok AS token, COUNT(*) AS count
            FROM tokens
            WHERE length(tok) >= 3 AND tok <> ''
            GROUP BY tok
            ORDER BY count DESC, token
            LIMIT :topn
        """)
        top_tokens = pd.read_sql(q_tokens, conn, params={"topn": args.top_n})

        # distribuição por item_type (spec_section x detail_plate)
        q_types = text(f"""
            SELECT item_type, COUNT(*) AS n
            FROM "{schema}"."line_items"
            GROUP BY item_type
            ORDER BY n DESC
        """)
        item_types = pd.read_sql(q_types, conn)

    # -------- print no console --------
    print("=== BASIC METRICS (from PostgreSQL) ===")
    print(f"📄 Documents processed: {int(counts.at[0,'documents'])}")
    print(f"📑 Total spec sections: {int(counts.at[0,'spec_sections'])}")
    print(f"📐 Total detail plates: {int(counts.at[0,'detail_plates'])}")
    print(f"🧾 Total Work Items:    {int(counts.at[0,'work_items'])}")
    print("💰 Total contract/invoice value: N/A (technical spec)\n")

    if not domain_df.empty:
        print("🏷️ Category Distribution (detail plates):")
        for _, r in domain_df.iterrows():
            print(f" - {r['domain']}: {int(r['count'])} items "
                  f"({r['pct_of_plates']}% of plates; {r['pct_of_total_items']}% of total)")
        print()
    else:
        print("⚠️ Nenhuma categoria encontrada em 'detail_plates'.\n")

    print(f"🔠 Most common words in line item titles (top {args.top_n}):")
    if top_tokens.empty:
        print(" (sem dados)\n")
    else:
        print(top_tokens.to_string(index=False))
        print()

    print("📊 Distribution by item_type:")
    if item_types.empty:
        print(" (sem dados)\n")
    else:
        print(item_types.to_string(index=False))
        print()

    # -------- CSVs opcionais --------
    if args.save_csv:
        counts.to_csv(out_dir / "basic_metrics_pg.csv", index=False)
        domain_df.to_csv(out_dir / "domain_distribution_pg.csv", index=False)
        top_tokens.to_csv(out_dir / "line_item_top_tokens_pg.csv", index=False)
        item_types.to_csv(out_dir / "item_type_distribution_pg.csv", index=False)
        print(f"💾 CSVs salvos em: {out_dir.resolve()}")


if __name__ == "__main__":
    main()

