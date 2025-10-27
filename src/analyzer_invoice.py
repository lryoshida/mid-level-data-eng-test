#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

def main():
    load_dotenv()
    ap = argparse.ArgumentParser(description="Generate a Markdown report from details_invoice.")
    ap.add_argument("--pg-url", help="Postgres URL (or PG_URL in .env)")
    ap.add_argument("--schema", default="public")
    args = ap.parse_args()

    pg_url = args.pg_url or os.getenv("PG_URL")
    if not pg_url: raise SystemExit("Set PG_URL in .env or pass --pg-url.")

    eng = create_engine(pg_url)
    schema = args.schema

    df = pd.read_sql_table("details_invoice", con=eng, schema=schema)
    if df.empty:
        raise SystemExit("details_invoice is empty.")

    # Basic metrics
    n_rows = len(df)
    total_value = float(df["total_price"].fillna(0).sum())

    items_by_value = (
        df.groupby("item", dropna=False)["total_price"]
          .sum().sort_values(ascending=False)
          .reset_index().rename(columns={"total_price":"sum_total"})
    )

    hsn_dist = (
        df.groupby("hsn_code", dropna=False)["total_price"]
          .sum().sort_values(ascending=False)
          .reset_index().rename(columns={"total_price":"sum_total"})
    )

    # Save auxiliary CSVs
    out_dir = Path("reports"); out_dir.mkdir(parents=True, exist_ok=True)
    items_by_value.to_csv(out_dir / "items_by_value.csv", index=False)
    hsn_dist.to_csv(out_dir / "hsn_distribution.csv", index=False)

    # Markdown report
    md = []
    md.append("# Invoice Report")
    md.append("")
    md.append(f"- Item rows (total): **{n_rows}**")
    md.append(f"- Grand total (`sum(total_price)`): **${total_value:,.2f}**")
    md.append("")
    md.append("## Top 5 items by value")
    md.append("")
    md.append("| Item | Value |")
    md.append("|------|------:|")
    for _, r in items_by_value.head(5).iterrows():
        md.append(f"| {r['item']} | ${float(r['sum_total']):,.2f} |")
    md.append("")
    md.append("## Distribution by HSN Code")
    md.append("")
    md.append("| HSN Code | Value |")
    md.append("|----------|------:|")
    for _, r in hsn_dist.iterrows():
        md.append(f"| {r['hsn_code']} | ${float(r['sum_total']):,.2f} |")
    md.append("")
    md.append("_Auxiliary files_: `reports/items_by_value.csv`, `reports/hsn_distribution.csv`")

    out_md = out_dir / "invoice_report.md"
    out_md.write_text("\n".join(md), encoding="utf-8")
    print(f"📝 Report written to: {out_md.resolve()}")

if __name__ == "__main__":
    main()
