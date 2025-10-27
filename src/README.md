# 🧾 PDF & Invoice ETL Mini-Pipeline

A small Python-based ETL pipeline for extracting, transforming, and optionally loading structured data from **PDFs** into **PostgreSQL** and **Excel reports**.

It supports:
- **Engineering specifications** (Jordan 2019 Standard Specifications)
- **Invoices / receipts** (table-like PDFs)
- Optional **analysis and reporting** modules

Works with **local files**, **AWS S3**, or **direct URLs**.

---

## 📁 Project Structure

```
.
├── src/
│   ├── pdf_processor.py        # Extract and process Jordan specifications
│   ├── invoice_processor.py    # Extract and process invoice line items
│   ├── analyzer_pdf.py         # Analyze and summarize Jordan data (optional)
│   ├── analyzer_invoice.py     # Analyze invoice data and KPIs (optional)
│   └── main.py                 # Main orchestrator for tasks
│
├── data/
│   ├── input/                  # Input PDFs
│   └── output/                 # Output Excel/CSV files
│
├── reports/                    # Optional reports or generated summaries
├── requirements.txt
└── README.md
```

---

## ⚙️ Requirements

- **Python 3.10+** (3.11 recommended)
- (Optional) **PostgreSQL** local or remote instance
- (Optional) `.env` file containing:

```
PG_URL=postgresql+psycopg2://postgres:<password>@127.0.0.1:5432/postgress
```

---

## 🧩 Installation

```bash
python -m venv .venv
# Activate virtual environment:
# Windows:
. .venv/Scripts/activate
# Linux/Mac:
source .venv/bin/activate

pip install -r requirements.txt
```

---

## 🚀 1. Extract JORDAN Specifications (PDF → Excel / PostgreSQL)

### **Local PDF → Excel**
```bash
python src/pdf_processor.py --pdf "data/input/Jordan-2019-Standard-Specifications-for-Construction.pdf"   --out-xlsx "data/output/jordan_specs_extracted.xlsx"
```

### **Local PDF → PostgreSQL**
```bash
python src/pdf_processor.py --pdf "data/input/Jordan-2019-Standard-Specifications-for-Construction.pdf"   --pg-url "postgresql+psycopg2://postgres:<password>@127.0.0.1:5432/postgress"   --schema public --replace
```

### **From S3**
```bash
python src/pdf_processor.py --s3-uri "s3://my-bucket/pdfs/Jordan-2019.pdf"   --pg-url "postgresql+psycopg2://postgres:<password>@127.0.0.1:5432/postgress" --schema public
```

### **From URL**
```bash
python src/pdf_processor.py --url "https://example.com/Jordan-2019.pdf"   --out-xlsx "data/output/jordan_specs_extracted.xlsx"
```

**Tables created:**
- `documents`
- `spec_sections`
- `detail_plates`
- `ejcdc_articles`
- `line_items`
- `line_item_tokens`
- `domain_distribution`

---

## 🧾 2. Extract INVOICE Data (PDF → Excel / PostgreSQL)

### **Local PDF → PostgreSQL**
```bash
python src/invoice_processor.py --input "data/input/invoices/"   --pg-url "postgresql+psycopg2://postgres:<password>@127.0.0.1:5432/postgress"   --schema public --replace
```

### **Local PDF → Excel**
```bash
python src/invoice_processor.py --input "data/input/invoices/"   --out-xlsx "data/output/invoices_extracted.xlsx"
```

### **From S3**
```bash
python src/invoice_processor.py --s3-uri "s3://my-bucket/invoices/invoice1.pdf"   --pg-url "postgresql+psycopg2://postgres:<password>@127.0.0.1:5432/postgress"   --schema public
```

**Generated table:**  
`public.details_invoice (id, item, hsn_code, quantity, unit_price, total_price, etc.)`

---

## 🧠 3. Analysis Modules (Optional)

After data is inserted into PostgreSQL:

### Jordan Data Analysis
```bash
python src/analyzer_pdf.py --pg-url "postgresql+psycopg2://postgres:<password>@127.0.0.1:5432/postgress" --schema public
```

### Invoice Data Analysis
```bash
python src/analyzer_invoice.py --pg-url "postgresql+psycopg2://postgres:<password>@127.0.0.1:5432/postgress" --schema public
```

These generate analytical summaries and optional reports under `reports/`.

---

## 🧩 4. Full Orchestration with main.py

Instead of calling each processor separately, use `main.py` to orchestrate tasks.

### **Jordan (PDF Processor)**
```bash
python src/main.py --task jordan --pg-url "postgresql+psycopg2://postgres:<password>@127.0.0.1:5432/postgress"
```

### **Invoice (Invoice Processor)**
```bash
python src/main.py --task invoice --pg-url "postgresql+psycopg2://postgres:<password>@127.0.0.1:5432/postgress"
```

You can also specify:
- `--out-xlsx` → custom Excel output name
- `--replace` → recreate tables before loading

---

## 🧱 Database Notes

- **Database is optional** – if no `--pg-url` or `.env` is provided, output is written only to Excel.  
- Use `--replace` to recreate tables (useful for clean runs).  
- Schema defaults to `public`.

---

## 🪣 S3 Integration

To use S3 repositories:
```bash
pip install boto3
```
Then configure your credentials (AWS CLI or environment variables):
```bash
aws configure
```

You can now use:
```bash
--s3-uri "s3://bucket/path/file.pdf"
```

---

## 📊 Outputs

All generated files are saved under:
```
data/output/
```

| Type | Example File | Description |
|------|---------------|--------------|
| Excel | `jordan_specs_extracted.xlsx` | Extracted specification tables |
| Excel | `invoices_extracted.xlsx` | Extracted invoice line items |
| Report | `reports/report_invoice.csv` | Optional analytical summary |

---

## 🧩 Troubleshooting

| Issue | Cause | Solution |
|--------|--------|-----------|
| `OperationalError` | PostgreSQL not running or wrong credentials | Check `pg_hba.conf` and service status |
| `UniqueViolation` | Existing data with same primary key | Use `--replace` to overwrite |
| `FileNotFoundError` | Wrong path to PDF | Check path or use absolute path |
| `psql: not recognized` | PostgreSQL not in PATH | Use full executable path (e.g. `"C:\Program Files\PostgreSQL\15\bin\psql.exe"`) |

---

## 📦 Requirements

**requirements.txt**
```
pandas
PyPDF2
sqlalchemy
psycopg2-binary
python-dotenv
xlsxwriter
requests
boto3
openpyxl
```
