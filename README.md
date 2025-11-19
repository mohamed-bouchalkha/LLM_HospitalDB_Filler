# Texas Star Schema ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline to process medical/synthetic data. It utilizes Large Language Models (Anthropic, Google Gemini, Groq) for data extraction, parses JSON/SQL data, integrates Synthea CSV datasets, and loads them into a MySQL Star Schema.

## 🛠️ Prerequisites

  * **Python** (3.8 or higher)
  * **MySQL Server** (via XAMPP, WAMP, or standalone)
  * **phpMyAdmin** (or any SQL client)

## 📦 Installation & Setup

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/mohamed-bouchalkha/LLM_HospitalDB_Filler
    ```

2.  **Create and activate a virtual environment:**

    ```bash
    # Create the virtual environment
    python -m venv .venv

    # Activate (Windows)
    .venv\Scripts\activate

    # Activate (macOS/Linux)
    # source .venv/bin/activate
    ```

3.  **Install dependencies:**

    ```bash
    pip install mysql-connector-python pandas anthropic python-dotenv google-generativeai groq
    ```

4.  **Environment Configuration:**
    Create a `.env` file in the root directory to store your API keys and database credentials.

    ```text
    # Example .env structure
    DB_HOST=localhost
    DB_USER=root
    DB_PASSWORD=
    DB_NAME=texas_star_schema

    # LLM Keys
    ANTHROPIC_API_KEY=your_key_here
    GOOGLE_API_KEY=your_key_here
    GROQ_API_KEY=your_key_here
    ```

## 🗄️ Database Initialization

Before running the pipeline, you must set up the database schema.

1.  Open **phpMyAdmin**.
2.  Create a database (e.g., `texas_star_schema`).
3.  Import the schema file provided in the root directory:
      * **File:** `texas_star_schema.sql`

## 🚀 Usage

You can run the project in **Auto Mode** (all-in-one) or **Manual Mode** (step-by-step).

### Option 1: Automated Pipeline

Run the master pipeline script to execute the entire workflow automatically:

```bash
python pipeline.py
```

### Option 2: Manual Step-by-Step Execution

If you need to debug or run specific stages individually, execute the scripts in the following order:

1.  **Pre-treatment:**
    ```bash
    python scripts\01_pretreatement_data.py
    ```
2.  **LLM Extraction:**
    ```bash
    python scripts\02_extraction_llm.py
    ```
3.  **Parse JSON to SQL:**
    ```bash
    python scripts\03_parse_json_to_sql.py
    ```
4.  **Insert Parsed SQL Data:**
    ```bash
    python scripts\04_parse_sql.py SQL\insert_data_v1.sql
    python scripts\04_parse_sql.py SQL\insert_data_v2.sql
    python scripts\04_parse_sql.py SQL\insert_data_v3.sql
    python scripts\04_parse_sql.py SQL\insert_data_v4.sql
    ```
5.  **Load Synthea Data:**
    ```bash
    python scripts\05_load_synthea_csv.py
    ```
6.  **Cleaning & Deduplication:**
    ```bash
    python scripts\06_clean_and_dedup.py
    ```
7.  **Validation & Enrichment:**
    ```bash
    python scripts\07_validate_and_enrich.py
    ```

## 📂 Project Structure

```text
├── .venv/                  # Virtual Environment
├── SQL/                    # SQL Insert files (v1-v4)
├── ressources/             # the data ressources for the project
├── Clean Data CSV/         # the final clean data 
├── scripts/                # Step-by-step processing scripts
│   ├── 01_pretreatement_data.py
│   ├── 02_extraction_llm.py
│   ├── 03_parse_json_to_sql.py
│   ├── 04_parse_sql.py
│   ├── 05_load_synthea_csv.py
│   ├── 06_clean_and_dedup.py
│   └── 07_validate_and_enrich.py
├── pipeline.py             # Orchestrator script
├── .env                    # Environment variables
└── README.md
```

-----