# 🏥 Texas Hospital Analytics - ETL Pipeline

This project implements a robust ETL (Extract, Transform, Load) pipeline to migrate, clean, and enrich Texas hospital data into a MySQL Data Warehouse using a Star Schema. It utilizes Python for orchestration, SQL for data definition, and GenAI for data enrichment.

## 📋 Prerequisites

Before running the project, ensure you have the following installed:

  * **Python 3.8+**
  * **MySQL Server** (via XAMPP, WAMP, or standalone)
  * **Git** (optional, for cloning)

## ⚙️ Installation

1.  **Clone the repository** (if applicable) or navigate to the project folder.

2.  **Set up the Virtual Environment**:

    ```bash
    # Create virtual environment
    python -m venv .venv

    # Activate environment (Windows)
    .venv\Scripts\activate

    # Activate environment (Mac/Linux)
    source .venv/bin/activate
    ```

3.  **Install Dependencies**:

    ```bash
    pip install mysql-connector-python pandas anthropic python-dotenv google-generativeai
    ```

4.  **Environment Configuration**:
    Create a `.env` file in the root directory to store your database credentials and API keys:

    ```env
    DB_HOST=localhost
    DB_USER=root
    DB_PASS=
    DB_NAME=hospital_analytics_texas
    API_KEY=your_api_key_here
    ```

-----

## 🗄️ Database Setup

**Crucial Step:** You must create the database schema before running the pipeline.

1.  Open **PhpMyAdmin** or your preferred SQL client (MySQL Workbench, DBeaver).
2.  Create a database named `hospital_analytics_texas` (if not exists).
3.  Import/Execute the schema file:
      * File: `SQL\texas_star_schema.sql`

-----

## 🚀 Usage

You can run the entire project using the automated pipeline or execute steps manually.

### Option 1: Automated Pipeline (Recommended)

Run the master orchestration script. This handles all steps sequentially, includes error handling, and provides execution timers.

```bash
python pipeline.py
```

### Option 2: Manual Execution

If you need to debug specific steps or run them individually, execute the scripts in this order:

**1. Pre-processing & Parsing**

```bash
# Prepare raw data
python scripts\1_pretreatement_data.py

# Convert JSON sources to SQL format
python scripts\2_parse_json_to_sql.py
```

**2. SQL Batch Loading**
*Note: The parser script is reused to handle large SQL dumps in batches to prevent memory overflows.*

```bash
python scripts\3_parse_sql.py SQL\insert_data_v1.sql
python scripts\3_parse_sql.py SQL\insert_data_v2.sql
python scripts\3_parse_sql.py SQL\insert_data_v3.sql
python scripts\3_parse_sql.py SQL\insert_data_v4.sql
```

**3. ETL & Validation**

```bash
# Load CSV data into Dimensions and Fact tables
python scripts\4_etl_pipeline_csv.py

# Run SQL cleaning scripts (Deduplication, Null handling)
python scripts\5_clean_data.py

# Perform Logic Validation & AI Data Enrichment
python scripts\6_validate.py
```

-----

## 📂 Project Structure

  * **`SQL/`**: Contains schema definitions (`texas_star_schema.sql`) and generated data inserts.
  * **`scripts/`**:
      * `1_pretreatement_data.py`: Initial raw data formatting.
      * `2_parse_json_to_sql.py`: Parsers for complex JSON structures.
      * `3_parse_sql.py`: Robust SQL executor with delimiter handling.
      * `4_etl_pipeline_csv.py`: Main ETL logic using Pandas.
      * `5_clean_data.py`: Database-level cleaning procedures.
      * `6_validate.py`: Data integrity checks and LLM-based missing value imputation.
  * **`pipeline.py`**: Master script to run the full workflow.