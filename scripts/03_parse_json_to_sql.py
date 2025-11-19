import json
import os

# Configuration
INPUT_FILE = "output.json"
OUTPUT_FILE = "output.sql"

def format_sql_value(val):
    """
    Helper to format Python values into SQL strings.
    None -> NULL
    String -> 'Safe String' (escaped)
    Numbers -> String representation
    """
    if val is None:
        return "NULL"
    if isinstance(val, str):
        # Escape single quotes by doubling them (Standard SQL escaping)
        safe_val = val.replace("'", "''")
        # Handle backslashes if necessary
        safe_val = safe_val.replace("\\", "\\\\")
        return f"'{safe_val}'"
    return str(val)

def generate_sql_script():
    # 1. Check if input file exists
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Error: {INPUT_FILE} not found. Please place your JSON file in the same directory.")
        return

    # 2. Load JSON
    try:
        with open(INPUT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"❌ Error decoding JSON: {e}")
        return

    print(f"🔄 Processing {len(data)} records from {INPUT_FILE}...")

    # 3. Open Output File
    with open(OUTPUT_FILE, "w", encoding="utf-8") as sql_file:
        
        # Header
        sql_file.write("-- ==================================================\n")
        f_header = f"-- SQL INSERT SCRIPT GENERATED FROM {INPUT_FILE}\n"
        sql_file.write(f_header)
        sql_file.write("-- ==================================================\n\n")
        sql_file.write("USE hospital_analytics_texas;\n\n")

        # 4. Iterate through every record in the JSON
        for row_id, record in data.items():
            sql_file.write(f"-- [TRANSACTION START] Record ID: {row_id}\n")
            
            # --- A. DIMENSION: PATIENT ---
            # We use ON DUPLICATE KEY UPDATE to handle duplicates gracefully
            # We capture the ID into a MySQL variable @pat_id
            p = record.get("dim_patient", {})
            sql_pat = f"""
INSERT INTO dim_patient (full_name, gender, birthdate, city, state, zip)
VALUES ({format_sql_value(p.get('full_name'))}, {format_sql_value(p.get('gender'))}, {format_sql_value(p.get('birthdate'))}, {format_sql_value(p.get('city'))}, {format_sql_value(p.get('state'))}, {format_sql_value(p.get('zip'))})
ON DUPLICATE KEY UPDATE patient_key=LAST_INSERT_ID(patient_key);
SET @pat_id = LAST_INSERT_ID();
"""
            sql_file.write(sql_pat.strip() + "\n")

            # --- B. DIMENSION: PROVIDER ---
            dr = record.get("dim_provider", {})
            sql_prov = f"""
INSERT INTO dim_provider (name, specialty)
VALUES ({format_sql_value(dr.get('name'))}, {format_sql_value(dr.get('specialty'))})
ON DUPLICATE KEY UPDATE provider_key=LAST_INSERT_ID(provider_key);
SET @prov_id = LAST_INSERT_ID();
"""
            sql_file.write(sql_prov.strip() + "\n")

            # --- C. DIMENSION: ORGANIZATION ---
            org = record.get("dim_organization", {})
            sql_org = f"""
INSERT INTO dim_organization (name, city, state)
VALUES ({format_sql_value(org.get('name'))}, {format_sql_value(org.get('city'))}, {format_sql_value(org.get('state'))})
ON DUPLICATE KEY UPDATE org_key=LAST_INSERT_ID(org_key);
SET @org_id = LAST_INSERT_ID();
"""
            sql_file.write(sql_org.strip() + "\n")

            # --- D. DIMENSION: PAYER ---
            ins = record.get("dim_payer", {})
            sql_payer = f"""
INSERT INTO dim_payer (name)
VALUES ({format_sql_value(ins.get('name'))})
ON DUPLICATE KEY UPDATE payer_key=LAST_INSERT_ID(payer_key);
SET @payer_id = LAST_INSERT_ID();
"""
            sql_file.write(sql_payer.strip() + "\n")

            # --- E. FACT TABLE: EVENTS ---
            # We reuse the MySQL variables (@pat_id, @prov_id, etc.) instead of Python values
            # --- E. FACT TABLE: EVENTS ---
            events = record.get("fact_patient_events", [])
            for event in events:
                # 1. ENSURE DATE EXISTS
                event_date = event.get('event_date')
                if event_date:
                    # We insert the date first. "INSERT IGNORE" skips if it already exists.
                    # We only insert the key; other columns (year, month) can be filled later or via triggers
                    sql_date = f"INSERT IGNORE INTO dim_date (date_key) VALUES ({format_sql_value(event_date)});"
                    sql_file.write(sql_date + "\n")

                # 2. INSERT FACT
                sql_fact = f"""
INSERT INTO fact_patient_events 
(patient_key, provider_key, org_key, payer_key, date_key, event_category, encounter_id, code, description, numeric_value, units, cost)
VALUES 
(@pat_id, @prov_id, @org_id, @payer_id, {format_sql_value(event_date)}, {format_sql_value(event.get('event_category'))}, {format_sql_value(event.get('encounter_id'))}, {format_sql_value(event.get('code'))}, {format_sql_value(event.get('description'))}, {format_sql_value(event.get('numeric_value'))}, {format_sql_value(event.get('units'))}, {format_sql_value(event.get('cost'))});
"""
                sql_file.write(sql_fact.strip() + "\n")

            sql_file.write("-- [TRANSACTION END]\n\n")

    print(f"✅ Successfully generated: {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_sql_script()