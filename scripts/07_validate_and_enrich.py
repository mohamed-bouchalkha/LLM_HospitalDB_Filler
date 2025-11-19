import os
import mysql.connector
import json
import re
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from groq import Groq
from dotenv import load_dotenv
from tqdm import tqdm  

# --- CONFIGURATION ---
load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
AI_MODEL = "llama-3.3-70b-versatile" 

DB_CONFIG = {
    'host': os.getenv("DB_HOST", "localhost"), 
    'user': os.getenv("DB_USER", "root"),
    'password': os.getenv("DB_PASS", ""), 
    'database': os.getenv("DB_NAME", "hospital_analytics_texas")
}

# Constants
BATCH_SIZE = 5000
MAX_WORKERS = 15  # Number of parallel AI requests

# --- SMART DATA ASSETS ---
TEXAS_ZIPS_MAP = {
    'AUSTIN': '78701', 'HOUSTON': '77002', 'DALLAS': '75201',
    'SAN ANTONIO': '78205', 'FORT WORTH': '76102', 'EL PASO': '79901',
    'ARLINGTON': '76010', 'CORPUS CHRISTI': '78401', 'PLANO': '75074',
    'LAREDO': '78040'
}
TEXAS_ZIPS_FALLBACK = ['75001', '73301', '77001', '78745', '75241', '75551', '76087', '79108']

def get_groq_client():
    if not GROQ_API_KEY: return None
    return Groq(api_key=GROQ_API_KEY)

# ==========================================
# PHASE 1: REGEX CLEANING (BATCHED)
# ==========================================
def clean_names_regex(conn):
    """Removes numbers from Provider names using Batch Updates."""
    cursor = conn.cursor(dictionary=True)
    print("\n🧼 Running Advanced Regex Cleaner (Providers)...")

    # 1. Fetch Data
    cursor.execute("SELECT provider_key, name FROM dim_provider WHERE name REGEXP '[0-9]'")
    providers = cursor.fetchall()

    if not providers:
        print("   > No providers found with numeric characters. Clean.")
        return

    # 2. Process in Memory
    updates = []
    for p in providers:
        clean_name = re.sub(r'\d+', '', p['name']).strip()
        if clean_name != p['name']:
            updates.append((clean_name, p['provider_key']))

    # 3. Batch Update
    if updates:
        print(f"   > Batch updating {len(updates)} records...")
        sql = "UPDATE dim_provider SET name = %s WHERE provider_key = %s"
        
        cursor = conn.cursor() # Switch to normal cursor for batching
        for i in range(0, len(updates), BATCH_SIZE):
            batch = updates[i:i + BATCH_SIZE]
            cursor.executemany(sql, batch)
            conn.commit()
            
    print(f"   > ✅ Fixed {len(updates)} Provider names.")

# ==========================================
# PHASE 2: SMART ENRICHMENT (PARALLEL & BATCHED)
# ==========================================

def fetch_ai_name(client, specialty):
    """Worker function for threading"""
    prompt = f"Generate a realistic doctor name (e.g. Dr. John Smith) for a specialist in {specialty}. Output ONLY the name. Do not use 'Sample' or single letters."
    try:
        resp = client.chat.completions.create(
            model=AI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.9,
            max_tokens=20
        )
        name = resp.choices[0].message.content.strip().replace('"', '').replace('.', '')
        
        # Cleaning
        if "Dr" not in name: name = f"Dr. {name}"
        if name.startswith("Dr") and not name.startswith("Dr."): name = name.replace("Dr", "Dr.")
        return name
    except:
        return "Dr. Unknown"

def run_smart_enrichment(conn):
    client = get_groq_client()
    cursor = conn.cursor(dictionary=True)
    print("\n🧠 Starting Smart Data Enrichment...")

    # --- A. ZIP CODES (BATCHED) ---
    sql_missing_zips = """
        SELECT patient_key, city 
        FROM dim_patient 
        WHERE zip IS NULL OR zip = '' OR zip = '0' OR LENGTH(zip) < 5
    """
    cursor.execute(sql_missing_zips)
    patients = cursor.fetchall()

    if patients:
        print(f"   ... Processing {len(patients)} missing ZIP codes...")
        zip_updates = []
        for p in patients:
            city_key = p['city'].upper() if p['city'] else ''
            new_zip = TEXAS_ZIPS_MAP.get(city_key, random.choice(TEXAS_ZIPS_FALLBACK))
            zip_updates.append((new_zip, p['patient_key']))
        
        # Batch Update
        sql_update = "UPDATE dim_patient SET zip = %s WHERE patient_key = %s"
        cursor = conn.cursor()
        for i in tqdm(range(0, len(zip_updates), BATCH_SIZE), desc="   💾 Updating Zips"):
            batch = zip_updates[i:i + BATCH_SIZE]
            cursor.executemany(sql_update, batch)
            conn.commit()
    else:
        print("   > Zip codes appear fully populated.")

    # --- B. PROVIDER NAMES (PARALLEL LLM) ---
    if client:
        cursor = conn.cursor(dictionary=True)
        sql_provider_fix = """
            SELECT provider_key, specialty 
            FROM dim_provider 
            WHERE name IS NULL OR name = '' OR name LIKE '%Unknown%' 
               OR name IN ('Dr. XYZ', 'Dr. ABC', 'Dr. X', 'Dr. W', 'Dr. Sample Doctor')
        """
        cursor.execute(sql_provider_fix)
        providers = cursor.fetchall()

        if providers:
            print(f"   ... Generating Names for {len(providers)} Providers (Parallel Mode)...")
            
            name_updates = []
            
            # THREADING: Run API calls in parallel
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Map futures to provider keys
                future_to_key = {executor.submit(fetch_ai_name, client, p['specialty']): p['provider_key'] for p in providers}
                
                for future in tqdm(as_completed(future_to_key), total=len(providers), desc="   🤖 AI Generation"):
                    prov_key = future_to_key[future]
                    try:
                        new_name = future.result()
                        name_updates.append((new_name, prov_key))
                    except Exception as exc:
                        pass

            # Batch Update Database
            print("   💾 Saving generated names...")
            sql_update = "UPDATE dim_provider SET name = %s WHERE provider_key = %s"
            cursor = conn.cursor()
            cursor.executemany(sql_update, name_updates)
            conn.commit()
            print(f"   > ✅ Updated {len(name_updates)} provider names.")
        else:
            print("   > No provider names needed fixing.")
    else:
        print("   > Skipping LLM generation (No API Key found).")

# ==========================================
# PHASE 3: LOGICAL VALIDATION (UNCHANGED)
# ==========================================
def perform_advanced_validation(conn):
    cursor = conn.cursor()
    print("\n" + "="*60)
    print("🔍 ADVANCED LOGICAL VALIDATION REPORT")
    print("="*60)

    validations = [
        {"name": "Birthdate in Future", "query": "SELECT COUNT(*) FROM dim_patient WHERE birthdate > CURDATE()"},
        {"name": "Event Before Birth", "query": "SELECT COUNT(*) FROM fact_patient_events f JOIN dim_patient p ON f.patient_key = p.patient_key WHERE f.date_key < p.birthdate"},
        {"name": "Invalid Zip Format", "query": "SELECT COUNT(*) FROM dim_patient WHERE zip NOT REGEXP '^[0-9]{5}$'"}
    ]

    for v in validations:
        cursor.execute(v['query'])
        count = cursor.fetchone()[0]
        status = "✅ PASS" if count == 0 else f"❌ FAIL ({count})"
        print(f"{status:<20} : {v['name']}")

# ==========================================
# MAIN
# ==========================================
def clean_and_validate():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        clean_names_regex(conn)
        run_smart_enrichment(conn)
        perform_advanced_validation(conn)
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected(): conn.close()

if __name__ == "__main__":
    clean_and_validate()