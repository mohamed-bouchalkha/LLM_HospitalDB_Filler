import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import time

# Optional: Try to import tqdm for progress bars, fallback if not installed
try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterator, **kwargs): return iterator

# --- CONFIGURATION ---
load_dotenv()

DB_CONFIG = {
    'host': os.getenv("DB_HOST"), 
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASS"), 
    'database': os.getenv("DB_NAME"),
    'allow_local_infile': True, # Optimization
    'compress': True # Optimization for large data transfers
}

DATA_FOLDER = r"ressources\texas_data"
BATCH_SIZE = 5000  # Number of rows to insert at once

class SmartDataETL:
    def __init__(self):
        print(f"🔌 Connecting to database: {DB_CONFIG['database']}...")
        try:
            self.conn = mysql.connector.connect(**DB_CONFIG)
            self.cursor = self.conn.cursor() 
            self.conn.autocommit = False
            
            # Initialize Caches
            self.cache_patients = {}
            self.cache_orgs = {}
            self.cache_providers = {}
            self.cache_payers = {} 
            self.cache_dates = {} 

        except mysql.connector.Error as err:
            print(f"❌ Connection Error: {err}")
            exit(1)

    # =================================================
    # HELPER: BATCH PROCESSING
    # =================================================

    def _batch_insert(self, sql, data, desc="Inserting"):
        """Generic helper to insert data in chunks."""
        if not data: return

        total = len(data)
        # Use tqdm for a progress bar if available
        for i in tqdm(range(0, total, BATCH_SIZE), desc=f"   💾 {desc}", unit="batch"):
            batch = data[i:i + BATCH_SIZE]
            try:
                self.cursor.executemany(sql, batch)
                self.conn.commit()
            except mysql.connector.Error as err:
                print(f"Error in batch {i}: {err}")
                self.conn.rollback()
                # Optional: Re-connect if connection lost
                if not self.conn.is_connected():
                     print("   ⚠️ Reconnecting...")
                     self.conn.reconnect(attempts=3, delay=2)
                     self.cursor = self.conn.cursor()

    def _ensure_dates_exist(self, date_series):
        """
        Takes a pandas Series of dates (strings), finds unique ones, 
        inserts missing ones into DB, and updates local cache.
        """
        # 1. Clean and find unique dates
        unique_dates = date_series.dropna().unique()
        
        # 2. Filter out dates we already have in memory
        new_dates = [d for d in unique_dates if d not in self.cache_dates]
        
        if not new_dates:
            return

        # 3. Prepare batch data
        batch_data = []
        for d_str in new_dates:
            try:
                dt = pd.to_datetime(d_str)
                if pd.isna(dt): continue
                
                date_val = dt.date()
                batch_data.append((
                    date_val, dt.year, dt.month, dt.strftime('%B'), dt.strftime('%A')
                ))
                # Update cache immediately
                self.cache_dates[d_str] = date_val
            except:
                continue

        # 4. INSERT IN BATCHES
        if batch_data:
            print(f"   📅 Pre-loading {len(batch_data)} new dates in batches...")
            sql = """INSERT IGNORE INTO dim_date 
                     (date_key, year, month, month_name, day_of_week) 
                     VALUES (%s, %s, %s, %s, %s)"""
            
            # We reuse the batch logic manually here to avoid circular dependencies or complex refactoring
            total = len(batch_data)
            for i in range(0, total, BATCH_SIZE):
                batch = batch_data[i:i + BATCH_SIZE]
                try:
                    self.cursor.executemany(sql, batch)
                    self.conn.commit()
                except mysql.connector.Error as e:
                    print(f"   ⚠️ Date Batch Error: {e}")
                    self.conn.rollback()

    # =================================================
    # SECTION 1: DIMENSION LOADERS
    # =================================================
    
    def load_payers(self):
        print("\n🚀 Processing Payers...")
        csv_path = os.path.join(DATA_FOLDER, "payers.csv")
        if not os.path.exists(csv_path): return
        
        df = pd.read_csv(csv_path).where(pd.notnull, None)
        sql = "INSERT INTO dim_payer (name) VALUES (%s)"
        
        for row in df.itertuples():
            self.cursor.execute(sql, (row.NAME,))
            self.cache_payers[row.Id] = self.cursor.lastrowid
        self.conn.commit()

    def load_organizations(self):
        print("\n🚀 Processing Organizations...")
        csv_path = os.path.join(DATA_FOLDER, "organizations.csv")
        if not os.path.exists(csv_path): return
        
        df = pd.read_csv(csv_path).where(pd.notnull, None)
        sql = "INSERT INTO dim_organization (name, city, state) VALUES (%s, %s, %s)"
        
        for row in df.itertuples():
            self.cursor.execute(sql, (row.NAME, row.CITY, row.STATE))
            self.cache_orgs[row.Id] = self.cursor.lastrowid
        self.conn.commit()

    def load_providers(self):
        print("\n🚀 Processing Providers...")
        csv_path = os.path.join(DATA_FOLDER, "providers.csv")
        if not os.path.exists(csv_path): return
        
        df = pd.read_csv(csv_path).where(pd.notnull, None)
        sql = "INSERT INTO dim_provider (name, specialty) VALUES (%s, %s)"
        
        for row in df.itertuples():
            self.cursor.execute(sql, (row.NAME, row.SPECIALITY))
            self.cache_providers[row.Id] = self.cursor.lastrowid
        self.conn.commit()

    def load_patients(self):
        print("\n🚀 Processing Patients...")
        csv_path = os.path.join(DATA_FOLDER, "patients.csv")
        if not os.path.exists(csv_path): return
        
        df = pd.read_csv(csv_path).where(pd.notnull, None)
        sql = """INSERT INTO dim_patient (full_name, gender, birthdate, city, state, zip)
                 VALUES (%s, %s, %s, %s, %s, %s)"""

        for row in df.itertuples():
            full_name = f"{row.FIRST} {row.LAST}"
            zip_code = str(row.ZIP) if row.ZIP else None
            self.cursor.execute(sql, (full_name, row.GENDER, row.BIRTHDATE, 
                                    row.CITY, row.STATE, zip_code))
            self.cache_patients[row.Id] = self.cursor.lastrowid
        self.conn.commit()

    # =================================================
    # SECTION 2: OPTIMIZED FACT LOADERS
    # =================================================

    def load_encounters(self):
        print("\n🚀 Processing Encounters...")
        path = os.path.join(DATA_FOLDER, "encounters.csv")
        if not os.path.exists(path): return
        
        df = pd.read_csv(path).where(pd.notnull, None)
        self._ensure_dates_exist(df['START'])

        data_to_insert = []
        print("   ⚙️ Mapping data...")
        
        for row in df.itertuples():
            pat_key = self.cache_patients.get(row.PATIENT)
            if not pat_key: continue

            data_to_insert.append((
                pat_key,
                self.cache_dates.get(row.START),
                self.cache_providers.get(row.PROVIDER),
                self.cache_orgs.get(row.ORGANIZATION),
                self.cache_payers.get(row.PAYER),
                row.Id,
                'Encounter',
                row.ENCOUNTERCLASS,
                row.DESCRIPTION,
                None, None, # val, units
                row.TOTAL_CLAIM_COST
            ))

        sql = """INSERT INTO fact_patient_events 
                 (patient_key, date_key, provider_key, org_key, payer_key, encounter_id, 
                  event_category, code, description, numeric_value, units, cost)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        self._batch_insert(sql, data_to_insert, "Encounters")

    def load_careplans(self):
        print("\n🚀 Processing CarePlans...")
        path = os.path.join(DATA_FOLDER, "careplans.csv")
        if not os.path.exists(path): return
        
        df = pd.read_csv(path).where(pd.notnull, None)
        self._ensure_dates_exist(df['START'])

        data_to_insert = []
        for row in df.itertuples():
            pat_key = self.cache_patients.get(row.PATIENT)
            if not pat_key: continue

            data_to_insert.append((
                pat_key,
                self.cache_dates.get(row.START),
                None, None, None,
                row.ENCOUNTER,
                'CarePlan',
                row.CODE,
                row.DESCRIPTION,
                None, None, None
            ))

        sql = """INSERT INTO fact_patient_events 
                 (patient_key, date_key, provider_key, org_key, payer_key, encounter_id, 
                  event_category, code, description, numeric_value, units, cost)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        self._batch_insert(sql, data_to_insert, "CarePlans")

    def load_conditions(self):
        print("\n🚀 Processing Conditions...")
        path = os.path.join(DATA_FOLDER, "conditions.csv")
        if not os.path.exists(path): return
        
        df = pd.read_csv(path).where(pd.notnull, None)
        self._ensure_dates_exist(df['START'])

        data_to_insert = []
        for row in df.itertuples():
            pat_key = self.cache_patients.get(row.PATIENT)
            if not pat_key: continue

            data_to_insert.append((
                pat_key,
                self.cache_dates.get(row.START),
                None, None, None,
                row.ENCOUNTER,
                'Diagnosis',
                row.CODE,
                row.DESCRIPTION,
                None, None, None
            ))

        sql = """INSERT INTO fact_patient_events 
                 (patient_key, date_key, provider_key, org_key, payer_key, encounter_id, 
                  event_category, code, description, numeric_value, units, cost)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        self._batch_insert(sql, data_to_insert, "Conditions")

    def load_medications(self):
        print("\n🚀 Processing Medications...")
        path = os.path.join(DATA_FOLDER, "medications.csv")
        if not os.path.exists(path): return
        
        df = pd.read_csv(path).where(pd.notnull, None)
        self._ensure_dates_exist(df['START'])

        data_to_insert = []
        for row in df.itertuples():
            pat_key = self.cache_patients.get(row.PATIENT)
            if not pat_key: continue

            data_to_insert.append((
                pat_key,
                self.cache_dates.get(row.START),
                None, None, 
                self.cache_payers.get(getattr(row, 'PAYER', None)), 
                row.ENCOUNTER,
                'Medication',
                row.CODE,
                row.DESCRIPTION,
                None, None,
                getattr(row, 'TOTALCOST', None)
            ))

        sql = """INSERT INTO fact_patient_events 
                 (patient_key, date_key, provider_key, org_key, payer_key, encounter_id, 
                  event_category, code, description, numeric_value, units, cost)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        self._batch_insert(sql, data_to_insert, "Medications")

    def load_allergies(self):
        print("\n🚀 Processing Allergies...")
        path = os.path.join(DATA_FOLDER, "allergies.csv")
        if not os.path.exists(path): return
        
        df = pd.read_csv(path).where(pd.notnull, None)
        self._ensure_dates_exist(df['START'])

        data_to_insert = []
        for row in df.itertuples():
            pat_key = self.cache_patients.get(row.PATIENT)
            if not pat_key: continue

            data_to_insert.append((
                pat_key,
                self.cache_dates.get(row.START),
                None, None, None,
                row.ENCOUNTER,
                'Allergy',
                row.CODE,
                row.DESCRIPTION,
                None, None, None
            ))

        sql = """INSERT INTO fact_patient_events 
                 (patient_key, date_key, provider_key, org_key, payer_key, encounter_id, 
                  event_category, code, description, numeric_value, units, cost)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        self._batch_insert(sql, data_to_insert, "Allergies")

    def load_observations(self):
        print("\n🚀 Processing Observations...")
        path = os.path.join(DATA_FOLDER, "observations.csv")
        if not os.path.exists(path): return
        
        # Chunked Read for massive files
        df = pd.read_csv(path).where(pd.notnull, None)
        self._ensure_dates_exist(df['DATE'])

        print("   ⚙️ Mapping data...")
        data_to_insert = []
        
        # Note: If mapping 5000 patients (2M rows) takes too long here, 
        # we can move mapping inside the batch loop, but this is usually fine for <5GB RAM
        for row in df.itertuples():
            pat_key = self.cache_patients.get(row.PATIENT)
            if not pat_key: continue

            try: val = float(row.VALUE) 
            except: val = None 

            data_to_insert.append((
                pat_key,
                self.cache_dates.get(row.DATE),
                None, None, None,
                row.ENCOUNTER,
                'Observation',
                row.CODE,
                row.DESCRIPTION,
                val,
                row.UNITS,
                None
            ))

        sql = """INSERT INTO fact_patient_events 
                 (patient_key, date_key, provider_key, org_key, payer_key, encounter_id, 
                  event_category, code, description, numeric_value, units, cost)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        self._batch_insert(sql, data_to_insert, "Observations")

    def run(self):
        start_time = time.time()
        try:
            self.load_payers()
            self.load_organizations()
            self.load_providers()
            self.load_patients()
            
            self.load_encounters()
            self.load_careplans()
            self.load_conditions()
            self.load_medications()
            self.load_allergies()
            self.load_observations()
            
            duration = time.time() - start_time
            print(f"\n✨ ETL Pipeline Complete in {duration:.2f} seconds.")
        except Exception as e:
            print(f"❌ Pipeline Failed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if self.conn.is_connected():
                self.conn.close()

if __name__ == "__main__":
    etl = SmartDataETL()
    etl.run()