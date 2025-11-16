"""
Script 2: process_staging_to_production.py
Appelle la procédure de validation SQL, puis charge les données 'pending'
propres dans les tables de production.
"""

import os
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv

# --- Configuration Globale ---
load_dotenv()
DB_CONFIG = {
    'host': os.getenv("DB_HOST"), 'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASS"), 'database': os.getenv("DB_NAME"),
    'charset': 'utf8mb4'
}

class ProductionLoader:
    
    def __init__(self, db_config: dict):
        try:
            self.db_conn = mysql.connector.connect(**db_config)
            self.db_cursor = self.db_conn.cursor(buffered=True, dictionary=True)
            print("✓ Connexion à la base de données (Production) établie")
        except mysql.connector.Error as err:
            print(f"❌ ERREUR de connexion DB: {err}")
            exit()
    
    def safe_cast(self, val, to_type, default=None):
        if val is None or val == 'null': return default
        try:
            if to_type == datetime.fromisoformat:
                return datetime.fromisoformat(val.replace("Z", "+00:00"))
            return to_type(val)
        except (ValueError, TypeError):
            return default

    def call_validation_procedure(self):
        print("\n🔬 Lancement de la procédure de validation SQL (sp_validate_staging_data)...")
        try:
            self.db_cursor.execute("CALL sp_validate_staging_data()")
            print("  Résultats de la validation :")

            for result in self.db_cursor.stored_results():
                rows = result.fetchall()  # consume full result set
                for row in rows:
                    print(f"    {row[list(row.keys())[0]]}")

            # Ensure no extra result sets remain
            while self.db_cursor.nextset():
                pass

            self.db_conn.commit()
            print("  ✓ Procédure de validation SQL terminée.")
        except Exception as e:
            print(f"  ❌ ERREUR: {e}\n  Assurez-vous d'avoir exécuté 'SQL/run_validation.sql' pour créer la procédure.")
            print("  Astuce: Dans MySQL, exécutez: SOURCE d:/My_Projects/LLM_HospitalDB_Filler/SQL/run_validation.sql;")
            self.db_conn.rollback()
            raise e


    def update_staging_status(self, table_name: str, p_key_col: str, p_key_val: int | str, status: str, message: str = ""):
        """Met à jour le statut de la ligne de staging (utilisé pour les erreurs Python)"""
        try:
            sql = f"""
                UPDATE {table_name} SET extraction_status = %s, error_message = %s, processed_at = CURRENT_TIMESTAMP
                WHERE {p_key_col} = %s
            """
            self.db_cursor.execute(sql, [status, message, p_key_val])
        except Exception as e:
            print(f"  ❌ Erreur MàJ statut staging: {e}")
            
    def process_patients(self):
        """Valide et insère les patients de staging vers production"""
        print("\nProcessing Patients...")
        self.db_cursor.execute("SELECT * FROM staging_patients WHERE extraction_status = 'pending'")
        patients = self.db_cursor.fetchall()
        
        count_success = 0
        for p in patients:
            try:
                patient_prod = {
                    'id': p['id'],
                    'birthdate': self.safe_cast(p['birthdate'], datetime.fromisoformat).date(),
                    'ssn': p['ssn'], 'first_name': p['first_name'], 'last_name': p['last_name'],
                    'race': p['race'], 'ethnicity': p['ethnicity'], 'gender': p['gender'],
                    'address': p['address'], 'city': p['city'], 'state': p['state'], 'zip': p['zip'],
                }
                
                cols = ', '.join(patient_prod.keys())
                placeholders = ', '.join(['%s'] * len(patient_prod))
                sql = f"INSERT INTO patients ({cols}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE address = VALUES(address), city = VALUES(city), updated_at = CURRENT_TIMESTAMP"
                
                self.db_cursor.execute(sql, list(patient_prod.values()))
                self.update_staging_status('staging_patients', 'id', p['id'], 'validated')
                count_success += 1
            except Exception as e:
                self.update_staging_status('staging_patients', 'id', p['id'], 'error', f"Erreur Python: {e}")
        
        self.db_conn.commit()
        print(f"  Processed: {len(patients)}, Inserted/Updated: {count_success}")

    def process_encounters(self):
        """Valide et insère les encounters"""
        print("\nProcessing Encounters...")
        self.db_cursor.execute("SELECT * FROM staging_encounters WHERE extraction_status = 'pending'")
        encounters = self.db_cursor.fetchall()
        
        count_success = 0
        for e in encounters:
            try:
                encounter_prod = {
                    'id': e['id'],
                    'start_datetime': self.safe_cast(e['start_datetime'], datetime.fromisoformat),
                    'stop_datetime': self.safe_cast(e['stop_datetime'], datetime.fromisoformat),
                    'patient_id': e['patient_id'],
                    'organization_id': e['organization_id'], 'provider_id': e['provider_id'],
                    'encounter_class': e['encounter_class'],
                    'code': self.safe_cast(e['code'], int), 'description': e['description'],
                    'base_encounter_cost': self.safe_cast(e['base_encounter_cost'], float),
                    'total_claim_cost': self.safe_cast(e['total_claim_cost'], float),
                    'payer_coverage': self.safe_cast(e['payer_coverage'], float),
                    'reason_description': e['reason_description']
                }
                
                cols = ', '.join(encounter_prod.keys())
                placeholders = ', '.join(['%s'] * len(encounter_prod))
                sql = f"INSERT INTO encounters ({cols}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE stop_datetime = VALUES(stop_datetime)"
                
                self.db_cursor.execute(sql, list(encounter_prod.values()))
                self.update_staging_status('staging_encounters', 'id', e['id'], 'validated')
                count_success += 1
            except Exception as ex:
                self.update_staging_status('staging_encounters', 'id', e['id'], 'error', f"Erreur Python: {ex}")

        self.db_conn.commit()
        print(f"  Processed: {len(encounters)}, Inserted/Updated: {count_success}")

    def process_generic_table(self, staging_table, prod_table, field_map):
        """Méthode générique pour les tables 'conditions', 'medications', etc."""
        print(f"\nProcessing {prod_table}...")
        self.db_cursor.execute(f"SELECT * FROM {staging_table} WHERE extraction_status = 'pending'")
        items = self.db_cursor.fetchall()
        
        count_success = 0
        for item in items:
            try:
                prod_data = {}
                for prod_field, (staging_field, field_type) in field_map.items():
                    prod_data[prod_field] = self.safe_cast(item[staging_field], field_type)
                
                cols = ', '.join(prod_data.keys())
                placeholders = ', '.join(['%s'] * len(prod_data))
                sql_insert = f"INSERT INTO {prod_table} ({cols}) VALUES ({placeholders})"
                
                self.db_cursor.execute(sql_insert, list(prod_data.values()))
                self.update_staging_status(staging_table, 'staging_id', item['staging_id'], 'validated')
                count_success += 1
            except Exception as ex:
                self.update_staging_status(staging_table, 'staging_id', item['staging_id'], 'error', f"Erreur Python: {ex}")
        
        self.db_conn.commit()
        print(f"  Processed: {len(items)}, Inserted: {count_success}")

    def optimize_tables(self):
        """SECTION 6: OPTIMISATION"""
        print("\n⚙️ Optimisation des tables de production...")
        tables_prod = ['patients', 'encounters', 'conditions', 'medications', 'observations', 'allergies', 'procedures', 'immunizations']
        for table in tables_prod:
            try:
                print(f"  > Analyse de la table {table}...")
                self.db_cursor.execute(f"ANALYZE TABLE {table};")
            except Exception as e:
                print(f"  > Erreur analyse {table}: {e}")
        print("  ✓ Optimisation terminée.")

    def run_all_processing(self):
        """Exécute toutes les étapes dans le bon ordre"""
        try:
            self.call_validation_procedure()
            self.process_patients()
            self.process_encounters()
            
            self.process_generic_table('staging_conditions', 'conditions', {
                'start_date': ('start_date', datetime.fromisoformat), 'stop_date': ('stop_date', datetime.fromisoformat),
                'patient_id': ('patient_id', str), 'encounter_id': ('encounter_id', str),
                'code': ('code', int), 'description': ('description', str)
            })
            
            # Étape 3: Traiter les tables enfants
            
            # Mappage pour Conditions
            self.process_generic_table('staging_conditions', 'conditions', {
                'start_date': ('start_date', datetime.fromisoformat), 
                'stop_date': ('stop_date', datetime.fromisoformat),
                'patient_id': ('patient_id', str), 
                'encounter_id': ('encounter_id', str),
                'code': ('code', int), 
                'description': ('description', str)
            })
            
            # Mappage pour Medications
            self.process_generic_table('staging_medications', 'medications', {
                'start_datetime': ('start_datetime', datetime.fromisoformat), 
                'stop_datetime': ('stop_datetime', datetime.fromisoformat),
                'patient_id': ('patient_id', str), 
                'encounter_id': ('encounter_id', str),
                'code': ('code', int), 
                'description': ('description', str),
                'base_cost': ('base_cost', float),
                'payer_coverage': ('payer_coverage', float),
                'total_cost': ('total_cost', float),
                'reason_description': ('reason_description', str)
            })
            
            # Mappage pour Observations
            # Note: 'code' est bien 'str' (VARCHAR) dans la table de production
            self.process_generic_table('staging_observations', 'observations', {
                'date_recorded': ('date_recorded', datetime.fromisoformat),
                'patient_id': ('patient_id', str), 
                'encounter_id': ('encounter_id', str),
                'code': ('code', str), 
                'description': ('description', str),
                'value': ('value', str),
                'units': ('units', str),
                'type': ('type', str)
            })
            
            # Mappage pour Allergies
            self.process_generic_table('staging_allergies', 'allergies', {
                'start_date': ('start_date', datetime.fromisoformat), 
                'stop_date': ('stop_date', datetime.fromisoformat),
                'patient_id': ('patient_id', str), 
                'encounter_id': ('encounter_id', str),
                'code': ('code', int), 
                'description': ('description', str)
            })
            
            # Mappage pour Procedures
            self.process_generic_table('staging_procedures', 'procedures', {
                'date_performed': ('date_performed', datetime.fromisoformat),
                'patient_id': ('patient_id', str), 
                'encounter_id': ('encounter_id', str),
                'code': ('code', int), 
                'description': ('description', str),
                'base_cost': ('base_cost', float),
                'reason_description': ('reason_description', str)
            })
            
            # Mappage pour Immunizations
            self.process_generic_table('staging_immunizations', 'immunizations', {
                'date_administered': ('date_administered', datetime.fromisoformat),
                'patient_id': ('patient_id', str), 
                'encounter_id': ('encounter_id', str),
                'code': ('code', int), 
                'description': ('description', str),
                'base_cost': ('base_cost', float)
            })

            
            self.optimize_tables() # Exécuter l'optimisation à la fin
            
        except Exception as e:
            print(f"\n❌ ERREUR GLOBALE: {e}")
            self.db_conn.rollback()
        finally:
            self.db_cursor.close()
            self.db_conn.close()
            print("\n✓ Connexion DB (Production) fermée.")

if __name__ == "__main__":
    print("... (SCRIPT 2: VALIDATION & LOAD) ...")
    loader = ProductionLoader(db_config=DB_CONFIG)
    loader.run_all_processing()
    print("\n✅ Validation et chargement en production terminés.")
    print("Lancez 'export_to_csv.py' pour générer les fichiers finaux.")