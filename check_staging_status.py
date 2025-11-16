"""
Script de diagnostic : Vérifie l'état des tables de staging pour comprendre pourquoi les autres tables sont vides
"""

import os
import mysql.connector
from dotenv import load_dotenv

# --- Configuration Globale ---
load_dotenv()
DB_CONFIG = {
    'host': os.getenv("DB_HOST"), 
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASS"), 
    'database': os.getenv("DB_NAME"),
    'charset': 'utf8mb4'
}

def check_staging_status():
    """Vérifie l'état des tables de staging"""
    print("\n" + "="*70)
    print("🔍 DIAGNOSTIC : ÉTAT DES TABLES DE STAGING")
    print("="*70)
    
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        staging_tables = [
            ('staging_patients', 'id'),
            ('staging_encounters', 'id'),
            ('staging_conditions', 'staging_id'),
            ('staging_medications', 'staging_id'),
            ('staging_observations', 'staging_id'),
            ('staging_allergies', 'staging_id'),
            ('staging_procedures', 'staging_id'),
            ('staging_immunizations', 'staging_id')
        ]
        
        for table_name, key_col in staging_tables:
            print(f"\n📋 {table_name}:")
            
            # Compter par statut
            cursor.execute(f"""
                SELECT 
                    extraction_status,
                    COUNT(*) as count
                FROM {table_name}
                GROUP BY extraction_status
            """)
            status_counts = cursor.fetchall()
            
            total = 0
            for status_row in status_counts:
                status = status_row['extraction_status'] or 'NULL'
                count = status_row['count']
                total += count
                
                icon = "✅" if status == 'validated' else "⏳" if status == 'pending' else "❌"
                print(f"   {icon} {status}: {count} ligne(s)")
            
            print(f"   📊 TOTAL: {total} ligne(s)")
            
            # Si des erreurs, afficher quelques exemples
            if total > 0:
                cursor.execute(f"""
                    SELECT {key_col}, error_message, report_filename
                    FROM {table_name}
                    WHERE extraction_status = 'error'
                    LIMIT 3
                """)
                errors = cursor.fetchall()
                if errors:
                    print(f"   ⚠️  Exemples d'erreurs:")
                    for err in errors:
                        key_val = err.get(key_col, 'N/A')
                        error_msg = err.get('error_message', 'N/A')[:100]
                        filename = err.get('report_filename', 'N/A')
                        print(f"      • {key_col}={key_val} ({filename}): {error_msg}")
        
        cursor.close()
        conn.close()
        
    except mysql.connector.Error as err:
        print(f"❌ ERREUR de connexion: {err}")

def check_production_status():
    """Vérifie l'état des tables de production"""
    print("\n" + "="*70)
    print("📊 ÉTAT DES TABLES DE PRODUCTION")
    print("="*70)
    
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        prod_tables = [
            'patients', 'encounters', 'conditions', 'medications',
            'observations', 'allergies', 'procedures', 'immunizations'
        ]
        
        for table in prod_tables:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            count = cursor.fetchone()['count']
            icon = "✅" if count > 0 else "⚠️"
            print(f"  {icon} {table}: {count} ligne(s)")
        
        cursor.close()
        conn.close()
        
    except mysql.connector.Error as err:
        print(f"❌ ERREUR de connexion: {err}")

def provide_solutions():
    """Fournit des solutions selon les problèmes détectés"""
    print("\n" + "="*70)
    print("💡 SOLUTIONS")
    print("="*70)
    
    print("\n🔧 Si les tables de staging sont vides :")
    print("   → Exécutez d'abord: python extract_to_staging_db.py")
    print("   → Cela remplira les tables de staging")
    
    print("\n🔧 Si les tables de staging ont des données mais status='error' :")
    print("   → Vérifiez les messages d'erreur ci-dessus")
    print("   → Corrigez les données dans staging si nécessaire")
    print("   → Ré-exécutez: python extract_to_staging_db.py")
    
    print("\n🔧 Si les tables de staging ont status='pending' mais production vide :")
    print("   → Exécutez: python process_staging_to_production.py")
    print("   → Vérifiez les messages d'erreur pendant l'exécution")
    
    print("\n🔧 Si encounters=0 mais patients>0 :")
    print("   → Vérifiez que staging_encounters contient des données")
    print("   → Vérifiez que start_datetime n'est pas NULL")
    print("   → Vérifiez que patient_id correspond à un patient existant")

if __name__ == "__main__":
    print("\n" + "="*70)
    print("🔍 DIAGNOSTIC : POURQUOI LES AUTRES TABLES SONT VIDES")
    print("="*70)
    
    check_staging_status()
    check_production_status()
    provide_solutions()
    
    print("\n" + "="*70)

