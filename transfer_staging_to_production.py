"""
Script simple pour transférer les données de staging vers production
"""

import os
import mysql.connector
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

DB_CONFIG = {
    'host': os.getenv("DB_HOST"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASS"),
    'database': os.getenv("DB_NAME"),
    'charset': 'utf8mb4'
}

print("="*70)
print("🔄 TRANSFERT DES DONNÉES DE STAGING VERS PRODUCTION")
print("="*70)

try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    print("✅ Connexion réussie!\n")
    
    # Vérifier les données dans staging
    print("📊 VÉRIFICATION DES DONNÉES DE STAGING:")
    print("-" * 70)
    
    staging_tables = [
        'staging_patients', 'staging_encounters', 'staging_conditions',
        'staging_medications', 'staging_observations', 'staging_allergies',
        'staging_procedures', 'staging_immunizations'
    ]
    
    for table in staging_tables:
        try:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            count = cursor.fetchone()['count']
            print(f"  {table:30} : {count:5} ligne(s)")
        except:
            print(f"  {table:30} : Table inexistante")
    
    print("\n" + "="*70)
    print("📊 VÉRIFICATION DES TABLES DE PRODUCTION:")
    print("-" * 70)
    
    prod_tables = [
        'patients', 'encounters', 'conditions', 'medications',
        'observations', 'allergies', 'procedures', 'immunizations'
    ]
    
    for table in prod_tables:
        try:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            count = cursor.fetchone()['count']
            print(f"  {table:30} : {count:5} ligne(s)")
        except:
            print(f"  {table:30} : Table inexistante")
    
    print("\n" + "="*70)
    print("💡 SOLUTION:")
    print("="*70)
    print("Pour transférer les données de staging vers production,")
    print("exécutez le script:")
    print("  python process_staging_to_production.py")
    print("\nCe script va:")
    print("  1. Valider les données de staging")
    print("  2. Transférer les données vers les tables de production")
    print("  3. Optimiser les tables")
    print("="*70)
    
    cursor.close()
    conn.close()
    
except mysql.connector.Error as err:
    print(f"❌ ERREUR: {err}")


