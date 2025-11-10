"""
Script 3: export_to_csv.py
Se connecte à la base de données de production et exporte
chaque table principale dans un fichier CSV séparé.
"""

import os
import mysql.connector
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# --- Configuration Globale ---
load_dotenv()
DB_CONFIG = {
    'host': os.getenv("DB_HOST"), 'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASS"), 'database': os.getenv("DB_NAME"),
    'charset': 'utf8mb4'
}
CSV_OUTPUT_FOLDER = Path("csv_export")
TABLES_TO_EXPORT = [
    'patients', 'encounters', 'conditions', 'medications',
    'observations', 'allergies', 'procedures', 'immunizations'
]

def export_tables_to_csv():
    """Exporte les tables de production en CSV."""
    print(f"Début de l'exportation vers : {CSV_OUTPUT_FOLDER.resolve()}")
    CSV_OUTPUT_FOLDER.mkdir(exist_ok=True)
    
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        
        for table_name in TABLES_TO_EXPORT:
            print(f"  📄 Exportation de la table '{table_name}'...")
            sql_query = f"SELECT * FROM {table_name}"
            
            try:
                df = pd.read_sql_query(sql_query, conn)
                if df.empty:
                    print(f"  ℹ️  Table '{table_name}' est vide. Fichier non créé.")
                    continue
                
                output_path = CSV_OUTPUT_FOLDER / f"{table_name}.csv"
                df.to_csv(output_path, index=False, encoding='utf-8-sig')
                print(f"  ✓ Succès ! ({len(df)} lignes) -> {output_path.name}")
                
            except Exception as e:
                print(f"  ❌ ERREUR lors de l'export de {table_name}: {e}")

        conn.close()
        print("\n✓ Connexion DB fermée.")
        
    except mysql.connector.Error as err:
        print(f"❌ ERREUR de connexion à la base de données: {err}")
        return

    print(f"\n{'='*50}\n✅ Exportation CSV terminée.\n{'='*50}")

if __name__ == "__main__":
    print("... (SCRIPT 3: EXPORT CSV) ...")
    export_tables_to_csv()