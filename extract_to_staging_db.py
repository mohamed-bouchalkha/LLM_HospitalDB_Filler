"""
Script 1: extract_to_staging_db.py
Système d'Extraction Intelligent : Rapports Non Structurés -> Tables de Staging SQL
Version Gemini API
"""

import os
import re
import json
import mysql.connector
from pathlib import Path
from typing import Dict, List
import google.generativeai as genai
from dotenv import load_dotenv

# --- Configuration Globale ---
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DB_CONFIG = {
    'host': os.getenv("DB_HOST"), 
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASS"), 
    'database': os.getenv("DB_NAME"),
    'charset': 'utf8mb4'
}
# Utiliser les rapports générés par structure_to_pdf_text.py
# Ils se trouvent dans: moroccan_unstructured_data/txt
REPORTS_FOLDER = Path("moroccan_unstructured_data") / "txt"


def extract_patient_metadata_from_text(report_text: str) -> Dict:
    marker = "METADONNEES_PATIENT_JSON:"
    idx = report_text.find(marker)
    if idx == -1:
        return {}
    start = idx + len(marker)
    length = len(report_text)
    while start < length and report_text[start] in " \n\r\t":
        start += 1
    if start >= length or report_text[start] != '{':
        return {}
    depth = 0
    end = start
    for i in range(start, length):
        ch = report_text[i]
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if depth != 0:
        return {}
    json_block = report_text[start:end]
    try:
        metadata = json.loads(json_block)
        if isinstance(metadata, dict):
            return metadata
    except json.JSONDecodeError:
        return {}
    return {}


class StagingExtractor:
    """Extrait les données et les charge dans les tables de staging"""
    
    def __init__(self, api_key: str, reports_folder: Path, db_config: dict):
        self.reports_folder = reports_folder
        
        # Configuration de Gemini
        genai.configure(api_key=api_key)
        # Utilise gemini-2.5-flash : rapide, performant et économique
        self.model = genai.GenerativeModel('gemini-2.5-flash')
        
        try:
            self.db_conn = mysql.connector.connect(**db_config)
            self.db_cursor = self.db_conn.cursor(dictionary=True)
            print("✓ Connexion à la base de données (Staging) établie")
        except mysql.connector.Error as err:
            print(f"✗ ERREUR de connexion DB: {err}")
            exit()
            
        print(f"✓ Dossier des rapports : {self.reports_folder}")
    
    def extract_with_llm(self, report_text: str, extraction_type: str) -> Dict:
        """Utilise Gemini AI pour extraire les données structurées du rapport"""
        
        # Prompts optimisés pour Gemini
#         prompts = {
#             'patient_info': """Extrait les informations du patient du rapport médical. 
# Retourne UNIQUEMENT un objet JSON valide avec ces champs:
# {"id", "first_name", "last_name", "birthdate": "YYYY-MM-DD", "gender": "M ou F", "ssn", "address", "city", "state", "zip", "race", "ethnicity"}
# Ne retourne que le JSON, sans texte supplémentaire.""",
            
#             'encounter': """Extrait les informations de la consultation du rapport médical.
# Retourne UNIQUEMENT un objet JSON valide avec ces champs:
# {"id", "start_datetime": "YYYY-MM-DD HH:MM:SS", "stop_datetime", "patient_id", "organization_id", "provider_id", "encounter_class", "code", "description", "base_encounter_cost", "total_claim_cost", "payer_coverage", "reason_description"}
# Ne retourne que le JSON, sans texte supplémentaire.""",
            
#             'conditions': """Extrait TOUTES les pathologies/diagnostics du rapport médical.
# Retourne UNIQUEMENT un array JSON valide:
# [{"patient_id", "encounter_id", "start_date": "YYYY-MM-DD", "stop_date", "code", "description"}, ...]
# Ne retourne que le JSON, sans texte supplémentaire.""",
            
#             'medications': """Extrait TOUTES les prescriptions médicamenteuses du rapport médical.
# Retourne UNIQUEMENT un array JSON valide:
# [{"patient_id", "encounter_id", "start_datetime", "stop_datetime", "code", "description", "base_cost", "total_cost", "payer_coverage", "reason_description"}, ...]
# Ne retourne que le JSON, sans texte supplémentaire.""",
            
#             'observations': """Extrait TOUTES les observations/mesures vitales du rapport médical.
# Retourne UNIQUEMENT un array JSON valide:
# [{"patient_id", "encounter_id", "date_recorded", "code", "description", "value", "units", "type"}, ...]
# Ne retourne que le JSON, sans texte supplémentaire.""",
            
#             'allergies': """Extrait TOUTES les allergies du rapport médical.
# Retourne UNIQUEMENT un array JSON valide:
# [{"patient_id", "encounter_id", "start_date", "stop_date", "code", "description"}, ...]
# Ne retourne que le JSON, sans texte supplémentaire.""",
            
#             'procedures': """Extrait TOUS les actes médicaux/interventions du rapport médical.
# Retourne UNIQUEMENT un array JSON valide:
# [{"patient_id", "encounter_id", "date_performed", "code", "description", "base_cost", "reason_description"}, ...]
# Ne retourne que le JSON, sans texte supplémentaire.""",
            
#             'immunizations': """Extrait TOUTES les vaccinations du rapport médical.
# Retourne UNIQUEMENT un array JSON valide:
# [{"patient_id", "encounter_id", "date_administered", "code", "description", "base_cost"}, ...]
# Ne retourne que le JSON, sans texte supplémentaire."""
#         }

        prompts = {
    'patient_info': """Extrait les informations du patient du rapport médical. 
Retourne UNIQUEMENT un objet JSON valide avec ces champs:
{"id", "first_name", "last_name", "birthdate": "YYYY-MM-DD", "gender": "M ou F", "ssn", "address", "city", "state", "zip", "race", "ethnicity"}
Ne retourne que le JSON, sans texte supplémentaire.""",
    
    'encounter': """Extrait les informations de la consultation du rapport médical.
Retourne UNIQUEMENT un objet JSON valide avec ces champs:
{"id", "start_datetime": "YYYY-MM-DD HH:MM:SS", "stop_datetime": "YYYY-MM-DD HH:MM:SS", "patient_id", "organization_id", "provider_id", "encounter_class", "code", "description", "base_encounter_cost", "total_claim_cost", "payer_coverage", "reason_description"}
Ne retourne que le JSON, sans texte supplémentaire.""",
    
    'conditions': """Extrait TOUTES les pathologies/diagnostics du rapport médical.
Retourne UNIQUEMENT un array JSON valide:
[{"start_date": "YYYY-MM-DD", "stop_date": "YYYY-MM-DD", "code", "description"}, ...]
Ne retourne que le JSON, sans texte supplémentaire. NE PAS INCLURE patient_id ou encounter_id.""",
    
    'medications': """Extrait TOUTES les prescriptions médicamenteuses du rapport médical.
Retourne UNIQUEMENT un array JSON valide:
[{"start_datetime": "YYYY-MM-DD HH:MM:SS", "stop_datetime": "YYYY-MM-DD HH:MM:SS", "code", "description", "base_cost", "total_cost", "payer_coverage", "reason_description"}, ...]
Ne retourne que le JSON, sans texte supplémentaire. NE PAS INCLURE patient_id ou encounter_id.""",
    
    'observations': """Extrait TOUTES les observations/mesures vitales du rapport médical.
Retourne UNIQUEMENT un array JSON valide:
[{"date_recorded": "YYYY-MM-DD HH:MM:SS", "code", "description", "value", "units", "type"}, ...]
Ne retourne que le JSON, sans texte supplémentaire. NE PAS INCLURE patient_id ou encounter_id.""",
    
    'allergies': """Extrait TOUTES les allergies du rapport médical.
Retourne UNIQUEMENT un array JSON valide:
[{"start_date": "YYYY-MM-DD", "stop_date": "YYYY-MM-DD", "code", "description"}, ...]
Ne retourne que le JSON, sans texte supplémentaire. NE PAS INCLURE patient_id ou encounter_id.""",
    
    'procedures': """Extrait TOUS les actes médicaux/interventions du rapport médical.
Retourne UNIQUEMENT un array JSON valide:
[{"date_performed": "YYYY-MM-DD HH:MM:SS", "code", "description", "base_cost", "reason_description"}, ...]
Ne retourne que le JSON, sans texte supplémentaire. NE PAS INCLURE patient_id ou encounter_id.""",
    
    'immunizations': """Extrait TOUTES les vaccinations du rapport médical.
Retourne UNIQUEMENT un array JSON valide:
[{"date_administered": "YYYY-MM-DD HH:MM:SS", "code", "description", "base_cost"}, ...]
Ne retourne que le JSON, sans texte supplémentaire. NE PAS INCLURE patient_id ou encounter_id."""
}
        
        prompt = prompts.get(extraction_type)
        if not prompt: 
            return {}

        try:
            # Appel à Gemini
            full_prompt = f"{prompt}\n\nRAPPORT MÉDICAL:\n{report_text[:12000]}"
            response = self.model.generate_content(full_prompt)
            
            # Extraction du JSON
            content = response.text
            
            # Nettoyage du markdown si présent
            content = re.sub(r'```json\s*', '', content)
            content = re.sub(r'```\s*', '', content)
            content = content.strip()
            
            # Extraction du JSON
            json_match = re.search(r'\{.*\}|\[.*\]', content, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            
            print(f"  ⚠️  Aucun JSON valide retourné par Gemini pour {extraction_type}")
            return {}
                
        except Exception as e:
            print(f"✗ Erreur extraction Gemini ({extraction_type}): {e}")
            return {} if extraction_type in ['patient_info', 'encounter'] else []
    
    def insert_staging_data(self, table_name: str, data: Dict | List[Dict], report_filename: str) -> int:
        """Insère les données extraites dans la table de staging correspondante"""
        if not data: 
            return 0
        data_list = data if isinstance(data, list) else [data]
        
        count = 0
        for item in data_list:
            if not isinstance(item, dict): 
                continue
            item['report_filename'] = report_filename
            
            # Filtrer les clés pour ne garder que celles attendues
            self.db_cursor.execute(f"DESCRIBE {table_name}")
            valid_cols = {row['Field'] for row in self.db_cursor.fetchall()}
            
            item_filtered = {k: v for k, v in item.items() if k in valid_cols}
            
            cols = ', '.join(item_filtered.keys())
            placeholders = ', '.join(['%s'] * len(item_filtered))
            sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
            
            try:
                self.db_cursor.execute(sql, list(item_filtered.values()))
                count += 1
            except mysql.connector.Error as err:
                if err.errno == 1062:  # Clé dupliquée
                    print(f"  ℹ️  Doublon ignoré pour {table_name}: {item.get('id')}")
                else:
                    print(f"  ✗ Erreur insertion staging {table_name}: {err}")
        
        self.db_conn.commit()
        return count

    # def process_report(self, report_path: Path) -> Dict[str, int]:
    #     """Traite un rapport complet et insère toutes les données de staging"""
    #     print(f"\n📄 Traitement : {report_path.name}")
    #     try:
    #         with open(report_path, 'r', encoding='utf-8') as f:
    #             report_text = f.read()
    #     except Exception as e:
    #         print(f"  ✗ Impossible de lire le fichier: {e}")
    #         return {}
        
    #     stats = {}
    #     stats['patients'] = self.insert_staging_data(
    #         'staging_patients', 
    #         self.extract_with_llm(report_text, 'patient_info'), 
    #         report_path.name
    #     )
    #     stats['encounters'] = self.insert_staging_data(
    #         'staging_encounters', 
    #         self.extract_with_llm(report_text, 'encounter'), 
    #         report_path.name
    #     )
    #     stats['conditions'] = self.insert_staging_data(
    #         'staging_conditions', 
    #         self.extract_with_llm(report_text, 'conditions'), 
    #         report_path.name
    #     )
    #     stats['medications'] = self.insert_staging_data(
    #         'staging_medications', 
    #         self.extract_with_llm(report_text, 'medications'), 
    #         report_path.name
    #     )
    #     stats['observations'] = self.insert_staging_data(
    #         'staging_observations', 
    #         self.extract_with_llm(report_text, 'observations'), 
    #         report_path.name
    #     )
    #     stats['allergies'] = self.insert_staging_data(
    #         'staging_allergies', 
    #         self.extract_with_llm(report_text, 'allergies'), 
    #         report_path.name
    #     )
    #     stats['procedures'] = self.insert_staging_data(
    #         'staging_procedures', 
    #         self.extract_with_llm(report_text, 'procedures'), 
    #         report_path.name
    #     )
    #     stats['immunizations'] = self.insert_staging_data(
    #         'staging_immunizations', 
    #         self.extract_with_llm(report_text, 'immunizations'), 
    #         report_path.name
    #     )
        
    #     print(f"  ✓ Traité: {stats}")
    #     return stats

    def process_report(self, report_path: Path) -> Dict[str, int]:
        """Traite un rapport complet et insère toutes les données de staging"""
        print(f"\n📄 Traitement : {report_path.name}")
        try:
            with open(report_path, 'r', encoding='utf-8') as f:
                report_text = f.read()
        except Exception as e:
            print(f"  ✗ Impossible de lire le fichier: {e}")
            return {}
        
        stats = {}

        # 1. Extraire les données PARENT (Patient et Encounter)
        patient_metadata = extract_patient_metadata_from_text(report_text)
        patient_data = {}
        if patient_metadata:
            patient_data = patient_metadata.copy()
        else:
            patient_data = self.extract_with_llm(report_text, 'patient_info')

        encounter_data = self.extract_with_llm(report_text, 'encounter')

        # Valider que les données de base existent
        if not patient_data or not isinstance(patient_data, dict):
            patient_data = {}

        # Compléter avec les métadonnées si besoin
        for key, val in (patient_metadata or {}).items():
            if key not in patient_data or not patient_data.get(key):
                patient_data[key] = val

        if not patient_data.get('id'):
            # Essayer de récupérer l'id via le nom de fichier (suffixe) si nécessaire
            id_from_filename = None
            match = re.search(r"patient_\d+_([a-f0-9-]+)\.txt", report_path.name, re.IGNORECASE)
            if match:
                id_from_filename = match.group(1)
            if id_from_filename:
                patient_data['id'] = id_from_filename

        if not patient_data.get('id'):
            print("  ✗ Échec de l'extraction de patient_info (ID manquant). Rapport ignoré.")
            return {}

        if not encounter_data or not isinstance(encounter_data, dict) or not encounter_data.get('id'):
            print("  ✗ Échec de l'extraction de encounter (ID manquant). Rapport ignoré.")
            return {}

        # Obtenir les IDs de lien
        report_patient_id = patient_data.get('id')
        report_encounter_id = encounter_data.get('id')
        
        # Lier l'encounter au patient
        encounter_data['patient_id'] = report_patient_id

        # 2. Insérer les données PARENT
        stats['patients'] = self.insert_staging_data(
            'staging_patients', patient_data, report_path.name
        )
        stats['encounters'] = self.insert_staging_data(
            'staging_encounters', encounter_data, report_path.name
        )

        # 3. Fonction "wrapper" pour traiter les tables ENFANT
        def process_child_table(table_name: str, extraction_type: str):
            child_data_list = self.extract_with_llm(report_text, extraction_type)
            if not child_data_list or not isinstance(child_data_list, list):
                return 0
            
            # FORCER les IDs parents pour garantir l'intégrité
            for item in child_data_list:
                if isinstance(item, dict):
                    item['patient_id'] = report_patient_id
                    item['encounter_id'] = report_encounter_id
            
            return self.insert_staging_data(
                table_name, child_data_list, report_path.name
            )

        # 4. Traiter toutes les tables ENFANT
        stats['conditions'] = process_child_table('staging_conditions', 'conditions')
        stats['medications'] = process_child_table('staging_medications', 'medications')
        stats['observations'] = process_child_table('staging_observations', 'observations')
        stats['allergies'] = process_child_table('staging_allergies', 'allergies')
        stats['procedures'] = process_child_table('staging_procedures', 'procedures')
        stats['immunizations'] = process_child_table('staging_immunizations', 'immunizations')
        
        print(f"  ✓ Traité: {stats}")
        return stats
    
    def process_all_reports(self, max_reports: int = None):
        """Traite tous les rapports du dossier"""
        # Recherche récursive de tous les .txt
        report_files = list(self.reports_folder.rglob("*.txt"))
        if not report_files:
            print(f"✗ Aucun fichier .txt trouvé dans : {self.reports_folder.resolve()}")
            return
        if max_reports: 
            report_files = report_files[:max_reports]
        
        print(f"\n{'='*70}\nTRAITEMENT DE {len(report_files)} RAPPORTS VERS STAGING\n{'='*70}")
        for idx, report_path in enumerate(report_files, 1):
            print(f"\n[{idx}/{len(report_files)}]")
            try:
                self.process_report(report_path)
            except Exception as e:
                print(f"✗ Erreur traitement {report_path.name}: {e}")
        print(f"\n{'='*70}\nEXTRACTION VERS STAGING TERMINÉE\n{'='*70}\n")
    
    def close(self):
        self.db_cursor.close()
        self.db_conn.close()
        print("✓ Connexion DB (Staging) fermée")

if __name__ == "__main__":
    if not GEMINI_API_KEY:
        print("✗ ERREUR: GEMINI_API_KEY n'est pas configurée dans le fichier .env")
        print("Obtenez votre clé API sur: https://makersuite.google.com/app/apikey")
        exit()
    
    print("🚀 (SCRIPT 1: EXTRACTION avec Gemini) 🚀")
    extractor = StagingExtractor(
        api_key=GEMINI_API_KEY, 
        reports_folder=REPORTS_FOLDER, 
        db_config=DB_CONFIG
    )
    extractor.process_all_reports(max_reports=None)  
    extractor.close()
    print("\n✅ Extraction terminée. Lancez 'process_staging_to_production.py'.")