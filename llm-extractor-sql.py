"""
Système d'Extraction Intelligent : Rapports Non Structurés → Base de Données SQL
Utilise les LLM (Claude/GPT) pour extraire les données des rapports médicaux
et les insérer dans une base MySQL/PostgreSQL
"""

import os
import re
import json
import mysql.connector
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import anthropic  # Pour Claude AI
# Alternative : import openai  # Pour GPT

class MedicalReportExtractor:
    """Extrait les données structurées des rapports médicaux non structurés"""
    
    def __init__(self, api_key: str, reports_folder: str, db_config: dict):
        """
        Initialise l'extracteur avec connexion LLM et DB
        
        Args:
            api_key: Clé API Claude/GPT
            reports_folder: Dossier contenant les rapports .txt
            db_config: Configuration de la base de données
        """
        self.reports_folder = Path(reports_folder)
        self.client = anthropic.Anthropic(api_key=api_key)
        
        # Connexion à la base de données
        self.db_conn = mysql.connector.connect(**db_config)
        self.db_cursor = self.db_conn.cursor()
        
        print("✓ Connexion à la base de données établie")
        print(f"✓ Dossier des rapports : {self.reports_folder}")
    
    def extract_with_llm(self, report_text: str, extraction_type: str) -> Dict:
        """
        Utilise Claude AI pour extraire les données structurées du rapport
        
        Args:
            report_text: Contenu du rapport médical
            extraction_type: Type de données à extraire
            
        Returns:
            Dictionnaire avec les données extraites
        """
        
        prompts = {
            'patient_info': """
Extrait les informations du patient de ce rapport médical.
Retourne un JSON avec ces champs :
{
    "id": "UUID du patient",
    "first_name": "Prénom",
    "last_name": "Nom",
    "birthdate": "YYYY-MM-DD",
    "gender": "M ou F",
    "ssn": "Numéro sécurité sociale",
    "address": "Adresse complète",
    "city": "Ville",
    "state": "État/Région",
    "zip": "Code postal",
    "race": "Origine ethnique",
    "ethnicity": "Ethnicité"
}
""",
            'encounter': """
Extrait les informations de la consultation de ce rapport.
Retourne un JSON avec ces champs :
{
    "id": "UUID de la consultation",
    "start_datetime": "YYYY-MM-DD HH:MM:SS",
    "stop_datetime": "YYYY-MM-DD HH:MM:SS",
    "patient_id": "UUID du patient",
    "organization_id": "UUID de l'organisation",
    "provider_id": "UUID du praticien",
    "encounter_class": "Type de visite",
    "code": "Code médical",
    "description": "Description",
    "base_encounter_cost": 0.00,
    "total_claim_cost": 0.00,
    "payer_coverage": 0.00,
    "reason_description": "Motif"
}
""",
            'conditions': """
Extrait TOUTES les pathologies/diagnostics de ce rapport.
Retourne un JSON array :
[{
    "patient_id": "UUID",
    "encounter_id": "UUID",
    "start_date": "YYYY-MM-DD",
    "stop_date": "YYYY-MM-DD ou null",
    "code": 123456,
    "description": "Nom de la pathologie"
}]
""",
            'medications': """
Extrait TOUTES les prescriptions médicamenteuses du rapport.
Retourne un JSON array :
[{
    "patient_id": "UUID",
    "encounter_id": "UUID",
    "start_datetime": "YYYY-MM-DD HH:MM:SS",
    "stop_datetime": "YYYY-MM-DD HH:MM:SS ou null",
    "code": 123456,
    "description": "Nom du médicament",
    "base_cost": 0.00,
    "total_cost": 0.00,
    "payer_coverage": 0.00,
    "reason_description": "Raison"
}]
""",
            'observations': """
Extrait TOUTES les observations cliniques et mesures vitales.
Retourne un JSON array :
[{
    "patient_id": "UUID",
    "encounter_id": "UUID",
    "date_recorded": "YYYY-MM-DD HH:MM:SS",
    "code": "Code LOINC",
    "description": "Type de mesure",
    "value": "Valeur mesurée",
    "units": "Unité",
    "type": "numeric ou text"
}]
""",
            'allergies': """
Extrait TOUTES les allergies mentionnées.
Retourne un JSON array :
[{
    "patient_id": "UUID",
    "encounter_id": "UUID",
    "start_date": "YYYY-MM-DD",
    "stop_date": "YYYY-MM-DD ou null",
    "code": 123456,
    "description": "Type d'allergie"
}]
""",
            'procedures': """
Extrait TOUS les actes médicaux et procédures.
Retourne un JSON array :
[{
    "patient_id": "UUID",
    "encounter_id": "UUID",
    "date_performed": "YYYY-MM-DD HH:MM:SS",
    "code": 123456,
    "description": "Nom de la procédure",
    "base_cost": 0.00,
    "reason_description": "Raison"
}]
""",
            'immunizations': """
Extrait TOUTES les vaccinations.
Retourne un JSON array :
[{
    "patient_id": "UUID",
    "encounter_id": "UUID",
    "date_administered": "YYYY-MM-DD HH:MM:SS",
    "code": 123,
    "description": "Nom du vaccin",
    "base_cost": 0.00
}]
"""
        }
        
        prompt = prompts.get(extraction_type, "")
        
        try:
            response = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=4096,
                messages=[
                    {
                        "role": "user",
                        "content": f"{prompt}\n\nRAPPORT MÉDICAL:\n{report_text[:10000]}"
                    }
                ]
            )
            
            # Extraire le JSON de la réponse
            content = response.content[0].text
            
            # Nettoyer pour obtenir le JSON pur
            json_match = re.search(r'\{.*\}|\[.*\]', content, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            else:
                return {}
                
        except Exception as e:
            print(f"❌ Erreur extraction LLM ({extraction_type}): {e}")
            return {} if extraction_type in ['patient_info', 'encounter'] else []
    
    def insert_patient(self, patient_data: Dict) -> bool:
        """Insère un patient dans la base de données"""
        try:
            sql = """
            INSERT INTO patients (
                id, birthdate, first_name, last_name, gender, ssn, 
                address, city, state, zip, race, ethnicity
            ) VALUES (
                %(id)s, %(birthdate)s, %(first_name)s, %(last_name)s, 
                %(gender)s, %(ssn)s, %(address)s, %(city)s, 
                %(state)s, %(zip)s, %(race)s, %(ethnicity)s
            )
            ON DUPLICATE KEY UPDATE
                address = VALUES(address),
                city = VALUES(city),
                updated_at = CURRENT_TIMESTAMP
            """
            self.db_cursor.execute(sql, patient_data)
            self.db_conn.commit()
            return True
        except Exception as e:
            print(f"❌ Erreur insertion patient: {e}")
            return False
    
    def insert_encounter(self, encounter_data: Dict) -> bool:
        """Insère une consultation"""
        try:
            sql = """
            INSERT INTO encounters (
                id, start_datetime, stop_datetime, patient_id, 
                organization_id, provider_id, encounter_class, code,
                description, base_encounter_cost, total_claim_cost, 
                payer_coverage, reason_description
            ) VALUES (
                %(id)s, %(start_datetime)s, %(stop_datetime)s, %(patient_id)s,
                %(organization_id)s, %(provider_id)s, %(encounter_class)s, 
                %(code)s, %(description)s, %(base_encounter_cost)s,
                %(total_claim_cost)s, %(payer_coverage)s, %(reason_description)s
            )
            ON DUPLICATE KEY UPDATE
                stop_datetime = VALUES(stop_datetime)
            """
            self.db_cursor.execute(sql, encounter_data)
            self.db_conn.commit()
            return True
        except Exception as e:
            print(f"❌ Erreur insertion encounter: {e}")
            return False
    
    def insert_conditions(self, conditions: List[Dict]) -> int:
        """Insère les pathologies"""
        count = 0
        for condition in conditions:
            try:
                sql = """
                INSERT INTO conditions (
                    start_date, stop_date, patient_id, encounter_id, 
                    code, description
                ) VALUES (
                    %(start_date)s, %(stop_date)s, %(patient_id)s, 
                    %(encounter_id)s, %(code)s, %(description)s
                )
                """
                self.db_cursor.execute(sql, condition)
                count += 1
            except Exception as e:
                print(f"❌ Erreur condition: {e}")
        
        self.db_conn.commit()
        return count
    
    def insert_medications(self, medications: List[Dict]) -> int:
        """Insère les médicaments"""
        count = 0
        for med in medications:
            try:
                sql = """
                INSERT INTO medications (
                    start_datetime, stop_datetime, patient_id, encounter_id,
                    code, description, base_cost, total_cost, 
                    payer_coverage, reason_description
                ) VALUES (
                    %(start_datetime)s, %(stop_datetime)s, %(patient_id)s,
                    %(encounter_id)s, %(code)s, %(description)s,
                    %(base_cost)s, %(total_cost)s, %(payer_coverage)s,
                    %(reason_description)s
                )
                """
                self.db_cursor.execute(sql, med)
                count += 1
            except Exception as e:
                print(f"❌ Erreur medication: {e}")
        
        self.db_conn.commit()
        return count
    
    def insert_observations(self, observations: List[Dict]) -> int:
        """Insère les observations cliniques"""
        count = 0
        for obs in observations:
            try:
                sql = """
                INSERT INTO observations (
                    date_recorded, patient_id, encounter_id, code,
                    description, value, units, type
                ) VALUES (
                    %(date_recorded)s, %(patient_id)s, %(encounter_id)s,
                    %(code)s, %(description)s, %(value)s, %(units)s, %(type)s
                )
                """
                self.db_cursor.execute(sql, obs)
                count += 1
            except Exception as e:
                print(f"❌ Erreur observation: {e}")
        
        self.db_conn.commit()
        return count
    
    def insert_allergies(self, allergies: List[Dict]) -> int:
        """Insère les allergies"""
        count = 0
        for allergy in allergies:
            try:
                sql = """
                INSERT INTO allergies (
                    start_date, stop_date, patient_id, encounter_id,
                    code, description
                ) VALUES (
                    %(start_date)s, %(stop_date)s, %(patient_id)s,
                    %(encounter_id)s, %(code)s, %(description)s
                )
                """
                self.db_cursor.execute(sql, allergy)
                count += 1
            except Exception as e:
                print(f"❌ Erreur allergy: {e}")
        
        self.db_conn.commit()
        return count
    
    def insert_procedures(self, procedures: List[Dict]) -> int:
        """Insère les procédures médicales"""
        count = 0
        for proc in procedures:
            try:
                sql = """
                INSERT INTO procedures (
                    date_performed, patient_id, encounter_id, code,
                    description, base_cost, reason_description
                ) VALUES (
                    %(date_performed)s, %(patient_id)s, %(encounter_id)s,
                    %(code)s, %(description)s, %(base_cost)s, 
                    %(reason_description)s
                )
                """
                self.db_cursor.execute(sql, proc)
                count += 1
            except Exception as e:
                print(f"❌ Erreur procedure: {e}")
        
        self.db_conn.commit()
        return count
    
    def insert_immunizations(self, immunizations: List[Dict]) -> int:
        """Insère les vaccinations"""
        count = 0
        for immun in immunizations:
            try:
                sql = """
                INSERT INTO immunizations (
                    date_administered, patient_id, encounter_id, code,
                    description, base_cost
                ) VALUES (
                    %(date_administered)s, %(patient_id)s, %(encounter_id)s,
                    %(code)s, %(description)s, %(base_cost)s
                )
                """
                self.db_cursor.execute(sql, immun)
                count += 1
            except Exception as e:
                print(f"❌ Erreur immunization: {e}")
        
        self.db_conn.commit()
        return count
    
    def process_report(self, report_path: Path) -> Dict[str, int]:
        """
        Traite un rapport complet et insère toutes les données
        
        Returns:
            Statistiques d'insertion
        """
        print(f"\n📄 Traitement : {report_path.name}")
        
        with open(report_path, 'r', encoding='utf-8') as f:
            report_text = f.read()
        
        stats = {
            'patients': 0,
            'encounters': 0,
            'conditions': 0,
            'medications': 0,
            'observations': 0,
            'allergies': 0,
            'procedures': 0,
            'immunizations': 0
        }
        
        # 1. Extraire et insérer les infos patient
        print("  🔍 Extraction patient...")
        patient_data = self.extract_with_llm(report_text, 'patient_info')
        if patient_data and self.insert_patient(patient_data):
            stats['patients'] = 1
            print("  ✓ Patient inséré")
        
        # 2. Extraire et insérer la consultation
        if 'consultation' in report_path.name.lower():
            print("  🔍 Extraction consultation...")
            encounter_data = self.extract_with_llm(report_text, 'encounter')
            if encounter_data and self.insert_encounter(encounter_data):
                stats['encounters'] = 1
                print("  ✓ Consultation insérée")
        
        # 3. Extraire et insérer les pathologies
        print("  🔍 Extraction conditions...")
        conditions = self.extract_with_llm(report_text, 'conditions')
        if conditions:
            stats['conditions'] = self.insert_conditions(conditions)
            print(f"  ✓ {stats['conditions']} conditions insérées")
        
        # 4. Extraire et insérer les médicaments
        print("  🔍 Extraction medications...")
        medications = self.extract_with_llm(report_text, 'medications')
        if medications:
            stats['medications'] = self.insert_medications(medications)
            print(f"  ✓ {stats['medications']} médicaments insérés")
        
        # 5. Extraire et insérer les observations
        print("  🔍 Extraction observations...")
        observations = self.extract_with_llm(report_text, 'observations')
        if observations:
            stats['observations'] = self.insert_observations(observations)
            print(f"  ✓ {stats['observations']} observations insérées")
        
        # 6. Extraire et insérer les allergies
        print("  🔍 Extraction allergies...")
        allergies = self.extract_with_llm(report_text, 'allergies')
        if allergies:
            stats['allergies'] = self.insert_allergies(allergies)
            print(f"  ✓ {stats['allergies']} allergies insérées")
        
        # 7. Extraire et insérer les procédures
        print("  🔍 Extraction procedures...")
        procedures = self.extract_with_llm(report_text, 'procedures')
        if procedures:
            stats['procedures'] = self.insert_procedures(procedures)
            print(f"  ✓ {stats['procedures']} procédures insérées")
        
        # 8. Extraire et insérer les vaccinations
        if 'vaccination' in report_path.name.lower():
            print("  🔍 Extraction immunizations...")
            immunizations = self.extract_with_llm(report_text, 'immunizations')
            if immunizations:
                stats['immunizations'] = self.insert_immunizations(immunizations)
                print(f"  ✓ {stats['immunizations']} vaccinations insérées")
        
        return stats
    
    def process_all_reports(self, max_reports: int = None):
        """Traite tous les rapports du dossier"""
        
        report_files = list(self.reports_folder.glob("*.txt"))
        
        if max_reports:
            report_files = report_files[:max_reports]
        
        print(f"\n{'='*70}")
        print(f"TRAITEMENT DE {len(report_files)} RAPPORTS")
        print(f"{'='*70}")
        
        global_stats = {
            'patients': 0,
            'encounters': 0,
            'conditions': 0,
            'medications': 0,
            'observations': 0,
            'allergies': 0,
            'procedures': 0,
            'immunizations': 0
        }
        
        for idx, report_path in enumerate(report_files, 1):
            print(f"\n[{idx}/{len(report_files)}]")
            
            try:
                stats = self.process_report(report_path)
                
                # Ajouter aux statistiques globales
                for key in global_stats:
                    global_stats[key] += stats[key]
                    
            except Exception as e:
                print(f"❌ Erreur traitement {report_path.name}: {e}")
        
        # Afficher le résumé
        print(f"\n{'='*70}")
        print("STATISTIQUES GLOBALES D'INSERTION")
        print(f"{'='*70}")
        print(f"  📊 Patients insérés      : {global_stats['patients']}")
        print(f"  🏥 Consultations insérées : {global_stats['encounters']}")
        print(f"  🩺 Conditions insérées    : {global_stats['conditions']}")
        print(f"  💊 Médicaments insérés    : {global_stats['medications']}")
        print(f"  📈 Observations insérées  : {global_stats['observations']}")
        print(f"  ⚠️  Allergies insérées     : {global_stats['allergies']}")
        print(f"  🔬 Procédures insérées    : {global_stats['procedures']}")
        print(f"  💉 Vaccinations insérées  : {global_stats['immunizations']}")
        print(f"{'='*70}\n")
    
    def close(self):
        """Ferme la connexion DB"""
        self.db_cursor.close()
        self.db_conn.close()
        print("✓ Connexion DB fermée")


# ═══════════════════════════════════════════════════════════════
# UTILISATION PRINCIPALE
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    
    # Configuration de la base de données
    DB_CONFIG = {
        'host': 'localhost',
        'user': 'root',
        'password': 'votre_password',
        'database': 'hospital_db',
        'charset': 'utf8mb4'
    }
    
    # Configuration des chemins
    REPORTS_FOLDER = r"C:\Users\hp\Desktop\LLM_HospitalDB_Filler\unstructured_data"
    CLAUDE_API_KEY = "votre_cle_api_anthropic"  # Obtenir sur console.anthropic.com
    
    print("""
╔═══════════════════════════════════════════════════════════════╗
║     SYSTÈME D'EXTRACTION INTELLIGENTE - RAPPORTS → SQL        ║
║              Powered by Claude AI + MySQL                     ║
╚═══════════════════════════════════════════════════════════════╝
    """)
    
    # Créer l'extracteur
    extractor = MedicalReportExtractor(
        api_key=CLAUDE_API_KEY,
        reports_folder=REPORTS_FOLDER,
        db_config=DB_CONFIG
    )
    
    # Traiter tous les rapports (ou limiter avec max_reports=10)
    extractor.process_all_reports(max_reports=5)  # Limité à 5 pour test
    
    # Fermer les connexions
    extractor.close()
    
    print("\n✅ Traitement terminé avec succès !")