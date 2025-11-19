import time
import pandas as pd
import json
import os
from google import genai
from google.genai import types
from dotenv import load_dotenv
import extraction_and_preprocessing

# 1. Configuration
load_dotenv()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
# client = genai.Client(api_key="AIzaSyCm4QU2zUK7VM8AEQycknMOKx_9D5l3tYM")
client = genai.Client(api_key="AIzaSyBDJCbFXtkCSXzo8a9oCmyXmBD3SfUWuSc")

exctraction_json_output = {}
# Chargement du dataset (Simulé ici, assurez-vous d'avoir chargé votre CSV)
# dataset = pd.read_csv("data/mtsamples.csv")

# 2. Définition du Schéma Cible (Aligné sur texas_star_schema.sql)
target_schema = """
{
  "dim_patient": {
    "full_name": "John Wayne",
    "gender": "M",
    "birthdate": "1954-05-26",
    "city": "Austin",
    "state": "TX",
    "zip": "78701"
  },
  "dim_provider": {
    "name": "Dr. Sarah Connor",
    "specialty": "Cardiology"
  },
  "dim_organization": {
    "name": "Texas Heart Institute",
    "city": "Houston",
    "state": "TX"
  },
  "dim_payer": {
    "name": "Blue Cross Blue Shield of Texas"
  },
  "fact_patient_events": [
    {
      "event_category": "Observation",
      "event_date": "2023-10-12",
      "code": null,
      "description": "Blood Pressure reading",
      "numeric_value": 145.00,
      "units": "mmHg",
      "cost": null
    },
    {
      "event_category": "Diagnosis",
      "event_date": "2023-10-12",
      "code": "I10",
      "description": "Essential (primary) hypertension",
      "numeric_value": null,
      "units": null,
      "cost": null
    },
    {
      "event_category": "Medication",
      "event_date": "2023-10-12",
      "code": "Rx-19283",
      "description": "Lisinopril 10mg tablet",
      "numeric_value": 10.00,
      "units": "mg",
      "cost": 15.50
    }
  ]
}
"""
def llm_extraction(dataset):
  count = 1500
  # 3. Boucle d'Extraction
  for idx in range(1500, 2000): # Limité à 5 pour le test
      
      # Construction du contexte à partir de la ligne du CSV
      row_context = "\n".join([f'{col}: {str(dataset[col].iloc[idx])}' for col in dataset.columns])
      
      prompt = f"""
      You are a medical data engineer. 
      
      **GOAL:** Extract data from the transcription. Since the source data is anonymized, you must **IMPUTE (GENERATE)** realistic synthetic data for missing demographics to populate a Texas Hospital Data Warehouse.

      **STRICT OUTPUT FORMAT (JSON ONLY):**
      {target_schema}
      
      **RULES FOR SYNTHETIC GENERATION (DIMENSIONS):**
      1. **dim_patient:** 
        - If City is missing, use a major Texas city (Austin, Houston, Dallas, San Antonio).
        - If State is missing, FORCE "TX".
        - If Zip is missing, generate a valid 5-digit TX zip (e.g., 75001, 78701).
        - If Birthdate is missing, generate a realistic date relative to the patient's age in text (or random between 1950-1990).
      2. **dim_provider:**
        - If Name is missing, generate a realistic doctor name (e.g., "Dr. Alice Chen").
        - If Specialty is missing, infer it from the medical context (e.g., "Cardiology", "Surgery").
      3. **dim_organization:**
        - If missing, generate a realistic hospital name in Texas (e.g., "Texas General Hospital", "Austin Medical Center").
      4. **dim_payer:**
        - If missing, generate a common insurer (e.g., "Blue Cross TX", "Aetna", "Medicare").

      **RULES FOR REALITY (FACTS):**
      5. **fact_patient_events:** DO NOT INVENT MEDICAL DATA. 
        - Only extract diagnoses, medications, labs, and vitals that are **actually present** in the text below.
        - If the text says "Weight 130", extract 130. Do not invent a weight if not listed.

      **TRANSCRIPTION:**
      {row_context}
      """

      try:
          response = client.models.generate_content(
              model="gemini-2.5-flash-lite", # Modèle rapide et efficace
              contents=prompt,
              config=types.GenerateContentConfig(
                  response_mime_type="application/json", # Force le format JSON
                  temperature=0.0 # Zéro créativité pour une extraction fidèle
              )
          )
          
          # Parsing et ajout des métadonnées
          data = json.loads(response.text)
          
          # On ajoute des métadonnées techniques pour le traçage (utile pour validation_queue)
          data['metadata'] = {
              'source_row_id': idx,
              'sample_name': str(dataset['sample_name'].iloc[idx])
          }
          
          
          print(f"✅ Row {idx} extracted.")
          exctraction_json_output[str(count)] = data
          print(data)
          count = count + 1
          if count % 5 == 0:
              print("⏳ Pausing briefly to respect API limits...")
              time.sleep(10)

      except Exception as e:
          print(f"❌ Error row {idx}: {e}")
  print(exctraction_json_output)
          
  with open("output_3.json", "w", encoding="utf-8") as f:
      json.dump(exctraction_json_output, f, indent=4)
      
      
if __name__ == "__main__" :
  dataset = pd.read_csv("data/mtsamples.csv")
  dataset['clean_transcription'] = dataset['transcription'].apply(extraction_and_preprocessing.preprocess_text)
  llm_extraction(dataset)