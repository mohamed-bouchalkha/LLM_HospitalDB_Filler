# 📊 RAPPORT : Rôle de `extract_to_staging_db.py`

## 🎯 Vue d'ensemble

**`extract_to_staging_db.py`** est le **Script 1** d'un système d'extraction intelligent qui transforme des **rapports médicaux non structurés** (fichiers texte) en **données structurées** stockées dans des **tables de staging SQL**.

---

## 🔑 Rôle Principal

Ce script sert de **pont intelligent** entre :
- **ENTRÉE** : Rapports médicaux bruts (fichiers `.txt` dans `moroccan_unstructured_data/txt/`)
- **SORTIE** : Tables de staging MySQL avec données structurées et normalisées

---

## 🏗️ Architecture et Fonctionnement

### 1. **Technologie Utilisée**
- **API Gemini 2.5 Flash** : Modèle d'IA de Google pour l'extraction de données
- **MySQL** : Base de données de staging
- **Python** : Langage de programmation

### 2. **Flux de Traitement**

```
Fichiers .txt (rapports médicaux)
    ↓
Extraction avec Gemini AI
    ↓
Structuration en JSON
    ↓
Insertion dans tables de staging MySQL
```

---

## 📋 Fonctionnalités Détaillées

### **A. Extraction de Métadonnées Patient**
- **Fonction** : `extract_patient_metadata_from_text()`
- **Rôle** : Parse les métadonnées JSON pré-formatées dans le texte du rapport
- **Format attendu** : `METADONNEES_PATIENT_JSON: {...}`

### **B. Extraction Intelligente avec IA**
- **Méthode** : `extract_with_llm()`
- **Rôle** : Utilise Gemini AI pour extraire 8 types de données différents :
  1. **Informations patient** (`patient_info`)
  2. **Consultations** (`encounter`)
  3. **Pathologies/Diagnostics** (`conditions`)
  4. **Médicaments** (`medications`)
  5. **Observations/Vitales** (`observations`)
  6. **Allergies** (`allergies`)
  7. **Actes médicaux** (`procedures`)
  8. **Vaccinations** (`immunizations`)

### **C. Insertion dans la Base de Données**
- **Méthode** : `insert_staging_data()`
- **Rôle** :
  - Valide les colonnes avant insertion
  - Filtre les données pour correspondre au schéma SQL
  - Gère les doublons (erreur 1062)
  - Ajoute le nom du fichier source (`report_filename`)

### **D. Traitement Complet d'un Rapport**
- **Méthode** : `process_report()`
- **Étapes** :
  1. Lecture du fichier texte
  2. Extraction des métadonnées patient (ou utilisation de l'IA)
  3. Génération d'IDs uniques (UUID) si manquants
  4. Extraction des données de consultation
  5. Insertion patient + consultation
  6. Extraction et insertion des tables enfants (conditions, médicaments, etc.)
  7. Liaison automatique avec `patient_id` et `encounter_id`

### **E. Traitement en Lot**
- **Méthode** : `process_all_reports()`
- **Rôle** : Traite tous les fichiers `.txt` du dossier de manière récursive
- **Option** : Limite possible avec `max_reports`

---

## 🗄️ Tables de Staging Ciblées

Le script insère les données dans les tables suivantes :

| Table | Contenu |
|-------|---------|
| `staging_patients` | Informations démographiques des patients |
| `staging_encounters` | Consultations/visites médicales |
| `staging_conditions` | Diagnostics et pathologies |
| `staging_medications` | Prescriptions médicamenteuses |
| `staging_observations` | Mesures vitales et observations |
| `staging_allergies` | Allergies déclarées |
| `staging_procedures` | Actes médicaux et interventions |
| `staging_immunizations` | Vaccinations |

---

## ⚙️ Configuration Requise

### Variables d'environnement (`.env`) :
```env
GEMINI_API_KEY=...      # Clé API Google Gemini
DB_HOST=...             # Hôte MySQL
DB_USER=...             # Utilisateur MySQL
DB_PASS=...             # Mot de passe MySQL
DB_NAME=...             # Nom de la base de données
```

### Structure de dossiers :
```
moroccan_unstructured_data/
  └── txt/
      ├── rapport1.txt
      ├── rapport2.txt
      └── ...
```

---

## 🔄 Position dans le Pipeline

Ce script est le **premier maillon** d'un pipeline de traitement :

```
1. extract_to_staging_db.py  ← VOUS ÊTES ICI
   ↓
2. process_staging_to_production.py
   ↓
3. Base de données de production
```

**Note finale** : Après l'exécution, le message indique de lancer `process_staging_to_production.py` pour la suite.

---

## ✨ Points Forts

1. **Intelligence Artificielle** : Utilise Gemini pour comprendre le contexte médical
2. **Robustesse** : Gestion d'erreurs, validation des données
3. **Traçabilité** : Chaque enregistrement garde le nom du fichier source
4. **Flexibilité** : Peut traiter des rapports avec ou sans métadonnées pré-formatées
5. **Performance** : Traitement en lot avec possibilité de limiter le nombre de rapports

---

## 📝 Exemple d'Utilisation

```python
extractor = StagingExtractor(
    api_key=GEMINI_API_KEY,
    reports_folder=Path("moroccan_unstructured_data/txt"),
    db_config=DB_CONFIG
)

# Traiter tous les rapports
extractor.process_all_reports()

# Ou limiter à 10 rapports pour tester
extractor.process_all_reports(max_reports=10)

extractor.close()
```

---

**Date du rapport** : Généré automatiquement  
**Version du script** : Gemini API

