# 📊 Explication : Comment les Tables SQL sont Remplies

## ✅ OUI, les tables SQL vont être remplies !

Le processus fonctionne exactement comme vous le pensez : **on extrait depuis les tables de STAGING pour remplir les tables de PRODUCTION**.

---

## 🔄 Le Processus en 3 Étapes

### **ÉTAPE 1 : Extraction → Tables de STAGING**
```bash
python extract_to_staging_db.py
```

**Ce qui se passe :**
- Lit les fichiers TXT/PDF
- Extrait les données avec Groq AI
- **STOCKE dans les tables de STAGING** (8 tables)

**Tables de STAGING remplies :**
1. `staging_patients` ← Informations des patients
2. `staging_encounters` ← Consultations
3. `staging_conditions` ← Pathologies/Diagnostics
4. `staging_medications` ← Médicaments
5. `staging_observations` ← Observations/Signes vitaux
6. `staging_allergies` ← Allergies
7. `staging_procedures` ← Actes médicaux
8. `staging_immunizations` ← Vaccinations

---

### **ÉTAPE 2 : Transfert → Tables de PRODUCTION**
```bash
python process_staging_to_production.py
```

**Ce qui se passe :**
- **LIT les données depuis les tables de STAGING**
- Valide les données
- Convertit les types (TEXT → DATE, DATETIME, INT, etc.)
- **STOCKE dans les tables de PRODUCTION** (8 tables)

**Tables de PRODUCTION remplies :**
1. `patients` ← Depuis `staging_patients`
2. `encounters` ← Depuis `staging_encounters`
3. `conditions` ← Depuis `staging_conditions`
4. `medications` ← Depuis `staging_medications`
5. `observations` ← Depuis `staging_observations`
6. `allergies` ← Depuis `staging_allergies`
7. `procedures` ← Depuis `staging_procedures`
8. `immunizations` ← Depuis `staging_immunizations`

---

### **ÉTAPE 3 : Export CSV**
```bash
python export_to_csv.py
```

**Ce qui se passe :**
- Lit les tables de PRODUCTION
- Exporte en fichiers CSV

---

## 📋 Détail du Transfert STAGING → PRODUCTION

### Exemple : Patients

**Table STAGING** (`staging_patients`) :
```sql
id: TEXT
birthdate: TEXT          ← "2020-05-15"
first_name: TEXT         ← "Ahmed"
last_name: TEXT          ← "Alaoui"
gender: TEXT             ← "M"
extraction_status: 'pending'
```

**Table PRODUCTION** (`patients`) :
```sql
id: VARCHAR(36)          ← Converti depuis TEXT
birthdate: DATE          ← Converti depuis TEXT "2020-05-15" → DATE
first_name: VARCHAR(100) ← Converti depuis TEXT
last_name: VARCHAR(100)  ← Converti depuis TEXT
gender: CHAR(1)          ← Converti depuis TEXT "M"
```

**Le script `process_staging_to_production.py` fait :**
1. `SELECT * FROM staging_patients WHERE extraction_status = 'pending'`
2. Convertit les types (TEXT → DATE, VARCHAR, etc.)
3. `INSERT INTO patients (...) VALUES (...)`
4. `UPDATE staging_patients SET extraction_status = 'validated'`

---

## 🎯 Résumé

| Étape | Script | Source | Destination | Résultat |
|-------|--------|--------|-------------|----------|
| 1 | `extract_to_staging_db.py` | Fichiers TXT/PDF | Tables STAGING | ✅ 8 tables staging remplies |
| 2 | `process_staging_to_production.py` | Tables STAGING | Tables PRODUCTION | ✅ 8 tables production remplies |
| 3 | `export_to_csv.py` | Tables PRODUCTION | Fichiers CSV | ✅ CSV exportés |

---

## ✅ Confirmation

**OUI**, c'est exactement ça :
- ✅ Les tables SQL **vont être remplies**
- ✅ On **extrait depuis les tables de STAGING**
- ✅ Pour **remplir les tables de PRODUCTION**

Le script `process_staging_to_production.py` fait exactement ce transfert automatiquement !

---

## 🔍 Pour Vérifier

Après avoir exécuté `process_staging_to_production.py`, vous verrez un résumé comme :

```
📊 RÉSUMÉ FINAL
  📋 patients: 150 ligne(s)
  📋 encounters: 200 ligne(s)
  📋 conditions: 300 ligne(s)
  📋 medications: 250 ligne(s)
  📋 observations: 400 ligne(s)
  📋 allergies: 50 ligne(s)
  📋 procedures: 100 ligne(s)
  📋 immunizations: 80 ligne(s)
  
  ✅ TOTAL: 1530 ligne(s) dans les tables de production
```

Cela confirme que les tables sont bien remplies ! 🎉

