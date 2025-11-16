-- ═══════════════════════════════════════════════════════════════
-- PARTIE 1 : VOS TABLES DE PRODUCTION (ORIGINALES)
-- ═══════════════════════════════════════════════════════════════

-- ═══════════════════════════════════════════════════════════════
-- SCHÉMA SQL COMPLET - BASE DE DONNÉES HOSPITALIÈRE SYNTHEA
-- Système de Gestion des Dossiers Médicaux Électroniques
-- ═══════════════════════════════════════════════════════════════

-- Nettoyage : Suppression des tables existantes (ordre inverse des dépendances)
DROP TABLE IF EXISTS supplies CASCADE;
DROP TABLE IF EXISTS imaging_studies CASCADE;
DROP TABLE IF EXISTS immunizations CASCADE;
DROP TABLE IF EXISTS devices CASCADE;
DROP TABLE IF EXISTS observations CASCADE;
DROP TABLE IF EXISTS procedures CASCADE;
DROP TABLE IF EXISTS medications CASCADE;
DROP TABLE IF EXISTS careplans CASCADE;
DROP TABLE IF EXISTS conditions CASCADE;
DROP TABLE IF EXISTS allergies CASCADE;
DROP TABLE IF EXISTS encounters CASCADE;
DROP TABLE IF EXISTS payer_transitions CASCADE;
DROP TABLE IF EXISTS providers CASCADE;
DROP TABLE IF EXISTS payers CASCADE;
DROP TABLE IF EXISTS organizations CASCADE;
DROP TABLE IF EXISTS patients CASCADE;
DROP PROCEDURE IF EXISTS sp_get_patient_medical_record;

-- ═══════════════════════════════════════════════════════════════
-- TABLE 1 : PATIENTS (Table centrale - Informations démographiques)
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE patients (
    id VARCHAR(36) PRIMARY KEY,
    birthdate DATE NOT NULL,
    deathdate DATE,
    ssn VARCHAR(11),
    drivers VARCHAR(20),
    passport VARCHAR(20),
    prefix VARCHAR(10),
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    suffix VARCHAR(10),
    maiden VARCHAR(100),
    marital VARCHAR(1),  -- M=Married, S=Single
    race VARCHAR(50),
    ethnicity VARCHAR(50),
    gender CHAR(1) NOT NULL,  -- M/F/autre
    birthplace VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    county VARCHAR(100),
    zip VARCHAR(10),
    lat DECIMAL(10, 7),
    lon DECIMAL(10, 7),
    healthcare_expenses DECIMAL(12, 2),
    healthcare_coverage DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_patient_name (last_name, first_name),
    INDEX idx_patient_birthdate (birthdate),
    INDEX idx_patient_ssn (ssn),
    INDEX idx_patient_location (city, state, zip)
);

-- ═══════════════════════════════════════════════════════════════
-- TABLE 2 : ORGANIZATIONS (Établissements de santé)
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE organizations (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip VARCHAR(10),
    lat DECIMAL(10, 7),
    lon DECIMAL(10, 7),
    phone VARCHAR(20),
    revenue DECIMAL(15, 2),
    utilization INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_org_name (name),
    INDEX idx_org_location (city, state)
);

-- ═══════════════════════════════════════════════════════════════
-- TABLE 3 : PAYERS (Assureurs / Mutuelles)
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE payers (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    address VARCHAR(255),
    city VARCHAR(100),
    state_headquartered VARCHAR(50),
    zip VARCHAR(10),
    phone VARCHAR(20),
    amount_covered DECIMAL(15, 2) DEFAULT 0,
    amount_uncovered DECIMAL(15, 2) DEFAULT 0,
    revenue DECIMAL(15, 2) DEFAULT 0,
    covered_encounters INT DEFAULT 0,
    uncovered_encounters INT DEFAULT 0,
    covered_medications INT DEFAULT 0,
    uncovered_medications INT DEFAULT 0,
    covered_procedures INT DEFAULT 0,
    uncovered_procedures INT DEFAULT 0,
    covered_immunizations INT DEFAULT 0,
    uncovered_immunizations INT DEFAULT 0,
    unique_customers INT DEFAULT 0,
    qols_avg DECIMAL(5, 4),
    member_months INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_payer_name (name)
);

-- ═══════════════════════════════════════════════════════════════
-- TABLE 4 : PROVIDERS (Praticiens de santé)
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE providers (
    id VARCHAR(36) PRIMARY KEY,
    organization_id VARCHAR(36),
    name VARCHAR(255) NOT NULL,
    gender CHAR(1),
    speciality VARCHAR(100),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip VARCHAR(10),
    lat DECIMAL(10, 7),
    lon DECIMAL(10, 7),
    utilization INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (organization_id) REFERENCES organizations(id) ON DELETE SET NULL,
    INDEX idx_provider_org (organization_id),
    INDEX idx_provider_name (name),
    INDEX idx_provider_speciality (speciality)
);

-- ═══════════════════════════════════════════════════════════════
-- TABLE 5 : PAYER_TRANSITIONS (Historique des couvertures)
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE payer_transitions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    patient_id VARCHAR(36) NOT NULL,
    start_year INT NOT NULL,
    end_year INT NOT NULL,
    payer_id VARCHAR(36),
    ownership VARCHAR(50),  -- Guardian, Self, etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (patient_id) REFERENCES patients(id) ON DELETE CASCADE,
    FOREIGN KEY (payer_id) REFERENCES payers(id) ON DELETE SET NULL,
    INDEX idx_transition_patient (patient_id),
    INDEX idx_transition_years (start_year, end_year)
);

-- ═══════════════════════════════════════════════════════════════
-- TABLE 6 : ENCOUNTERS (Rencontres médicales / Consultations)
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE encounters (
    id VARCHAR(36) PRIMARY KEY,
    start_datetime DATETIME NOT NULL,
    stop_datetime DATETIME,
    patient_id VARCHAR(36) NOT NULL,
    organization_id VARCHAR(36),
    provider_id VARCHAR(36),
    payer_id VARCHAR(36),
    encounter_class VARCHAR(50),  -- ambulatory, wellness, emergency, inpatient, etc.
    code BIGINT,
    description TEXT,
    base_encounter_cost DECIMAL(10, 2),
    total_claim_cost DECIMAL(10, 2),
    payer_coverage DECIMAL(10, 2),
    reason_code BIGINT,
    reason_description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (patient_id) REFERENCES patients(id) ON DELETE CASCADE,
    FOREIGN KEY (organization_id) REFERENCES organizations(id) ON DELETE SET NULL,
    FOREIGN KEY (provider_id) REFERENCES providers(id) ON DELETE SET NULL,
    FOREIGN KEY (payer_id) REFERENCES payers(id) ON DELETE SET NULL,
    INDEX idx_encounter_patient (patient_id),
    INDEX idx_encounter_date (start_datetime),
    INDEX idx_encounter_class (encounter_class),
    INDEX idx_encounter_provider (provider_id)
);

-- ═══════════════════════════════════════════════════════════════
-- TABLE 7 : ALLERGIES (Allergies des patients)
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE allergies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    start_date DATE NOT NULL,
    stop_date DATE,
    patient_id VARCHAR(36) NOT NULL,
    encounter_id VARCHAR(36),
    code BIGINT NOT NULL,
    description VARCHAR(500) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (patient_id) REFERENCES patients(id) ON DELETE CASCADE,
    FOREIGN KEY (encounter_id) REFERENCES encounters(id) ON DELETE SET NULL,
    INDEX idx_allergy_patient (patient_id),
    INDEX idx_allergy_active (patient_id, stop_date)
);

-- ═══════════════════════════════════════════════════════════════
-- TABLE 8 : CONDITIONS (Pathologies / Diagnostics)
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE conditions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    start_date DATE NOT NULL,
    stop_date DATE,
    patient_id VARCHAR(36) NOT NULL,
    encounter_id VARCHAR(36),
    code BIGINT NOT NULL,
    description TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (patient_id) REFERENCES patients(id) ON DELETE CASCADE,
    FOREIGN KEY (encounter_id) REFERENCES encounters(id) ON DELETE SET NULL,
    INDEX idx_condition_patient (patient_id),
    INDEX idx_condition_code (code),
    INDEX idx_condition_active (patient_id, stop_date)
);

-- ═══════════════════════════════════════════════════════════════
-- TABLE 9 : CAREPLANS (Plans de soins)
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE careplans (
    id VARCHAR(36) PRIMARY KEY,
    start_date DATE NOT NULL,
    stop_date DATE,
    patient_id VARCHAR(36) NOT NULL,
    encounter_id VARCHAR(36),
    code BIGINT NOT NULL,
    description TEXT,
    reason_code BIGINT,
    reason_description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (patient_id) REFERENCES patients(id) ON DELETE CASCADE,
    FOREIGN KEY (encounter_id) REFERENCES encounters(id) ON DELETE SET NULL,
    INDEX idx_careplan_patient (patient_id),
    INDEX idx_careplan_active (patient_id, stop_date)
);

-- ═══════════════════════════════════════════════════════════════
-- TABLE 10 : MEDICATIONS (Prescriptions médicamenteuses)
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE medications (
    id INT AUTO_INCREMENT PRIMARY KEY,
    start_datetime DATETIME NOT NULL,
    stop_datetime DATETIME,
    patient_id VARCHAR(36) NOT NULL,
    payer_id VARCHAR(36),
    encounter_id VARCHAR(36),
    code BIGINT NOT NULL,
    description TEXT NOT NULL,
    base_cost DECIMAL(10, 2),
    payer_coverage DECIMAL(10, 2),
    dispenses INT DEFAULT 1,
    total_cost DECIMAL(10, 2),
    reason_code BIGINT,
    reason_description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (patient_id) REFERENCES patients(id) ON DELETE CASCADE,
    FOREIGN KEY (payer_id) REFERENCES payers(id) ON DELETE SET NULL,
    FOREIGN KEY (encounter_id) REFERENCES encounters(id) ON DELETE SET NULL,
    INDEX idx_medication_patient (patient_id),
    INDEX idx_medication_code (code),
    INDEX idx_medication_active (patient_id, stop_datetime)
);

-- ═══════════════════════════════════════════════════════════════
-- TABLE 11 : PROCEDURES (Actes médicaux / Interventions)
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE procedures (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date_performed DATETIME NOT NULL,
    patient_id VARCHAR(36) NOT NULL,
    encounter_id VARCHAR(36),
    code BIGINT NOT NULL,
    description TEXT NOT NULL,
    base_cost DECIMAL(10, 2),
    reason_code BIGINT,
    reason_description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (patient_id) REFERENCES patients(id) ON DELETE CASCADE,
    FOREIGN KEY (encounter_id) REFERENCES encounters(id) ON DELETE SET NULL,
    INDEX idx_procedure_patient (patient_id),
    INDEX idx_procedure_date (date_performed),
    INDEX idx_procedure_code (code)
);

-- ═══════════════════════════════════════════════════════════════
-- TABLE 12 : OBSERVATIONS (Mesures cliniques / Signes vitaux)
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE observations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date_recorded DATETIME NOT NULL,
    patient_id VARCHAR(36) NOT NULL,
    encounter_id VARCHAR(36),
    code VARCHAR(50) NOT NULL,  -- LOINC code
    description TEXT NOT NULL,
    value VARCHAR(255),
    units VARCHAR(50),
    type VARCHAR(50),  -- numeric, text, etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (patient_id) REFERENCES patients(id) ON DELETE CASCADE,
    FOREIGN KEY (encounter_id) REFERENCES encounters(id) ON DELETE SET NULL,
    INDEX idx_observation_patient (patient_id),
    INDEX idx_observation_date (date_recorded),
    INDEX idx_observation_code (code)
);

-- ═══════════════════════════════════════════════════════════════
-- TABLE 13 : IMMUNIZATIONS (Vaccinations)
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE immunizations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date_administered DATETIME NOT NULL,
    patient_id VARCHAR(36) NOT NULL,
    encounter_id VARCHAR(36),
    code INT NOT NULL,  -- CVX code
    description TEXT NOT NULL,
    base_cost DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (patient_id) REFERENCES patients(id) ON DELETE CASCADE,
    FOREIGN KEY (encounter_id) REFERENCES encounters(id) ON DELETE SET NULL,
    INDEX idx_immunization_patient (patient_id),
    INDEX idx_immunization_date (date_administered),
    INDEX idx_immunization_code (code)
);

-- ═══════════════════════════════════════════════════════════════
-- TABLE 14 : DEVICES (Dispositifs médicaux implantés)
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE devices (
    id INT AUTO_INCREMENT PRIMARY KEY,
    start_datetime DATETIME NOT NULL,
    stop_datetime DATETIME,
    patient_id VARCHAR(36) NOT NULL,
    encounter_id VARCHAR(36),
    code BIGINT NOT NULL,
    description TEXT NOT NULL,
    udi VARCHAR(255),  -- Unique Device Identifier
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (patient_id) REFERENCES patients(id) ON DELETE CASCADE,
    FOREIGN KEY (encounter_id) REFERENCES encounters(id) ON DELETE SET NULL,
    INDEX idx_device_patient (patient_id),
    INDEX idx_device_code (code),
    INDEX idx_device_udi (udi)
);

-- ═══════════════════════════════════════════════════════════════
-- TABLE 15 : IMAGING_STUDIES (Examens d'imagerie médicale)
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE imaging_studies (
    id VARCHAR(36) PRIMARY KEY,
    date_performed DATETIME NOT NULL,
    patient_id VARCHAR(36) NOT NULL,
    encounter_id VARCHAR(36),
    bodysite_code BIGINT,
    bodysite_description VARCHAR(255),
    modality_code VARCHAR(10),  -- DX, CT, MR, etc.
    modality_description VARCHAR(255),
    sop_code VARCHAR(100),
    sop_description VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (patient_id) REFERENCES patients(id) ON DELETE CASCADE,
    FOREIGN KEY (encounter_id) REFERENCES encounters(id) ON DELETE SET NULL,
    INDEX idx_imaging_patient (patient_id),
    INDEX idx_imaging_date (date_performed),
    INDEX idx_imaging_modality (modality_code)
);

-- ═══════════════════════════════════════════════════════════════
-- TABLE 16 : SUPPLIES (Fournitures médicales)
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE supplies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date_used DATETIME NOT NULL,
    patient_id VARCHAR(36) NOT NULL,
    encounter_id VARCHAR(36),
    code VARCHAR(50) NOT NULL,
    description TEXT,
    quantity INT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (patient_id) REFERENCES patients(id) ON DELETE CASCADE,
    FOREIGN KEY (encounter_id) REFERENCES encounters(id) ON DELETE SET NULL,
    INDEX idx_supply_patient (patient_id),
    INDEX idx_supply_date (date_used)
);

-- ═══════════════════════════════════════════════════════════════
-- VUES UTILES POUR L'ANALYSE ET LE REPORTING
-- ═══════════════════════════════════════════════════════════════

-- Vue : Patients avec leurs informations d'assurance actuelle
CREATE OR REPLACE VIEW v_patients_with_insurance AS
SELECT 
    p.id,
    p.first_name,
    p.last_name,
    p.birthdate,
    p.gender,
    p.city,
    p.state,
    pyt.payer_id,
    py.name AS payer_name,
    pyt.ownership
FROM patients p
LEFT JOIN payer_transitions pyt ON p.id = pyt.patient_id 
    AND pyt.end_year = (SELECT MAX(end_year) FROM payer_transitions WHERE patient_id = p.id)
LEFT JOIN payers py ON pyt.payer_id = py.id;

-- Vue : Résumé des consultations par patient
CREATE OR REPLACE VIEW v_patient_encounter_summary AS
SELECT 
    p.id AS patient_id,
    CONCAT(p.first_name, ' ', p.last_name) AS patient_name,
    COUNT(e.id) AS total_encounters,
    SUM(e.total_claim_cost) AS total_medical_costs,
    SUM(e.payer_coverage) AS total_insurance_coverage,
    SUM(e.total_claim_cost - e.payer_coverage) AS total_out_of_pocket,
    MAX(e.start_datetime) AS last_visit_date
FROM patients p
LEFT JOIN encounters e ON p.id = e.patient_id
GROUP BY p.id, p.first_name, p.last_name;

-- Vue : Allergies actives par patient
CREATE OR REPLACE VIEW v_active_allergies AS
SELECT 
    a.patient_id,
    CONCAT(p.first_name, ' ', p.last_name) AS patient_name,
    a.description AS allergy,
    a.code,
    a.start_date
FROM allergies a
JOIN patients p ON a.patient_id = p.id
WHERE a.stop_date IS NULL;

-- Vue : Conditions médicales actives
-- Vue : Conditions médicales actives
CREATE OR REPLACE VIEW v_active_conditions AS
SELECT 
    c.patient_id,
    CONCAT(p.first_name, ' ', p.last_name) AS patient_name,
    c.description AS `condition`,  -- <--- LA CORRECTION EST ICI
    c.code,
    c.start_date,
    DATEDIFF(CURDATE(), c.start_date) AS days_since_diagnosis
FROM conditions c
JOIN patients p ON c.patient_id = p.id
WHERE c.stop_date IS NULL;

-- Vue : Médicaments actifs par patient
CREATE OR REPLACE VIEW v_active_medications AS
SELECT 
    m.patient_id,
    CONCAT(p.first_name, ' ', p.last_name) AS patient_name,
    m.description AS medication,
    m.code,
    m.start_datetime,
    m.base_cost,
    m.reason_description
FROM medications m
JOIN patients p ON m.patient_id = p.id
WHERE m.stop_datetime IS NULL;

-- ═══════════════════════════════════════════════════════════════
-- TRIGGERS POUR L'AUDIT ET LA MISE À JOUR AUTOMATIQUE
-- ═══════════════════════════════════════════════════════════════

-- Trigger : Mise à jour automatique du timestamp
DELIMITER //

CREATE TRIGGER trg_patients_update_timestamp
BEFORE UPDATE ON patients
FOR EACH ROW
BEGIN
    SET NEW.updated_at = CURRENT_TIMESTAMP;
END//

-- Trigger : Calcul automatique des coûts totaux pour les encounters
CREATE TRIGGER trg_encounters_calculate_costs
BEFORE INSERT ON encounters
FOR EACH ROW
BEGIN
    IF NEW.total_claim_cost IS NULL THEN
        SET NEW.total_claim_cost = NEW.base_encounter_cost;
    END IF;
    IF NEW.payer_coverage IS NULL THEN
        SET NEW.payer_coverage = 0;
    END IF;
END//

DELIMITER ;

-- ═══════════════════════════════════════════════════════════════
-- PROCÉDURES STOCKÉES UTILES
-- ═══════════════════════════════════════════════════════════════

DELIMITER //

-- Procédure : Obtenir le dossier médical complet d'un patient
CREATE PROCEDURE sp_get_patient_medical_record(IN p_patient_id VARCHAR(36))
BEGIN
    -- Informations du patient
    SELECT * FROM patients WHERE id = p_patient_id;
    
    -- Allergies
    SELECT * FROM allergies WHERE patient_id = p_patient_id ORDER BY start_date DESC;
    
    -- Conditions
    SELECT * FROM conditions WHERE patient_id = p_patient_id ORDER BY start_date DESC;
    
    -- Consultations
    SELECT * FROM encounters WHERE patient_id = p_patient_id ORDER BY start_datetime DESC LIMIT 10;
    
    -- Médicaments actifs
    SELECT * FROM medications WHERE patient_id = p_patient_id AND stop_datetime IS NULL;
    
    -- Vaccinations
    SELECT * FROM immunizations WHERE patient_id = p_patient_id ORDER BY date_administered DESC;
END//

DELIMITER ;

-- ═══════════════════════════════════════════════════════════════
-- COMMENTAIRES POUR LA DOCUMENTATION
-- ═══════════════════════════════════════════════════════════════

-- Structure de la base de données :
-- 1. Tables principales : patients, organizations, payers, providers
-- 2. Tables de liaison : encounters (table pivot centrale)
-- 3. Tables médicales : conditions, medications, procedures, observations
-- 4. Tables complémentaires : allergies, immunizations, devices, imaging_studies
-- 5. Tables de support : careplans, payer_transitions, supplies

-- Clés étrangères respectent la cascade :
-- - ON DELETE CASCADE pour les données dépendantes du patient
-- - ON DELETE SET NULL pour les références externes (organisations, providers, etc.)

-- Index optimisés pour :
-- - Recherches par patient (index sur patient_id partout)
-- - Recherches temporelles (index sur dates)
-- - Recherches par code médical (SNOMED, LOINC, CVX, RxNorm)

COMMIT;


-- ═══════════════════════════════════════════════════════════════
-- PARTIE 2 : TABLES DE STAGING (À AJOUTER À LA FIN)
-- ═══════════════════════════════════════════════════════════════

DROP TABLE IF EXISTS staging_patients;
CREATE TABLE staging_patients (
    id TEXT,
    first_name TEXT,
    last_name TEXT,
    birthdate TEXT,
    gender TEXT,
    ssn TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    race TEXT,
    ethnicity TEXT,
    report_filename VARCHAR(255),
    extraction_status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,
    processed_at TIMESTAMP,
    UNIQUE KEY uk_staging_patient_id (id(36))
);

DROP TABLE IF EXISTS staging_encounters;
CREATE TABLE staging_encounters (
    id TEXT,
    start_datetime TEXT,
    stop_datetime TEXT,
    patient_id TEXT,
    organization_id TEXT,
    provider_id TEXT,
    encounter_class TEXT,
    code TEXT,
    description TEXT,
    base_encounter_cost TEXT,
    total_claim_cost TEXT,
    payer_coverage TEXT,
    reason_description TEXT,
    report_filename VARCHAR(255),
    extraction_status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,
    processed_at TIMESTAMP,
    UNIQUE KEY uk_staging_encounter_id (id(36))
);

DROP TABLE IF EXISTS staging_conditions;
CREATE TABLE staging_conditions (
    staging_id INT AUTO_INCREMENT PRIMARY KEY,
    patient_id TEXT,
    encounter_id TEXT,
    start_date TEXT,
    stop_date TEXT,
    code TEXT,
    description TEXT,
    report_filename VARCHAR(255),
    extraction_status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,
    processed_at TIMESTAMP
);

DROP TABLE IF EXISTS staging_medications;
CREATE TABLE staging_medications (
    staging_id INT AUTO_INCREMENT PRIMARY KEY,
    patient_id TEXT,
    encounter_id TEXT,
    start_datetime TEXT,
    stop_datetime TEXT,
    code TEXT,
    description TEXT,
    base_cost TEXT,
    total_cost TEXT,
    payer_coverage TEXT,
    reason_description TEXT,
    report_filename VARCHAR(255),
    extraction_status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,
    processed_at TIMESTAMP
);

DROP TABLE IF EXISTS staging_observations;
CREATE TABLE staging_observations (
    staging_id INT AUTO_INCREMENT PRIMARY KEY,
    patient_id TEXT,
    encounter_id TEXT,
    date_recorded TEXT,
    code TEXT,
    description TEXT,
    value TEXT,
    units TEXT,
    type TEXT,
    report_filename VARCHAR(255),
    extraction_status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,
    processed_at TIMESTAMP
);

DROP TABLE IF EXISTS staging_allergies;
CREATE TABLE staging_allergies (
    staging_id INT AUTO_INCREMENT PRIMARY KEY,
    patient_id TEXT,
    encounter_id TEXT,
    start_date TEXT,
    stop_date TEXT,
    code TEXT,
    description TEXT,
    report_filename VARCHAR(255),
    extraction_status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,
    processed_at TIMESTAMP
);

DROP TABLE IF EXISTS staging_procedures;
CREATE TABLE staging_procedures (
    staging_id INT AUTO_INCREMENT PRIMARY KEY,
    patient_id TEXT,
    encounter_id TEXT,
    date_performed TEXT,
    code TEXT,
    description TEXT,
    base_cost TEXT,
    reason_description TEXT,
    report_filename VARCHAR(255),
    extraction_status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,
    processed_at TIMESTAMP
);

DROP TABLE IF EXISTS staging_immunizations;
CREATE TABLE staging_immunizations (
    staging_id INT AUTO_INCREMENT PRIMARY KEY,
    patient_id TEXT,
    encounter_id TEXT,
    date_administered TEXT,
    code TEXT,
    description TEXT,
    base_cost TEXT,
    report_filename VARCHAR(255),
    extraction_status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,
    processed_at TIMESTAMP
);

COMMIT;