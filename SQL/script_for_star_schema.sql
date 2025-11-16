-- ==========================================
-- NETTOYAGE DES ANCIENNES TABLES STAR SCHEMA
-- ==========================================
DROP TABLE IF EXISTS fact_encounters;
DROP TABLE IF EXISTS dim_patient;
DROP TABLE IF EXISTS dim_provider;
DROP TABLE IF EXISTS dim_organization;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_condition;
DROP TABLE IF EXISTS dim_medication;
DROP TABLE IF EXISTS dim_procedure;
DROP TABLE IF EXISTS dim_observation;
DROP TABLE IF EXISTS dim_allergy;
DROP TABLE IF EXISTS dim_immunization;

-- ==========================================
-- DIMENSIONS
-- ==========================================

-- DIMENSION PATIENT
CREATE TABLE dim_patient (
    patient_id INT AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(36) UNIQUE,  -- ancien id du patient du staging
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    birthdate DATE,
    gender VARCHAR(10),
    ssn VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip VARCHAR(20),
    race VARCHAR(50),
    ethnicity VARCHAR(50),
    age INT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO dim_patient (code, first_name, last_name, birthdate, gender, ssn, address, city, state, zip, race, ethnicity, age)
SELECT
    id AS code,
    first_name,
    last_name,
    STR_TO_DATE(birthdate, '%Y-%m-%d') AS birthdate,
    gender,
    ssn,
    address,
    city,
    state,
    zip,
    race,
    ethnicity,
    TIMESTAMPDIFF(YEAR, STR_TO_DATE(birthdate, '%Y-%m-%d'), CURDATE()) AS age
FROM staging_patients;

-- DIMENSION ORGANIZATION
CREATE TABLE dim_organization (
    organization_id INT AUTO_INCREMENT PRIMARY KEY,
    organization_code VARCHAR(36) NOT NULL,  -- code original du staging
    name VARCHAR(255),
    city VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO dim_organization (organization_code, name, city)
SELECT DISTINCT
    organization_id AS organization_code,
    organization_id AS name,  -- utilisation de l’ID comme nom
    'Rabat' AS city
FROM staging_encounters
WHERE organization_id IS NOT NULL;

-- DIMENSION PROVIDER
CREATE TABLE dim_provider (
    provider_id INT AUTO_INCREMENT PRIMARY KEY,
    provider_code VARCHAR(36) NOT NULL,  -- code original du staging
    name VARCHAR(255) NOT NULL,
    organization_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (organization_id) REFERENCES dim_organization(organization_id)
);

INSERT INTO dim_provider (provider_code, name, organization_id)
SELECT DISTINCT
    e.provider_id AS provider_code,
    e.provider_id AS name,
    o.organization_id
FROM staging_encounters e
JOIN dim_organization o
    ON e.organization_id = o.organization_code
WHERE e.provider_id IS NOT NULL;

-- DIMENSION DATE
CREATE TABLE dim_date (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    month_name VARCHAR(20),
    quarter INT,
    day_of_month INT,
    day_of_week INT,
    day_name VARCHAR(20),
    is_weekend BOOLEAN
);

INSERT INTO dim_date (date_id, year, month, month_name, quarter, day_of_month, day_of_week, day_name, is_weekend)
SELECT DISTINCT
    STR_TO_DATE(SUBSTRING_INDEX(start_datetime, ' ', 1), '%Y-%m-%d') AS date_id,
    YEAR(STR_TO_DATE(SUBSTRING_INDEX(start_datetime, ' ', 1), '%Y-%m-%d')) AS year,
    MONTH(STR_TO_DATE(SUBSTRING_INDEX(start_datetime, ' ', 1), '%Y-%m-%d')) AS month,
    MONTHNAME(STR_TO_DATE(SUBSTRING_INDEX(start_datetime, ' ', 1), '%Y-%m-%d')) AS month_name,
    QUARTER(STR_TO_DATE(SUBSTRING_INDEX(start_datetime, ' ', 1), '%Y-%m-%d')) AS quarter,
    DAY(STR_TO_DATE(SUBSTRING_INDEX(start_datetime, ' ', 1), '%Y-%m-%d')) AS day_of_month,
    DAYOFWEEK(STR_TO_DATE(SUBSTRING_INDEX(start_datetime, ' ', 1), '%Y-%m-%d')) AS day_of_week,
    DAYNAME(STR_TO_DATE(SUBSTRING_INDEX(start_datetime, ' ', 1), '%Y-%m-%d')) AS day_name,
    CASE WHEN DAYOFWEEK(STR_TO_DATE(SUBSTRING_INDEX(start_datetime, ' ', 1), '%Y-%m-%d')) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM staging_encounters
WHERE start_datetime IS NOT NULL;

-- ==========================================
-- TABLE DE FAITS
-- ==========================================
CREATE TABLE fact_encounters (
    encounter_id VARCHAR(36) PRIMARY KEY,
    patient_id INT,
    provider_id INT,
    organization_id INT,
    date_id DATE,
    encounter_class VARCHAR(50),
    base_encounter_cost DECIMAL(12,2),
    total_claim_cost DECIMAL(12,2),
    payer_coverage DECIMAL(12,2),
    reason_description TEXT,
    duration_minutes INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (patient_id) REFERENCES dim_patient(patient_id),
    FOREIGN KEY (provider_id) REFERENCES dim_provider(provider_id),
    FOREIGN KEY (organization_id) REFERENCES dim_organization(organization_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

INSERT INTO fact_encounters (encounter_id, patient_id, provider_id, organization_id, date_id, encounter_class, base_encounter_cost, total_claim_cost, payer_coverage, reason_description, duration_minutes)
SELECT
    e.id AS encounter_id,
    p.patient_id,
    pr.provider_id,
    o.organization_id,
    STR_TO_DATE(SUBSTRING_INDEX(e.start_datetime, ' ', 1), '%Y-%m-%d') AS date_id,
    e.encounter_class,
    CAST(e.base_encounter_cost AS DECIMAL(12,2)),
    CAST(e.total_claim_cost AS DECIMAL(12,2)),
    CAST(e.payer_coverage AS DECIMAL(12,2)),
    e.reason_description,
    TIMESTAMPDIFF(MINUTE, STR_TO_DATE(e.start_datetime, '%Y-%m-%d %H:%i:%s'), STR_TO_DATE(e.stop_datetime, '%Y-%m-%d %H:%i:%s')) AS duration_minutes
FROM staging_encounters e
JOIN dim_patient p ON e.patient_id = p.code
JOIN dim_provider pr ON e.provider_id = pr.provider_code
JOIN dim_organization o ON e.organization_id = o.organization_code;

-- ==========================================
-- DIMENSIONS COMPLÉMENTAIRES
-- ==========================================

-- Condition
CREATE TABLE dim_condition (
    condition_id INT AUTO_INCREMENT PRIMARY KEY,
    encounter_id VARCHAR(36),
    patient_id INT,
    code VARCHAR(50),
    description TEXT,
    start_date DATE,
    stop_date DATE,
    FOREIGN KEY (encounter_id) REFERENCES fact_encounters(encounter_id),
    FOREIGN KEY (patient_id) REFERENCES dim_patient(patient_id)
);

INSERT INTO dim_condition (encounter_id, patient_id, code, description, start_date, stop_date)
SELECT
    c.encounter_id,
    p.patient_id,
    c.code,
    c.description,
    STR_TO_DATE(c.start_date, '%Y-%m-%d'),
    STR_TO_DATE(c.stop_date, '%Y-%m-%d')
FROM staging_conditions c
JOIN dim_patient p ON c.patient_id = p.code
JOIN fact_encounters f ON c.encounter_id = f.encounter_id;


-- Médication
CREATE TABLE dim_medication (
    medication_id INT AUTO_INCREMENT PRIMARY KEY,
    encounter_id VARCHAR(36),
    patient_id INT,
    code VARCHAR(50),
    description TEXT,
    start_date DATETIME,
    stop_date DATETIME,
    base_cost DECIMAL(12,2),
    total_cost DECIMAL(12,2),
    payer_coverage DECIMAL(12,2),
    FOREIGN KEY (encounter_id) REFERENCES fact_encounters(encounter_id),
    FOREIGN KEY (patient_id) REFERENCES dim_patient(patient_id)
);

INSERT INTO dim_medication (encounter_id, patient_id, code, description, start_date, stop_date, base_cost, total_cost, payer_coverage)
SELECT
    m.encounter_id,
    p.patient_id,
    m.code,
    m.description,
    STR_TO_DATE(m.start_datetime, '%Y-%m-%d %H:%i:%s'),
    STR_TO_DATE(m.stop_datetime, '%Y-%m-%d %H:%i:%s'),
    CAST(m.base_cost AS DECIMAL(12,2)),
    CAST(m.total_cost AS DECIMAL(12,2)),
    CAST(m.payer_coverage AS DECIMAL(12,2))
FROM staging_medications m
JOIN dim_patient p ON m.patient_id = p.code;

-- Procédures
CREATE TABLE dim_procedure (
    procedure_id INT AUTO_INCREMENT PRIMARY KEY,
    encounter_id VARCHAR(36),
    patient_id INT,
    code VARCHAR(50),
    description TEXT,
    date_performed DATE,
    base_cost DECIMAL(12,2),
    FOREIGN KEY (encounter_id) REFERENCES fact_encounters(encounter_id),
    FOREIGN KEY (patient_id) REFERENCES dim_patient(patient_id)
);

INSERT INTO dim_procedure (encounter_id, patient_id, code, description, date_performed, base_cost)
SELECT
    pr.encounter_id,
    p.patient_id,
    pr.code,
    pr.description,
    STR_TO_DATE(pr.date_performed, '%Y-%m-%d'),
    CAST(pr.base_cost AS DECIMAL(12,2))
FROM staging_procedures pr
JOIN dim_patient p ON pr.patient_id = p.code;

-- Observations
CREATE TABLE dim_observation (
    observation_id INT AUTO_INCREMENT PRIMARY KEY,
    encounter_id VARCHAR(36),
    patient_id INT,
    code VARCHAR(50),
    description TEXT,
    value TEXT,
    units VARCHAR(50),
    type VARCHAR(50),
    date_recorded DATETIME,
    FOREIGN KEY (encounter_id) REFERENCES fact_encounters(encounter_id),
    FOREIGN KEY (patient_id) REFERENCES dim_patient(patient_id)
);

INSERT INTO dim_observation (encounter_id, patient_id, code, description, value, units, type, date_recorded)
SELECT
    o.encounter_id,
    p.patient_id,
    o.code,
    o.description,
    o.value,
    o.units,
    o.type,
    STR_TO_DATE(o.date_recorded, '%Y-%m-%d %H:%i:%s')
FROM staging_observations o
JOIN dim_patient p ON o.patient_id = p.code;

-- Allergies
CREATE TABLE dim_allergy (
    allergy_id INT AUTO_INCREMENT PRIMARY KEY,
    encounter_id VARCHAR(36),
    patient_id INT,
    code VARCHAR(50),
    description TEXT,
    start_date DATE,
    stop_date DATE,
    FOREIGN KEY (encounter_id) REFERENCES fact_encounters(encounter_id),
    FOREIGN KEY (patient_id) REFERENCES dim_patient(patient_id)
);

INSERT INTO dim_allergy (encounter_id, patient_id, code, description, start_date, stop_date)
SELECT
    a.encounter_id,
    p.patient_id,
    a.code,
    a.description,
    STR_TO_DATE(a.start_date, '%Y-%m-%d'),
    STR_TO_DATE(a.stop_date, '%Y-%m-%d')
FROM staging_allergies a
JOIN dim_patient p ON a.patient_id = p.code;

-- Immunizations
CREATE TABLE dim_immunization (
    immunization_id INT AUTO_INCREMENT PRIMARY KEY,
    encounter_id VARCHAR(36),
    patient_id INT,
    code VARCHAR(50),
    description TEXT,
    date_administered DATE,
    base_cost DECIMAL(12,2),
    FOREIGN KEY (encounter_id) REFERENCES fact_encounters(encounter_id),
    FOREIGN KEY (patient_id) REFERENCES dim_patient(patient_id)
);

INSERT INTO dim_immunization (encounter_id, patient_id, code, description, date_administered, base_cost)
SELECT
    i.encounter_id,
    p.patient_id,
    i.code,
    i.description,
    STR_TO_DATE(i.date_administered, '%Y-%m-%d'),
    CAST(i.base_cost AS DECIMAL(12,2))
FROM staging_immunizations i
JOIN dim_patient p ON i.patient_id = p.code;

COMMIT;
