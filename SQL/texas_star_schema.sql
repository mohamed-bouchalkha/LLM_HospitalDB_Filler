-- ============================================================
-- DESCRIPTION: Patient-Centric Schema 
-- ============================================================

DROP DATABASE IF EXISTS hospital_analytics_texas;
CREATE DATABASE hospital_analytics_texas;
USE hospital_analytics_texas;

-- ============================================================
-- 1. METADATA & CONSTRAINTS
-- ============================================================
-- CREATE TABLE PARAMETRAGES ( ID_PARAMETRE VARCHAR(20), VALEUR_PARAM VARCHAR(500) );
-- INSERT INTO PARAMETRAGES VALUES ('ANOMALY_TAG', ' <?!ANOMALY>');

-- CREATE TABLE CONSTRAINTS (IDCONSTRAINT VARCHAR(20) PRIMARY KEY, CATEGORY VARCHAR(100), CONTRAINTE VARCHAR(1000));
-- -- Removed NS_UUID as source_id is gone
-- INSERT INTO CONSTRAINTS VALUES ('NS_ALPHA',  'FORMAT', '^[a-zA-Z \-\.]+$');
-- INSERT INTO CONSTRAINTS VALUES ('NS_TEXAS',  'DOMAIN', '^(TX|Texas)$'); 
-- INSERT INTO CONSTRAINTS VALUES ('NS_ZIP',    'FORMAT', '^[0-9]{5}$');

-- ============================================================
-- 2. DIMENSIONS
-- ============================================================

CREATE TABLE dim_date (
    date_key DATE PRIMARY KEY,
    year INT,
    month INT,
    month_name VARCHAR(20),
    day_of_week VARCHAR(20)
);

CREATE TABLE dim_patient (
    patient_key INT AUTO_INCREMENT PRIMARY KEY,
    -- source_id REMOVED
    full_name VARCHAR(150),
    gender VARCHAR(10),
    birthdate DATE,
    city VARCHAR(100),
    state VARCHAR(50),
    zip VARCHAR(20)
);

CREATE TABLE dim_organization (
    org_key INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50)
);

CREATE TABLE dim_provider (
    provider_key INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(150),
    specialty VARCHAR(100)
);

CREATE TABLE dim_payer (
    payer_key INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255)
);

-- ============================================================
-- 3. THE UNIFIED FACT TABLE
-- ============================================================

CREATE TABLE fact_patient_events (
    event_key INT AUTO_INCREMENT PRIMARY KEY,
    
    -- 1. FOREIGN KEYS (Linking to Integer PKs)
    patient_key INT NOT NULL,
    date_key DATE,
    provider_key INT,
    org_key INT,
    payer_key INT, 
    
    -- 2. WHAT HAPPENED
    event_category VARCHAR(50),     
    
    -- 3. DETAILS
    encounter_id VARCHAR(36), 
    code VARCHAR(50),
    description TEXT,
    
    -- 4. METRICS
    numeric_value DECIMAL(10,2),
    units VARCHAR(50),
    cost DECIMAL(10,2),
    
    -- 5. RELATIONSHIPS
    FOREIGN KEY (patient_key) REFERENCES dim_patient(patient_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (provider_key) REFERENCES dim_provider(provider_key),
    FOREIGN KEY (org_key) REFERENCES dim_organization(org_key),
    FOREIGN KEY (payer_key) REFERENCES dim_payer(payer_key)
);