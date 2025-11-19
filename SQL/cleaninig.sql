-- ============================================================
-- OPTIMIZED DATA CLEANING & DEDUPLICATION SCRIPT 
-- ============================================================

USE hospital_analytics_texas;

-- Disable checks to speed up mass updates
SET FOREIGN_KEY_CHECKS = 0;
SET SQL_SAFE_UPDATES = 0;
SET UNIQUE_CHECKS = 0;

-- ============================================================
-- 1. STANDARDIZATION
-- ============================================================

SELECT '--- 🚀 STARTING STANDARDIZATION ---' AS MSG;

-- A. Patient Standardization
-- FIX 1: Explicitly remove the full_name column
-- Note: Python script handles error 1091 if column is already gone
ALTER TABLE dim_patient DROP COLUMN full_name;

UPDATE dim_patient 
SET 
    state = CASE WHEN state = 'Texas' THEN 'TX' ELSE state END,
    city = UPPER(TRIM(IFNULL(city, 'UNKNOWN')))
WHERE state = 'Texas' OR city != UPPER(TRIM(city)) OR city IS NULL;

-- B. Organization Standardization
UPDATE dim_organization
SET 
    state = CASE WHEN state = 'Texas' THEN 'TX' ELSE state END,
    city = UPPER(TRIM(IFNULL(city, 'UNKNOWN')))
WHERE state = 'Texas' OR city != UPPER(TRIM(city)) OR city IS NULL;

-- C. Provider Standardization
UPDATE dim_provider
SET 
    name = TRIM(name),
    specialty = IFNULL(NULLIF(specialty, ''), 'General Practice')
WHERE name != TRIM(name) OR specialty IS NULL OR specialty = '';

-- D. Randomization (This usually takes ~30 seconds)
SELECT '--- 🎲 RANDOMIZING DATA (Genders & Dates) ---' AS MSG;

UPDATE dim_patient
SET gender = IF(RAND() < 0.5, 'M', 'F')
WHERE gender IS NULL OR gender = '';

UPDATE dim_patient
SET birthdate = DATE_ADD('1940-01-01', INTERVAL FLOOR(RAND() * 25000) DAY)
WHERE birthdate IS NULL;

-- ============================================================
-- 2. DEDUPLICATION (The Heavy Part)
-- ============================================================

SELECT '--- 🔄 STARTING ORGANIZATION DEDUPLICATION (Wait time: ~2-5 mins) ---' AS MSG;

-- A. Deduplicate DIM_ORGANIZATION
CREATE TEMPORARY TABLE org_cleanup (
    id_to_keep INT, 
    id_to_delete INT,
    INDEX(id_to_keep), 
    INDEX(id_to_delete)
);

INSERT INTO org_cleanup
SELECT MIN(org_key), org_key
FROM dim_organization
GROUP BY name, city, state
HAVING COUNT(*) > 1;

-- HEAVY QUERY: Updating 4M+ rows
UPDATE fact_patient_events f
JOIN dim_organization bad ON f.org_key = bad.org_key
JOIN dim_organization good 
    ON bad.name = good.name 
    AND bad.city = good.city 
    AND bad.state = good.state
    AND good.org_key < bad.org_key
SET f.org_key = good.org_key;

DELETE bad FROM dim_organization bad
JOIN dim_organization good 
    ON bad.name = good.name 
    AND bad.city = good.city 
    AND bad.state = good.state
    AND good.org_key < bad.org_key;

DROP TEMPORARY TABLE org_cleanup;


SELECT '--- 🔄 STARTING PROVIDER DEDUPLICATION (Wait time: ~2 mins) ---' AS MSG;

-- B. Deduplicate DIM_PROVIDER
UPDATE fact_patient_events f
JOIN dim_provider bad ON f.provider_key = bad.provider_key
JOIN dim_provider good 
    ON bad.name = good.name 
    AND bad.specialty = good.specialty
    AND good.provider_key < bad.provider_key
SET f.provider_key = good.provider_key;

DELETE bad FROM dim_provider bad
JOIN dim_provider good 
    ON bad.name = good.name 
    AND bad.specialty = good.specialty
    AND good.provider_key < bad.provider_key;


SELECT '--- 🔄 STARTING PAYER DEDUPLICATION ---' AS MSG;

-- C. Deduplicate DIM_PAYER
UPDATE fact_patient_events f
JOIN dim_payer bad ON f.payer_key = bad.payer_key
JOIN dim_payer good 
    ON bad.name = good.name 
    AND good.payer_key < bad.payer_key
SET f.payer_key = good.payer_key;

DELETE bad FROM dim_payer bad
JOIN dim_payer good 
    ON bad.name = good.name 
    AND good.payer_key < bad.payer_key;

-- ============================================================
-- 3. FINALIZATION
-- ============================================================

SELECT '--- 📅 FINALIZING DATES ---' AS MSG;

-- FIX 2: Populate NULL date_keys BEFORE calculating Year/Month
-- Generates a random date between 2020-01-01 and 2023-09-27
UPDATE dim_date
SET date_key = DATE_ADD('2020-01-01', INTERVAL FLOOR(RAND() * 1000) DAY)
WHERE date_key IS NULL;

-- Update derived Date columns
UPDATE dim_date
SET 
    year = YEAR(date_key),
    month = MONTH(date_key),
    month_name = MONTHNAME(date_key),
    day_of_week = DAYNAME(date_key)
WHERE year IS NULL OR month IS NULL;

-- Re-enable checks
SET FOREIGN_KEY_CHECKS = 1;
SET UNIQUE_CHECKS = 1;

SELECT '--- ✅ CLEANING COMPLETE ---' AS FINAL_MSG;