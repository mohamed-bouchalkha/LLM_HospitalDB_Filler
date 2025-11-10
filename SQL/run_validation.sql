-- run_validation.sql
-- Crée la procédure stockée pour valider les tables de staging.

DELIMITER $$

DROP PROCEDURE IF EXISTS sp_validate_staging_data$$

CREATE PROCEDURE sp_validate_staging_data()
BEGIN
    DECLARE affected_rows INT;
    
    SELECT '--- LANCEMENT DE LA VALIDATION SQL ---' AS 'STATUS';

    -- SECTION 1 & 4: ANOMALIES & DOUBLONS (Patients)
    UPDATE staging_patients 
    SET extraction_status = 'error', error_message = 'ID ou nom manquant'
    WHERE (id IS NULL OR id = '' OR first_name IS NULL OR first_name = '')
    AND extraction_status = 'pending';
    
    SET affected_rows = ROW_COUNT();
    SELECT CONCAT('  [PATIENTS] > ', affected_rows, ' marqués (ID/nom manquant).') AS 'LOG';

    UPDATE staging_patients p
    JOIN (
        SELECT ssn, MIN(id) as min_id
        FROM (SELECT id, ssn FROM staging_patients WHERE ssn IS NOT NULL AND ssn != '' AND extraction_status = 'pending') as temp
        GROUP BY ssn
        HAVING COUNT(ssn) > 1
    ) AS duplicates ON p.ssn = duplicates.ssn AND p.id != duplicates.min_id
    SET p.extraction_status = 'error', p.error_message = 'Doublon de SSN'
    WHERE p.extraction_status = 'pending';
    
    SET affected_rows = ROW_COUNT();
    SELECT CONCAT('  [PATIENTS] > ', affected_rows, ' marqués (doublon SSN).') AS 'LOG';

    -- SECTION 1 & 3: ANOMALIES & VALIDATION (Encounters)
    UPDATE staging_encounters 
    SET extraction_status = 'error', error_message = 'ID ou start_datetime manquant'
    WHERE (id IS NULL OR id = '' OR start_datetime IS NULL OR start_datetime = '')
    AND extraction_status = 'pending';
    
    SET affected_rows = ROW_COUNT();
    SELECT CONCAT('  [ENCOUNTERS] > ', affected_rows, ' marqués (ID/date manquante).') AS 'LOG';
    
    UPDATE staging_encounters s
    LEFT JOIN staging_patients p ON s.patient_id = p.id
    SET s.extraction_status = 'error', 
        s.error_message = 'Clé étrangère: patient_id (staging) non trouvé'
    WHERE s.extraction_status = 'pending' AND p.id IS NULL;
    
    SET affected_rows = ROW_COUNT();
    SELECT CONCAT('  [ENCOUNTERS] > ', affected_rows, ' marqués (orphelins patient_id).') AS 'LOG';

    -- SECTION 3: VALIDATION (Clés étrangères - Tables génériques)
    UPDATE staging_conditions s LEFT JOIN staging_encounters e ON s.encounter_id = e.id
    SET s.extraction_status = 'error', s.error_message = 'Clé étrangère: encounter_id (staging) non trouvé'
    WHERE s.extraction_status = 'pending' AND e.id IS NULL;
    SET affected_rows = ROW_COUNT();
    SELECT CONCAT('  [CONDITIONS] > ', affected_rows, ' marquées (orphelins encounter_id).') AS 'LOG';

    UPDATE staging_medications s LEFT JOIN staging_encounters e ON s.encounter_id = e.id
    SET s.extraction_status = 'error', s.error_message = 'Clé étrangère: encounter_id (staging) non trouvé'
    WHERE s.extraction_status = 'pending' AND e.id IS NULL;
    SET affected_rows = ROW_COUNT();
    SELECT CONCAT('  [MEDICATIONS] > ', affected_rows, ' marqués (orphelins encounter_id).') AS 'LOG';
    
    UPDATE staging_observations s LEFT JOIN staging_encounters e ON s.encounter_id = e.id
    SET s.extraction_status = 'error', s.error_message = 'Clé étrangère: encounter_id (staging) non trouvé'
    WHERE s.extraction_status = 'pending' AND e.id IS NULL;
    SET affected_rows = ROW_COUNT();
    SELECT CONCAT('  [OBSERVATIONS] > ', affected_rows, ' marqués (orphelins encounter_id).') AS 'LOG';

    UPDATE staging_allergies s LEFT JOIN staging_encounters e ON s.encounter_id = e.id
    SET s.extraction_status = 'error', s.error_message = 'Clé étrangère: encounter_id (staging) non trouvé'
    WHERE s.extraction_status = 'pending' AND e.id IS NULL;
    SET affected_rows = ROW_COUNT();
    SELECT CONCAT('  [ALLERGIES] > ', affected_rows, ' marqués (orphelins encounter_id).') AS 'LOG';

    UPDATE staging_procedures s LEFT JOIN staging_encounters e ON s.encounter_id = e.id
    SET s.extraction_status = 'error', s.error_message = 'Clé étrangère: encounter_id (staging) non trouvé'
    WHERE s.extraction_status = 'pending' AND e.id IS NULL;
    SET affected_rows = ROW_COUNT();
    SELECT CONCAT('  [PROCEDURES] > ', affected_rows, ' marqués (orphelins encounter_id).') AS 'LOG';
    
    UPDATE staging_immunizations s LEFT JOIN staging_encounters e ON s.encounter_id = e.id
    SET s.extraction_status = 'error', s.error_message = 'Clé étrangère: encounter_id (staging) non trouvé'
    WHERE s.extraction_status = 'pending' AND e.id IS NULL;
    SET affected_rows = ROW_COUNT();
    SELECT CONCAT('  [IMMUNIZATIONS] > ', affected_rows, ' marqués (orphelins encounter_id).') AS 'LOG';

    SELECT '--- VALIDATION SQL TERMINÉE ---' AS 'STATUS';

END$$

DELIMITER ;