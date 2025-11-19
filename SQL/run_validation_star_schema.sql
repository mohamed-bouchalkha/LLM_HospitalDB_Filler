-- Création de la table de logs
DROP PROCEDURE IF EXISTS sp_validate_staging_data;
CREATE TABLE IF NOT EXISTS staging_validation_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(50),
    message VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DELIMITER $$

CREATE PROCEDURE sp_validate_staging_data()
BEGIN
    DECLARE affected_rows INT;

    -- --- DÉMARRAGE ---
    INSERT INTO staging_validation_log(table_name, message)
    VALUES ('SYSTEM', '--- DÉMARRAGE DE LA VALIDATION ---');

    /* ==============================
       1. VALIDATION STAGING_PATIENTS
       ============================== */

    -- 1.1 Champs obligatoires manquants
    UPDATE staging_patients
    SET extraction_status = 'error',
        error_message = 'ID ou nom manquant'
    WHERE (id IS NULL OR id = '' OR first_name IS NULL OR first_name = '')
      AND extraction_status = 'pending';
    SET affected_rows = ROW_COUNT();
    INSERT INTO staging_validation_log(table_name, message)
    VALUES ('PATIENTS', CONCAT(affected_rows, ' erreurs : ID/nom manquant.'));

    -- 1.2 Doublons sur id
    UPDATE staging_patients p
    JOIN (
        SELECT id
        FROM staging_patients
        WHERE extraction_status = 'pending'
        GROUP BY id
        HAVING COUNT(*) > 1
    ) d ON d.id = p.id
    SET p.extraction_status = 'error',
        p.error_message = 'Doublon patient_id'
    WHERE p.extraction_status = 'pending';
    SET affected_rows = ROW_COUNT();
    INSERT INTO staging_validation_log(table_name, message)
    VALUES ('PATIENTS', CONCAT(affected_rows, ' erreurs : doublons id.'));

    /* ==============================
       2. VALIDATION STAGING_ENCOUNTERS
       ============================== */

    -- Champs manquants
    UPDATE staging_encounters
    SET extraction_status = 'error',
        error_message = 'ID ou start_datetime manquant'
    WHERE (id IS NULL OR id = '' OR start_datetime IS NULL OR start_datetime = '')
      AND extraction_status = 'pending';
    SET affected_rows = ROW_COUNT();
    INSERT INTO staging_validation_log(table_name, message)
    VALUES ('ENCOUNTERS', CONCAT(affected_rows, ' erreurs : ID/date manquants.'));

    -- FK patient
    UPDATE staging_encounters e
    LEFT JOIN staging_patients p ON e.patient_id = p.id
    SET e.extraction_status = 'error',
        e.error_message = 'patient_id introuvable dans staging_patients'
    WHERE p.id IS NULL
      AND e.extraction_status = 'pending';
    SET affected_rows = ROW_COUNT();
    INSERT INTO staging_validation_log(table_name, message)
    VALUES ('ENCOUNTERS', CONCAT(affected_rows, ' erreurs : patient_id orphelin.'));

    /* ==============================
       3. VALIDATION TABLES ANNEXES
       ============================== */

    -- Exemple pour CONDITIONS (idem pour autres tables)
    UPDATE staging_conditions c
    LEFT JOIN staging_encounters e ON c.encounter_id = e.id
    SET c.extraction_status = 'error',
        c.error_message = 'encounter_id introuvable'
    WHERE e.id IS NULL
      AND c.extraction_status = 'pending';
    SET affected_rows = ROW_COUNT();
    INSERT INTO staging_validation_log(table_name, message)
    VALUES ('CONDITIONS', CONCAT(affected_rows, ' erreurs encounter_id orphelin.'));

    -- Répéter pour MEDICATIONS, PROCEDURES, OBSERVATIONS, ALLERGIES, IMMUNIZATIONS
    -- en remplaçant table_name et c/m/pr/o/a/i selon la table

    INSERT INTO staging_validation_log(table_name, message)
    VALUES ('SYSTEM', '--- VALIDATION TERMINÉE ---');
END$$

DELIMITER ;
