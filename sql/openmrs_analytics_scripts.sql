-- CREATE VISIT DATA VIEW
CREATE OR REPLACE VIEW visit_data_vw AS SELECT patient_id, encounter_type_location.visit_id, visit_type_id, visit_type_name, date_started, date_stopped, visit_location_id, visit_location_name, visit_location_retired, visit_voided, obs_location.encounter_id, encounter_type_id, encounter_type_name, encounter_location_id, encounter_location_name, encounter_datetime, encounter_provider_id, obs_location.obs_id, obs_concept_id, obs_concept_name_en, obs_datetime, obs_location_id, obs_location_name, obs_value, obs_value_concept_id FROM
(SELECT obs_id, name as obs_value from obs JOIN concept_name ON value_coded = concept_name.concept_id  WHERE value_coded is NOT NULL AND locale = 'en' AND concept_name.voided = 0 AND locale_preferred = 1
UNION
SELECT obs_id,value_text as obs_value from obs WHERE value_text is NOT NULL
UNION
SELECT obs_id,value_datetime as obs_value from obs WHERE value_datetime is NOT NULL
UNION
SELECT obs_id,value_numeric as obs_value from obs  WHERE value_numeric is NOT NULL ORDER BY obs_id) as obs_value_data
JOIN
(SELECT  obs_id, obs.concept_id AS obs_concept_id, obs.encounter_id, obs_datetime, obs.location_id as obs_location_id,name as obs_location_name from obs join location on obs.location_id = location.location_id) as obs_location on obs_value_data.obs_id = obs_location.obs_id
JOIN
(SELECT concept_id, name AS obs_concept_name_en FROM concept_name WHERE locale = 'en' AND concept_name.voided = 0 AND locale_preferred = 1) as obs_concept_name on obs_location.obs_concept_id = obs_concept_name.concept_id
JOIN 
(SELECT encounter.encounter_id, encounter_type_id, encounter_type.name AS encounter_type_name, encounter.location_id AS encounter_location_id, location.name AS encounter_location_name, encounter_datetime, encounter.visit_id, encounter_provider.provider_id AS encounter_provider_id FROM encounter JOIN encounter_type ON encounter.encounter_type = encounter_type.encounter_type_id JOIN location ON encounter.location_id = location.location_id LEFT JOIN encounter_provider ON encounter.encounter_id = encounter_provider.encounter_id WHERE encounter_provider.voided = 0 OR encounter_provider.voided is NULL) AS encounter_type_location ON encounter_type_location.encounter_id = obs_location.encounter_id 
JOIN
(SELECT visit.patient_id, visit.visit_type_id, visit_type.name AS visit_type_name, visit.date_started, visit.date_stopped, visit.location_id AS visit_location_id, location.name AS visit_location_name, location.retired AS visit_location_retired, visit.voided AS visit_voided, visit.visit_id FROM visit JOIN visit_type ON visit.visit_type_id = visit_type.visit_type_id JOIN location ON visit.location_id = location.location_id) AS visit_location ON encounter_type_location.visit_id = visit_location.visit_id 
JOIN
(SELECT obs_id, value_coded AS obs_value_concept_id FROM obs) AS obs_concept_id ON obs_value_data.obs_id = obs_concept_id.obs_id
ORDER BY obs_id;

-- CREATE VISIT DATA MATERIALIZED VIEW
CREATE TABLE IF NOT EXISTS visit_data_matvw (
    patient_id INT(11),
    visit_id INT(11),
    visit_type_id INT(11),
    visit_type_name VARCHAR(255),
    date_started DATETIME,
    date_stopped DATETIME,
    visit_location_id INT(11),
    visit_location_name VARCHAR(255),
    visit_location_retired TINYINT(1),
    visit_voided TINYINT(1),
    encounter_id INT(11),
    encounter_type_id INT(11),
    encounter_type_name VARCHAR(50),
    encounter_location_id INT(11),
    encounter_location_name VARCHAR(255),
    encounter_datetime DATETIME,
    encounter_provider_id INT(11),
    obs_id INT(11),
    obs_concept_id INT(11),
    obs_concept_name_en VARCHAR(255),
    obs_datetime DATETIME,
    obs_location_id INT(11),
    obs_location_name VARCHAR(255),
    obs_value VARCHAR(255),
    obs_value_concept_id INT(11)
) ENGINE=INNODB;

-- CREATE INDEXES FOR VISIT DATA MATERIALIZED VIEW
CREATE UNIQUE INDEX obs_id_idx ON visit_data_matvw(obs_id);
CREATE INDEX visit_type_id_idx ON visit_data_matvw(visit_type_id);
CREATE INDEX obs_value_concept_id_idx ON visit_data_matvw(obs_value_concept_id);
CREATE INDEX visit_location_retired_idx ON visit_data_matvw(visit_location_retired);

-- CREATE PATIENT DATA VIEW
CREATE OR REPLACE VIEW patient_data_vw AS SELECT patient_info.patient_id,patient_identifier_info.identifier,identifier_type_id,identifier_type_name,identifier_location_id,identifier_location_name,identifier_location_retired,given_name,family_name, name_preferred, birthdate,gender,patient_voided,dead
FROM
(SELECT patient_id,patient.voided AS patient_voided,person.gender,person.birthdate,person.dead,person_name.given_name,person_name.family_name, person_name.preferred AS name_preferred
FROM patient join person ON patient.patient_id=person.person_id JOIN person_name ON person.person_id=person_name.person_id
WHERE patient.voided=0 AND person_name.voided=0) AS patient_info
JOIN
(SELECT patient_identifier.patient_id,patient_identifier.identifier,patient_identifier.identifier_type AS identifier_type_id,patient_identifier.location_id AS identifier_location_id,location.name AS identifier_location_name,location.retired AS identifier_location_retired,patient_identifier_type.name AS identifier_type_name
FROM patient_identifier
JOIN patient_identifier_type ON patient_identifier.identifier_type=patient_identifier_type.patient_identifier_type_id 
JOIN location ON patient_identifier.location_id=location.location_id 
WHERE patient_identifier.voided=0) AS patient_identifier_info ON patient_info.patient_id=patient_identifier_info.patient_id;

-- CREATE PATIENT DATA MATERIALIZED VIEW
CREATE TABLE IF NOT EXISTS patient_data (
    patient_id INT(11),
    identifier VARCHAR(50),
    identifier_type_id INT(11),
    identifier_type_name VARCHAR(50),
    identifier_location_id INT(11),
    identifier_location_name VARCHAR(255),
    identifier_location_retired TINYINT(1),
    given_name VARCHAR(50),
    family_name VARCHAR(50),
    name_preferred TINYINT(1),
    birthdate DATE,
    gender VARCHAR(50),
    patient_voided TINYINT(1),
    dead TINYINT(1)
) ENGINE=INNODB;

-- CREATE INDEXES FOR PATIENT DATA MATERIALIZED VIEW
CREATE INDEX patient_id_idx ON patient_data_matvw(patient_id);

-- CREATE PROCEDURE FOR REFRESHING MATERIALIZED VIEWS
DELIMITER $$
CREATE PROCEDURE RefreshMaterializedViews()
    BEGIN
        TRUNCATE TABLE visit_data_matvw;
        TRUNCATE TABLE patient_data_matvw;
        INSERT INTO visit_data_matvw (SELECT * FROM visit_data_vw);
        INSERT INTO patient_data_matvw (SELECT * FROM patient_data_vw);
    END
$$

-- REFRESH MATERIALIZED VIEWS
CALL RefreshMaterializedViews();
