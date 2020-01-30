-- INIT
DROP TABLE IF EXISTS patient_data_matvw;
DROP TABLE IF EXISTS visit_data_matvw;
DROP TABLE IF EXISTS age_range;
DROP TABLE IF EXISTS location_data_matvw;
DROP TRIGGER IF EXISTS age_range;
DROP PROCEDURE IF EXISTS RefreshMaterializedViews;
DROP FUNCTION IF EXISTS ViralLoadRange;

-- CREATE VISIT DATA VIEW
CREATE OR REPLACE VIEW visit_data_vw AS SELECT patient_id, encounter_type_location.visit_id, visit_type_id, visit_type_name, date_started, date_stopped, visit_location_id, visit_location_name, visit_location_retired, visit_voided, obs_location.encounter_id, encounter_type_id, encounter_type_name, encounter_location_id, encounter_location_name, encounter_datetime, encounter_provider_id,encounter_provider_person_id, encounter_provider_name, encounter_provider_retired, obs_location.obs_id, obs_concept_id, obs_concept_name_en, obs_datetime, obs_location_id, obs_location_name, obs_value, obs_value_concept_id FROM
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
(SELECT encounter.encounter_id, encounter_type_id, encounter_type.name AS encounter_type_name, encounter.location_id AS encounter_location_id, location.name AS encounter_location_name, encounter_datetime, encounter.visit_id, encounter_provider.provider_id AS encounter_provider_id,provider.person_id AS encounter_provider_person_id, concat_ws(" ",given_name,family_name) AS encounter_provider_name, provider.retired AS encounter_provider_retired FROM encounter JOIN encounter_type ON encounter.encounter_type = encounter_type.encounter_type_id JOIN location ON encounter.location_id = location.location_id LEFT JOIN encounter_provider ON encounter.encounter_id = encounter_provider.encounter_id LEFT JOIN provider ON encounter_provider.provider_id = provider.provider_id LEFT JOIN person_name ON provider.person_id = person_name.person_id WHERE (encounter_provider.voided = 0 OR encounter_provider.voided IS NULL) AND (person_name.preferred = 0 OR person_name.preferred IS  NULL)) AS encounter_type_location ON encounter_type_location.encounter_id = obs_location.encounter_id 
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
    encounter_provider_person_id INT(11), 
    encounter_provider_name VARCHAR(255),
    encounter_provider_retired INT(11),
    obs_id INT(11),
    obs_concept_id INT(11),
    obs_concept_name_en VARCHAR(255),
    obs_datetime DATETIME,
    obs_location_id INT(11),
    obs_location_name VARCHAR(255),
    obs_value TEXT,
    obs_value_concept_id INT(11),
    facility_name VARCHAR(255),
    facility_id INT(11),
    facility_dhis_ou_id VARCHAR(255),
    facility_retired TINYINT(1),
    district_name VARCHAR(255),
    district_id INT(11),
    district_retired TINYINT(1),
    province_name VARCHAR(255),
    province_id INT(11),
    province_retired TINYINT(1)
) ENGINE=INNODB;


-- CREATE INDEXES FOR VISIT DATA MATERIALIZED VIEW
CREATE UNIQUE INDEX obs_id_idx ON visit_data_matvw(obs_id);
CREATE INDEX patient_id_idx ON visit_data_matvw(patient_id);
CREATE INDEX visit_type_id_idx ON visit_data_matvw(visit_type_id);
CREATE INDEX obs_value_concept_id_idx ON visit_data_matvw(obs_value_concept_id);
CREATE INDEX visit_location_retired_idx ON visit_data_matvw(visit_location_retired);
CREATE INDEX date_started_idx ON visit_data_matvw(date_started);
CREATE INDEX facility_name_idx ON visit_data_matvw(facility_name);
CREATE INDEX district_name_idx ON visit_data_matvw(district_name);
CREATE INDEX province_name_idx ON visit_data_matvw(province_name);

-- CREATE PATIENT DATA VIEW
CREATE OR REPLACE VIEW patient_data_vw AS SELECT patient_info.patient_id,patient_identifier_info.identifier,identifier_type_id,identifier_type_name,identifier_location_id,identifier_location_name,identifier_date_created,identifier_location_retired, birthdate,TIMESTAMPDIFF(YEAR, birthdate, CURDATE()) AS age ,gender,patient_voided,dead
FROM
(SELECT patient_id,patient.voided AS patient_voided,person.gender,person.birthdate,person.dead
FROM patient join person ON patient.patient_id=person.person_id
WHERE patient.voided=0) AS patient_info
JOIN
(SELECT patient_identifier.patient_id,patient_identifier.identifier,patient_identifier.identifier_type AS identifier_type_id,patient_identifier.location_id AS identifier_location_id,location.name AS identifier_location_name,location.retired AS identifier_location_retired,patient_identifier_type.name AS identifier_type_name, patient_identifier.date_created AS identifier_date_created
FROM patient_identifier
JOIN patient_identifier_type ON patient_identifier.identifier_type=patient_identifier_type.patient_identifier_type_id 
JOIN location ON patient_identifier.location_id=location.location_id 
WHERE patient_identifier.voided=0) AS patient_identifier_info ON patient_info.patient_id=patient_identifier_info.patient_id;

-- CREATE PATIENT DATA MATERIALIZED VIEW
CREATE TABLE IF NOT EXISTS patient_data_matvw (
    patient_id INT(11),
    identifier VARCHAR(50),
    identifier_type_id INT(11),
    identifier_type_name VARCHAR(50),
    identifier_location_id INT(11),
    identifier_location_name VARCHAR(255),
    identifier_date_created DATE,
    identifier_location_retired TINYINT(1),
    birthdate DATE,
    age INT,
    age_range VARCHAR(50),
    gender VARCHAR(50),
    patient_voided TINYINT(1),
    dead TINYINT(1),
    facility_name VARCHAR(255),
    facility_id INT(11),
    facility_dhis_ou_id VARCHAR(255),
    facility_retired TINYINT(1),
    district_name VARCHAR(255),
    district_id INT(11),
    district_retired TINYINT(1),
    province_name VARCHAR(255),
    province_id INT(11),
    province_retired TINYINT(1)
) ENGINE=INNODB;

-- CREATE INDEXES FOR PATIENT DATA MATERIALIZED VIEW
CREATE INDEX patient_id_idx ON patient_data_matvw(patient_id);
CREATE INDEX identifier_date_created_idx ON patient_data_matvw(identifier_date_created);
CREATE INDEX facility_name_idx ON patient_data_matvw(facility_name);
CREATE INDEX district_name_idx ON patient_data_matvw(district_name);
CREATE INDEX province_name_idx ON patient_data_matvw(province_name);
CREATE INDEX age_idx ON patient_data_matvw(age);
CREATE INDEX age_range_idx ON patient_data_matvw(age_range);

-- CREATE LOCATION DATA VIEW
CREATE OR REPLACE VIEW location_data_vw AS SELECT facility_name,facility_id,facility_dhis_ou_id, facility_retired,district_name, district_id,district_retired,province_name,province_id,province_retired from (SELECT location.location_id as facility_id,location.name as facility_name,parent_location, location.retired as facility_retired  FROM location join location_tag_map on location.location_id = location_tag_map.location_id  join location_tag on location_tag.location_tag_id = location_tag_map.location_tag_id WHERE location_tag.location_tag_id = 5) as facility JOIN (SELECT location.location_id as district_id,location.name as district_name,location.retired as district_retired,parent_location FROM location join location_tag_map on location.location_id = location_tag_map.location_id  join location_tag on location_tag.location_tag_id = location_tag_map.location_tag_id WHERE location_tag.location_tag_id = 6) as district on facility.parent_location = district_id JOIN (SELECT location.location_id as province_id,location.name as province_name, location.retired as province_retired FROM location join location_tag_map on location.location_id = location_tag_map.location_id  join location_tag on location_tag.location_tag_id = location_tag_map.location_tag_id WHERE location_tag.location_tag_id = 7) as province on district.parent_location = province_id LEFT JOIN (SELECT location.location_id, value_reference as facility_dhis_ou_id from location join location_attribute on location.location_id = location_attribute.location_id WHERE attribute_type_id = 2 AND location_attribute.voided = 0) as dhis_ou on facility_id = dhis_ou.location_id;

-- CREATE LOCATION DATA MATERIALIZED VIEW
CREATE TABLE IF NOT EXISTS location_data_matvw (
    facility_name VARCHAR(255),
    facility_id INT(11),
    facility_dhis_ou_id VARCHAR(255),
    facility_retired TINYINT(1),
    district_name VARCHAR(255),
    district_id INT(11),
    district_retired TINYINT(1),
    province_name VARCHAR(255),
    province_id INT(11),
    province_retired TINYINT(1)
) ENGINE=INNODB;

-- CREATE INDEXES FOR LOCATION DATA MATERIALIZED VIEW
CREATE INDEX facility_name_idx ON location_data_matvw(facility_name);
CREATE INDEX district_name_idx ON location_data_matvw(district_name);
CREATE INDEX province_name_idx ON location_data_matvw(province_name);
CREATE INDEX facility_retired_idx ON location_data_matvw(facility_retired);
CREATE INDEX district_retired_idx ON location_data_matvw(district_retired);
CREATE INDEX province_retired_idx ON location_data_matvw(province_retired);

-- CREATE A JOINT PATIENT AND VISIT VIEW
CREATE OR REPLACE VIEW patient_visit_data_vw AS SELECT 
    visit_data_matvw.patient_id,
    identifier,
    identifier_type_id,
    identifier_type_name,
    identifier_location_id,
    identifier_location_name,
    identifier_date_created,
    identifier_location_retired,
    birthdate,
    age,
    age_range,
    gender,
    patient_voided,
    dead,
    visit_id,
    visit_type_id,
    visit_type_name,
    date_started,
    date_stopped,
    visit_location_id,
    visit_location_name,
    visit_location_retired,
    visit_voided,
    encounter_id,
    encounter_type_id,
    encounter_type_name,
    encounter_location_id,
    encounter_location_name,
    encounter_datetime,
    encounter_provider_id,
    encounter_provider_person_id,
    encounter_provider_name,
    encounter_provider_retired,
    obs_id,
    obs_concept_id,
    obs_concept_name_en,
    obs_datetime,
    obs_location_id,
    obs_location_name,
    obs_value,
    obs_value_concept_id,
    visit_data_matvw.facility_name,
    visit_data_matvw.facility_id,
    visit_data_matvw.facility_retired,
    visit_data_matvw.district_name,
    visit_data_matvw.district_id,
    visit_data_matvw.district_retired,
    visit_data_matvw.province_name,
    visit_data_matvw.province_id,
    visit_data_matvw.province_retired
 FROM patient_data_matvw JOIN visit_data_matvw ON patient_data_matvw.patient_id = visit_data_matvw.patient_id;

-- CREATE AGE RANGE TABLE
CREATE TABLE age_range(age_range_id INT AUTO_INCREMENT PRIMARY KEY ,age_range VARCHAR(6));

-- INSERT CONSTANT AGE RANGES
INSERT INTO age_range(age_range)VALUES ('<9'),('10-14'),('15-19'),('20-24'),('25-29'),('30-34'),('35-39'),('40-44'),('45-49'),('50+');

-- CREATE PROCEDURE FOR REFRESHING MATERIALIZED VIEWS
DELIMITER $$
CREATE PROCEDURE RefreshMaterializedViews()
    BEGIN
        TRUNCATE TABLE visit_data_matvw;
        TRUNCATE TABLE patient_data_matvw;
	TRUNCATE TABLE location_data_matvw;
        INSERT INTO location_data_matvw (SELECT * FROM location_data_vw);
        INSERT INTO visit_data_matvw(SELECT * FROM visit_data_vw join location_data_vw on visit_data_vw.visit_location_id = location_data_vw.facility_id );
        INSERT INTO patient_data_matvw (patient_id,identifier,identifier_type_id,identifier_type_name,identifier_location_id,identifier_location_name,identifier_date_created,identifier_location_retired,birthdate,age,gender,patient_voided,dead,facility_name,facility_id,facility_dhis_ou_id,facility_retired,district_name,district_id,district_retired,province_name,province_id,province_retired) SELECT * FROM patient_data_vw join location_data_vw on patient_data_vw.identifier_location_id = location_data_vw.facility_id;
    END;

CREATE FUNCTION ViralLoadRange(
    viralLoad INT
) 
RETURNS VARCHAR(50)
DETERMINISTIC
BEGIN
    DECLARE viralLoadRange VARCHAR(50);
 
    IF viralLoad >= 1000 THEN
        SET viralLoadRange = 'Viral Load 1000 and above';
    ELSEIF viralLoad BETWEEN 1 AND 999 THEN
        SET viralLoadRange = 'Viral load below 1000';
    ELSEIF viralLoad = 0 THEN
        SET viralLoadRange = 'Viral load is 0';
    END IF;
    RETURN viralLoadRange;
END;

CREATE TRIGGER age_range BEFORE INSERT ON patient_data_matvw
FOR EACH ROW
BEGIN
    IF NEW.age BETWEEN 0 AND 9 THEN
        SET NEW.age_range = '<9';
    ELSEIF NEW.age BETWEEN 10 AND 14 THEN
        SET NEW.age_range = '10-14';
    ELSEIF NEW.age BETWEEN 15 AND 19 THEN
        SET NEW.age_range = '15-19';
    ELSEIF NEW.age BETWEEN 20 AND 24 THEN
        SET NEW.age_range = '20-24';
    ELSEIF NEW.age BETWEEN 25 AND 29 THEN
        SET NEW.age_range = '25-29';
    ELSEIF NEW.age BETWEEN 30 AND 34 THEN
        SET NEW.age_range = '30-34';
    ELSEIF NEW.age BETWEEN 35 AND 39 THEN
        SET NEW.age_range = '35-39';
    ELSEIF NEW.age BETWEEN 40 AND 44 THEN
        SET NEW.age_range = '40-44';
    ELSEIF NEW.age BETWEEN 45 AND 49 THEN
        SET NEW.age_range = '45-49';
    ELSEIF NEW.age >= 50 THEN
        SET NEW.age_range = '50+';
    END IF;
END;
$$

GRANT EXECUTE ON PROCEDURE openmrs.RefreshMaterializedViews TO 'smartcerv'@'%';
GRANT EXECUTE ON FUNCTION openmrs.ViralLoadRange TO 'smartcerv'@'%';
-- REFRESH MATERIALIZED VIEWS
CALL RefreshMaterializedViews();
