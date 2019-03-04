-- creating FOREIGN TABLES to get data from openmrs for location_attribute,location_attribute_type, and location

-- Create Tables
CREATE FOREIGN TABLE location (
location_id INTEGER,
NAME VARCHAR (255) DEFAULT '',
description VARCHAR (255),
address1 VARCHAR (255),
address2 VARCHAR (255),
city_village VARCHAR (255),
state_province VARCHAR (255),
postal_code VARCHAR (50),
country VARCHAR (50),
latitude VARCHAR (50),
longitude VARCHAR (50),
creator INTEGER,
date_created VARCHAR (50),
county_district VARCHAR (255),
address3 VARCHAR (255),
address4 VARCHAR (255),
address5 VARCHAR (255),
address6 VARCHAR (255),
retired SMALLINT,
retired_by INTEGER,
date_retired VARCHAR (50),
retire_reason VARCHAR (255),
parent_location INTEGER,
uuid CHAR (38),
changed_by INTEGER,
date_changed VARCHAR (50))
SERVER mysql_server
OPTIONS (dbname 'openmrs', TABLE_NAME 'location');

CREATE FOREIGN TABLE location_attribute_type (
location_attribute_type_id INTEGER,
name VARCHAR (255),
description  VARCHAR (1024),
datatype VARCHAR (255),
datatype_config TEXT,
preferred_handler VARCHAR (255),
handler_config TEXT,
min_occurs INTEGER,
max_occurs INTEGER,
creator  INTEGER,
date_created VARCHAR (50),
date_changed VARCHAR (50),
retired INT2,
retired_by INTEGER,
date_retired VARCHAR (50),
retire_reason VARCHAR (255))
SERVER mysql_server
OPTIONS (dbname 'openmrs', TABLE_NAME 'location_attribute_type');

CREATE FOREIGN TABLE location_attribute (
location_attribute_id INTEGER,
location_id INTEGER,
attribute_type_id INTEGER,
value_reference TEXT,
uuid UUID,
creator INTEGER,
date_created VARCHAR (50),
changed_by INTEGER,
date_changed VARCHAR (50),
voided INT2,
voided_by INTEGER,
date_voided VARCHAR (50),
void_reason TEXT)
SERVER mysql_server
OPTIONS (dbname 'openmrs', TABLE_NAME 'location_attribute');

-- Create view for mapping openmrs location uuid to dhis organisation unit id

CREATE OR REPLACE VIEW openmrs_dhis_location_map 
AS SELECT location.uuid AS location_uuid,
 value_reference AS dhis_orgunit_id
  FROM location JOIN 
  location_attribute 
  ON location.location_id = location_attribute.location_id;