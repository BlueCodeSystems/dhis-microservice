-- creating FOREIGN TABLES to get data from openmrs for concepts, users and location this tables

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

CREATE FOREIGN TABLE location_attribute_type_id (
location_attribute_type_id INT(11) PRIMARY KEY,
name VARCHAR (255),
description  VARCHAR (1024),
datatype VARCHAR (255),
datatype_config TEXT,
preferred_handler VARCHAR (255),
handler_config TEXT,
min_occurs INT(11),
max_occurs INT(11),
creator  INT(11),
date_created VARCHAR (50),
date_changed VARCHAR (50),
retired INT2,
retired_by INT(11),
date_retired VARCHAR (50),
retire_reason VARCHAR (255))
SERVER mysql_server
OPTIONS (dbname 'openmrs', TABLE_NAME 'location_attribute_type_id');

CREATE FOREIGN TABLE location_attribute (
location_attribute_id INT(11) PRIMARY KEY,
location_id INT(11),
attribute_type_id INT(11),
value_reference TEXT,
uuid UUID,
creator INT(11),
date_created VARCHAR (50),
changed_by INT(11),
date_changed VARCHAR (50),
voided INT2,
voided_by INT(11),
date_voided VARCHAR (50),
void_reason TEXT)
SERVER mysql_server
OPTIONS (dbname 'openmrs', TABLE_NAME 'location_attribute');

-- Create indexes
CREATE INDEX idx_location_attribute_location_id
ON location_attribute(location_id);

CREATE UNIQUE INDEX idx_location_uuid
ON location(uuid);

CREATE UNIQUE INDEX idx_location_id
ON location(location_id);

CREATE INDEX idx_location_name
ON location(name);