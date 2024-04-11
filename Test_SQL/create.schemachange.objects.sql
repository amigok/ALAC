DROP TABLE SCHEMACHANGE.CHANGE_HISTORY;
DROP TABLE SCHEMACHANGE.CHANGE_HISTORY_DEV;
DROP SCHEMA SCHEMACHANGE;

CREATE DATABASE METADATA;

GRANT USAGE ON DATABASE METADATA TO ROLE SCHEMACHANGE_DATA;
GRANT USAGE ON DATABASE METADATA TO ROLE SCHEMACHANGE_TEAM1;
GRANT USAGE ON DATABASE METADATA TO ROLE SCHEMACHANGE_TEAM2;
GRANT USAGE ON DATABASE METADATA TO ROLE SCHEMACHANGE_DEV;

CREATE SCHEMA METADATA.SCHEMACHANGE;

GRANT USAGE ON SCHEMA METADATA.SCHEMACHANGE TO ROLE SCHEMACHANGE_DATA;
GRANT USAGE ON SCHEMA METADATA.SCHEMACHANGE TO ROLE SCHEMACHANGE_TEAM1;
GRANT USAGE ON SCHEMA METADATA.SCHEMACHANGE TO ROLE SCHEMACHANGE_TEAM2;
GRANT USAGE ON SCHEMA METADATA.SCHEMACHANGE TO ROLE SCHEMACHANGE_DEV;

CREATE TABLE IF NOT EXISTS METADATA.SCHEMACHANGE.CHANGE_HISTORY
(
    VERSION VARCHAR
   ,DESCRIPTION VARCHAR
   ,SCRIPT VARCHAR
   ,SCRIPT_TYPE VARCHAR
   ,CHECKSUM VARCHAR
   ,EXECUTION_TIME NUMBER
   ,STATUS VARCHAR
   ,INSTALLED_BY VARCHAR
   ,INSTALLED_ON TIMESTAMP_LTZ
);

CREATE TABLE IF NOT EXISTS METADATA.SCHEMACHANGE.CHANGE_HISTORY_DEV
(
    VERSION VARCHAR
   ,DESCRIPTION VARCHAR
   ,SCRIPT VARCHAR
   ,SCRIPT_TYPE VARCHAR
   ,CHECKSUM VARCHAR
   ,EXECUTION_TIME NUMBER
   ,STATUS VARCHAR
   ,INSTALLED_BY VARCHAR
   ,INSTALLED_ON TIMESTAMP_LTZ
);

GRANT SELECT, INSERT, DELETE, UPDATE, TRUNCATE, REFERENCES ON TABLE METADATA.SCHEMACHANGE.CHANGE_HISTORY TO ROLE SCHEMACHANGE_DATA;
GRANT SELECT, INSERT, DELETE, UPDATE, TRUNCATE, REFERENCES ON TABLE METADATA.SCHEMACHANGE.CHANGE_HISTORY TO ROLE SCHEMACHANGE_TEAM1;
GRANT SELECT, INSERT, DELETE, UPDATE, TRUNCATE, REFERENCES ON TABLE METADATA.SCHEMACHANGE.CHANGE_HISTORY TO ROLE SCHEMACHANGE_TEAM2;
GRANT SELECT, INSERT, DELETE, UPDATE, TRUNCATE, REFERENCES ON TABLE METADATA.SCHEMACHANGE.CHANGE_HISTORY TO ROLE SCHEMACHANGE_DEV;

GRANT SELECT, INSERT, DELETE, UPDATE, TRUNCATE, REFERENCES ON TABLE METADATA.SCHEMACHANGE.CHANGE_HISTORY_DEV TO ROLE SCHEMACHANGE_DEV;