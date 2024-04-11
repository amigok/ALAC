drop database SCHEMACHANGE_POC_ANALYTICS;
drop database SCHEMACHANGE_POC_ANALYTICS_DEV;

create database SCHEMACHANGE_POC_ANALYTICS comment='Schemachange POC Database';
create database SCHEMACHANGE_POC_ANALYTICS_DEV comment='Schemachange POC Database';

use database SCHEMACHANGE_POC_ANALYTICS;
create schema DATA comment='Schemachange POC DATA schema';
create schema TEAM1 comment='Schemachange POC TEAM1 schema';
create schema TEAM2 comment='Schemachange POC TEAM2 schema';

use database SCHEMACHANGE_POC_ANALYTICS_DEV;
create schema DATA comment='Schemachange POC DATA schema';
create schema TEAM1 comment='Schemachange POC TEAM1 schema';
create schema TEAM2 comment='Schemachange POC TEAM2 schema';
