/*
=================================================================
Create Database and Schemas
=================================================================
Script Purpose:
    This script creates the schemas for the 'data_warehouse'
    database. It sets up three schemas within the database:
    'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will drop and recreate all three schemas.
    Any existing data in these schemas will be permanently deleted.
    Proceed with caution.
=================================================================
*/

-- Create the three layer schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
