/*
---------------------------------
Create Database and Schemas
---------------------------------

Purpose:
This script creates a database called DataWarehouse after first checking whether it already exists.
If the database is found, it will be dropped and recreated. The script also creates three schemas inside the database: bronze, silver, and gold.

Warning:
Executing this script will completely remove the existing DataWarehouse database.
All stored data will be permanently deleted. Make sure appropriate backups are taken before running this script.

*/

