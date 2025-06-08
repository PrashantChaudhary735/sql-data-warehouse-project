/*
================================================
DDL Script: Create Bronze Tables
================================================
Script Purpose: 
This script creates tables in the 'bronze' schema, dropping existing tables if they already exists.
Run this script to re-defined the DDL Structure of 'bronze' tables
*/


-- Creating table to load the customer data
-- Naming convention should be proper as per the rules of the bronze layer:

-- This IF condition is used to make this below mentioned queries rerunnable, 
-- It checks whether the table exists or not in the database, if it does exists,
-- we are dropping the table and then again creating it.

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info
CREATE TABLE bronze.crm_cust_info
(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_material_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE
);


IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info
CREATE TABLE bronze.crm_prd_info
(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
)


-- To rename the name of any table in MS SQL 
--sp_rename 'bronze.prd_info', 'bronze.crm_prd_info';

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details
CREATE TABLE bronze.crm_sales_details
(
sls_ord_num NVARCHAR(20),
sls_prd_key NVARCHAR(50),
sl_cust_id INT,
sls_order_dt  INT,
sls_ship_dt INT,
sls_due_dt INT,
sl_Sales INT,
sls_quantity INT,
sls_price INT);

-- Creating table of ERP system

IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101
CREATE TABLE bronze.erp_loc_a101
(
cid NVARCHAR(50),
cntry NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12
CREATE TABLE bronze.erp_cust_az12
(
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2
CREATE TABLE bronze.erp_px_cat_g1v2
(
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50));


 









