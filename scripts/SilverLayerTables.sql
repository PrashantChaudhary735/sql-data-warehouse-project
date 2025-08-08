use DataWarehouse; 

-- MetaData Columns: Extra columns added by data engineer

use DataWarehouse; 

SELECT * FROM silver.crm_cust_info; 
SELECT * FROm silver.crm_prd_info; 

-- Quality Check: 1 
-- Check for nulls or duplicates in primary key
-- Expectation: No Result
SELECT 
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
having COUNT(*) > 1 OR cst_id IS NULL; 

SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
having COUNT(*) > 1 OR cst_id IS NULL; 

-- There are multiple records present for the above mentioned query and we need to clean it first then only we can add the cst_id as Primary key column. 
-- Data cleansing for the primary key column
SELECT
* 
FROM bronze.crm_cust_info
WHERE cst_id = '29466'

-- Ranking the values based on the cst_create_date function

INSERT INTO silver.crm_cust_info (
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_gndr,
cst_material_status,
cst_create_date,

)
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_gndr,
cst_material_status,
cst_create_date
FROM #temp1;
SELECT * FROM #temp1; 



SELECT * FROM silver.crm_cust_info  
TRUNCATE TABLE silver.crm_cust_info;

WITH Table1 
AS
(
SELECT
*,
ROW_NUMBER() OVER( PARTITION BY cst_id ORDER BY cst_create_date DESC) as rnk
FROM bronze.crm_cust_info
) 

SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'NA'
END cst_gndr,
CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
	 WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
	 ELSE 'NA'
END cst_material_status,
cst_create_date INTO #temp1
FROM Table1
WHERE rnk = 1

use DataWarehouse; 

DROP table #temp1; 



-- Quality Check: 2
-- Check for unwanted spaces in string values
-- Expectation: No results
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) ;

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- Quality Check: 3
-- Data standardization and consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info; 

/*
=========================================================
Working on crm_prd_info table;
=========================================================
*/


-- Replacing the '-' with '_' in the crm_prd_info table (Column: prd_key)
-- ALso need to 

use DataWarehouse; 

INSERT INTO silver.crm_prd_info
(
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)
SELECT
prd_id,
REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
SUBSTRING(prd_key,7, LEN(prd_key)) as prd_key,
prd_nm,
ISNULL(prd_cost,0) as prd_cost,
CASE WHEN UPPER(TRIM(prd_line))= 'M' THEN 'Mountain'
	 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
	 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
	 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
	 ELSE 'NA'
END as prd_line,
CAST(prd_start_dt AS date) as prd_start_dt,
CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt asc)- 1 as date) as prd_end_dt
FROM BRONZE.crm_prd_info

SELECT * FROM bronze.crm_prd_info;

SELECT 
UPPER(TRIM('prd_line')) 
FROM bronze.crm_prd_info;




-- Quality Check 
--Query to check unwanted spaces 

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm) ;

-- Check for Nulls or negative numbers
-- Expectation: No results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data standardization and consitency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info; 


-- Check for Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt; 

-- Derive the end date from the start date.
-- End date = Start date of the next record
use Datawarehouse; 

SELECT
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt asc)- 1 as prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509'); 


/*
============================================
Building silver layer:
Clean & load: crm_sales_details table
============================================
*/

-- Checking whether all product keys are present or not in both the product and sales details table.
SELECT * FRom 
bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key from silver.crm_prd_info);

-- Check for invalid dates: 
SELECT 
* FROM 
bronze.crm_sales_details
WHERE sls_order_dt <= 0;

SELECT CAST(CONVERT(NVarchar(50),sls_order_dt) as Date)
FROM bronze.crm_sales_details; 

SELECT
NULLIF(sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0  
OR LEN(sls_order_dt) != 8 
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101; 

--
SELECT * 
FROM bronze.crm_sales_details; 

SELECT
sls_ord_num,
sls_prd_key,
sl_cust_id,
Case WHEN sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL
	 ELSE CAST(CAST(sls_order_dt as VARCHAR) AS DATE)
END AS sls_order_dt,
Case WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL
	 ELSE CAST(CAST(sls_ship_dt as VARCHAR) AS DATE)
END AS sls_ship_dt,
Case WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_due_dt as Varchar) AS DATE)
END AS sls_due_dt,
--sls_ship_dt,
--sls_due_dt,
sl_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details

-- Check for invalid date orders: 
SELECT
*
FROM
bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

SELECT * 
FROM bronze.crm_sales_details

-- Check Data Consistency: Between Sales, Quantity and Price
-- >> Sales = Quantity * Price
-- >> Values must not ne Null, zero or negative.
SELECT 
sl_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sl_Sales != sls_quantity * sls_price
OR sl_Sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sl_Sales <= 0 OR sls_quantity <= 0 or sls_price <= 0
ORDER BY sl_Sales, sls_quantity, sls_price; 

SELECT * FROM bronze.crm_sales_details

-- Need to update the values as per the below mentioned logic: 
-- If sales is negative, zero or null derive it using Quantity and Price
-- If price is zero or nul, calculate it using sales and quantity
-- If price is negative, convert it to a positive value.

SELECT *
FROM bronze.crm_sales_details; 



SELECT
sls_ord_num,
sls_prd_key,
sl_cust_id,
Case WHEN sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL
	 ELSE CAST(CAST(sls_order_dt as VARCHAR) AS DATE)
END AS sls_order_dt,
Case WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL
	 ELSE CAST(CAST(sls_ship_dt as VARCHAR) AS DATE)
END AS sls_ship_dt,
Case WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_due_dt as Varchar) AS DATE)
END AS sls_due_dt,
CASE WHEN sl_sales IS NULL OR sl_Sales <= 0 or sl_sales != sls_quantity * ABS(sls_price)
	 THEN sls_quantity * ABS(sls_price)
	 ELSE sl_sales
END as sls_sales,

CASE WHEN sls_price IS NULL or sls_price <= 0
	 THEN sl_Sales / NULLIF(sls_quantity,0)
	 ELSE sls_price
END as sls_price,
sls_quantity,
sls_price
FROM bronze.crm_sales_details


--Building silver layer, cleaning and loading 
--erp_cust_az12

use DataWarehouse; 

SELECT * FROM bronze.erp_cust_az12 where cs;

Select COUNT(*) From bronze.erp_cust_az12
where SUBSTRING(cid,4,LEN(cid)) IN (SELECT cst_key FROM silver.crm_cust_info)

SELECT SUBSTRING(cid,4,LEN(cid)) FROM bronze.erp_cust_az12; 

-- Below is the main query for cleaning process 

INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
SELECT 
CASE WHEN cid like 'NAS%' THEN SUBSTRING(cid,4, LEN(cid))
	 ELSE cid
END cid, -- Removed 'NAS' perfix if present anywhere to match the value with the customer table
Case When bdate > GETDATE() THEN NULL
ELSE bdate
END as bdate, -- Handled future birthdates
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'FEMALE'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END as gen -- Handled different values of gender present in the bronze tables
FROM bronze.erp_cust_az12;


-- Identifying out-of-range birth dates

SELECT
DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()


SELECT DISTINCT gen
FROM bronze.erp_cust_az12

SELECT * FRom silver.erp_cust_az12;


-- Building silver layer for erp_loc_a101

SELECT 
* 
FROM bronze.erp_loc_a101;


-- Main silver query for table erp_loc_a101
INSERT INTO silver.erp_loc_a101
(cid, cntry)
SELECT
REPLACE(cid, '-', '') cid,
CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
	 WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
	 WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END AS cntry
FROm bronze.erp_loc_a101

SELECT * FROm silver.erp_loc_a101
-- Checking standardization and consistency

SELECT 
DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER By cntry;


-- Building silver layer for table erp_px_cat_g1v2

INSERT INTO silver.erp_px_cat_g1v2
(id,cat,subcat,maintenance)
SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2


TRUNCate table silver.erp_px_cat_g1v2

SELECT COUNT(*) FROm silver.erp_px_cat_g1v2; 


