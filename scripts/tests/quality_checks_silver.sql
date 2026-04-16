*/ 
==================================================
Quality Checks
==================================================
Script Purpose:
  This script performs various quality checks for data consistency, accuracy, 
  and standarization across the 'silver' schema. It includes checks for:
  - Null or duplicate primary keys.
  - Unwanted spaces in string fields.
  - Data standardization and consistency.
  - Invalid date ranges and orders.
  - Data consistency between related fields.

Usage Notes:
  - Run these checks after data loading Silver Layer.
  - Investigate and resolve any discrepancies found during the checks.
=========================================================================
*/
  
--============================cust_info====================--
SELECT TOP 1000 * FROM bronze.crm_cust_info 

--=====

SELECT cst_id, COUNT(*) FROM bronze.crm_cust_info
GROUP BY cst_id 
HAVING COUNT(*) > 1 OR cst_id IS NULL

--======
SELECT cst_firstname FROM bronze.crm_cust_info
WHERE TRIM(cst_firstname) != cst_firstname

SELECT cst_lastname FROM bronze.crm_cust_info
WHERE TRIM(cst_lastname) != cst_lastname

---=====
PRINT '>> Truncating Table: silver.crm_cust_info';
TRUNCATE TABLE silver.crm_cust_info;
PRINT '>> Inserting Data Into: silver.crm_cust_info';
INSERT INTO silver.crm_cust_info (
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date

)
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS fistname,
TRIM(cst_lastname) AS lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	 ELSE 'n/a'
END AS cst_marital_status
,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE' 
	WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
	ELSE 'n/a'
END AS cst_gndr,
cst_create_date
FROM (
SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date) AS flag_last
FROM bronze.crm_cust_info 
WHERE cst_id IS NOT NULL 
)t WHERE flag_last = 1

--============================prd_info====================--

SELECT TOP 1000 * FROM bronze.crm_prd_info
--====
SELECT prd_id , COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL 
--=======

SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key,7,LEN(prd_key))  IN(
SELECT DISTINCT sls_prd_key FROM bronze.crm_sales_details )

--========
TRUNCATE TABLE silver.crm_prd_info
INSERT INTO silver.crm_prd_info (
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
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	 WHEN 'M' THEN 'Mountain'
	 WHEN 'R' THEN 'Road'
	 WHEN 'S' THEN 'Other Sales'
	 WHEN 'T' THEN 'Touring'
	 ELSE 'n/a'
END AS prd_line,
CAST (prd_start_dt AS DATE ) AS prd_start_dt,
CAST (
		LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1
		AS DATE
	) AS prd_end_dt
FROM bronze.crm_prd_info


--============================sales_details====================--
SELECT TOP 1000 * FROM bronze.crm_sales_details

SELECT sls_cust_id, COUNT(*)
FROM bronze.crm_sales_details 
GROUP BY sls_cust_id
HAVING COUNT(*)> 1 OR sls_cust_id IS NULL
--===============
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details 
WHERE sls_ord_num != TRIM(sls_ord_num)

--===============

SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details 
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)
--===============
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details 
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)
--=====================
--==Check for Invalid Dates
SELECT
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <=0 OR LEN(sls_order_dt) != 8

SELECT
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 8 
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101


--============
--============Check sls_ship_dt column=============
SELECT
NULLIF(sls_ship_dt,0) sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0
OR LEN(sls_ship_dt) != 8 
OR sls_ship_dt > 20500101
OR sls_ship_dt < 19000101

--======================
--=============Check due date

SELECT
NULLIF(sls_due_dt,0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
OR LEN(sls_due_dt) != 8 
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101

--================
--== check for invalid check order
SELECT * FROM bronze.crm_sales_details 
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

--==============
--== Check Data Consistency: Between Sales, Quantity, and Price
-->> Sales = Quantity * Price 
-->> Values must not be NUll, zero, or negative.

SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,

CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <= 0 
		THEN sls_sales / NULLIF(sls_quantity,0)
	ELSE sls_price 
END AS price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <=0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price



--===============
INSERT INTO silver.crm_sales_details (
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt, 
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
)
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
END	AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE)
END	AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE)
END	AS sls_due_dt,

CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,

sls_quantity,

CASE WHEN sls_price IS NULL OR sls_price <= 0 
		THEN sls_sales / NULLIF(sls_quantity,0)
	ELSE sls_price 
END AS sls_price
FROM bronze.crm_sales_details 

--=======================erp_cust_az12================

SELECT TOP 1000 * FROM bronze.erp_cust_az12
--================
SELECT 
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4 , LEN(cid))
	ELSE cid
END AS cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4 , LEN(cid))
	ELSE cid
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info )


--===============
-- Identify Out-of-Range Dates 
SELECT DISTINCT bdate FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

CASE WHEN bdate > GETDATE() THEN NULL
	 ELSE bdate
END AS bdate
--=========
-- Data Standardization & Consistency
SELECT DISTINCT gen
FROM bronze.erp_cust_az12

CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen

SELECT DISTINCT
gen, 
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen 
FROM bronze.erp_cust_az12


--================
INSERT INTO silver.erp_cust_az12 (
cid,
bdate,
gen

)
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4 , LEN(cid))
	ELSE cid
END AS cid,
CASE WHEN bdate > GETDATE() THEN NULL
	 ELSE bdate
END AS bdate,

CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12

--==========================erp_loc_a101===========
SELECT TOP 1000 * FROM bronze.erp_loc_a101

SELECT
REPLACE(cid, '-', '') AS cid,
cntry
FROM bronze.erp_loc_a101 WHERE REPLACE(cid, '-', '') NOT IN ( 
SELECT cst_key FROM silver.crm_cust_info)
--======
-- Date Standradization & Consistency 
SELECT DISTINCT cntry FROM bronze.erp_loc_a101

SELECT DISTINCT
cntry AS old_cntry
,
CASE WHEN UPPER(TRIM(cntry)) IN ('GERMANY','DE') THEN 'Germany'
	 WHEN UPPER(TRIM(cntry)) IN ('UNITED STATES','USA','US') THEN 'United States'
	 WHEN UPPER(TRIM(cntry)) IN ('AUSTRALIA','AU') THEN 'Australia'
	 WHEN UPPER(TRIM(cntry)) IN ('UNITED KINGDOM','UK') THEN 'United Kingdom'
	 WHEN UPPER(TRIM(cntry)) IN ('CANADA','CA') THEN 'Canada'
	 WHEN UPPER(TRIM(cntry)) IN ('FRANCE') THEN 'France'
	 ELSE 'n/a'
END AS cntry 
FROM bronze.erp_loc_a101 
ORDER BY cntry

--==============
INSERT INTO silver.erp_loc_a101(
cid,
cntry
)
SELECT
REPLACE(cid, '-', '') AS cid,

CASE WHEN UPPER(TRIM(cntry)) IN ('GERMANY','DE') THEN 'Germany'
	 WHEN UPPER(TRIM(cntry)) IN ('UNITED STATES','USA','US') THEN 'United States'
	 WHEN UPPER(TRIM(cntry)) IN ('AUSTRALIA','AU') THEN 'Australia'
	 WHEN UPPER(TRIM(cntry)) IN ('UNITED KINGDOM','UK') THEN 'United Kingdom'
	 WHEN UPPER(TRIM(cntry)) IN ('CANADA','CA') THEN 'Canada'
	 WHEN UPPER(TRIM(cntry)) IN ('FRANCE') THEN 'France'
	 ELSE 'n/a'
END AS cntry 
FROM bronze.erp_loc_a101

SELECT * FROM silver.erp_loc_a101


--===================erp_px_cat_g1v2=============

SELECT TOP 1000 * FROM bronze.erp_px_cat_g1v2
--==== 
-- Check for Unwanted Space
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) 
OR maintenance != TRIM(maintenance)
-- Date Standradization & Consistency 
SELECT DISTINCT 
cat 
FROM bronze.erp_px_cat_g1v2
--===
SELECT DISTINCT 
subcat 
FROM bronze.erp_px_cat_g1v2
--==
SELECT DISTINCT 
maintenance 
FROM bronze.erp_px_cat_g1v2
--======

INSERT INTO silver.erp_px_cat_g1v2(
id,
cat,
subcat,
maintenance

)
SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2 

SELECT * FROM silver.erp_px_cat_g1v2
