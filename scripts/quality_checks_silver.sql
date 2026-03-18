/*
Quality Checks -

This script performs quality checks for bronze layer datasets before inserting the data into silver layer. 
It includes:
- Nulls or duplicate primary keys
- Unwanted spaces
- Data standarization and Consistency
- Invalid data ranges and orders
*/

--EXAMINING THE DATA FROM BRONZE LAYER
--Working on crm_cust_info

--CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY
SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY 1
HAVING COUNT(*) >1;

-- After testing we have found some duplicates, now it's time to decide which one of them to pick
SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info; */

-- Running the above query will give us the rank of id's in order of date so we can fetch the record with latest date wherever there are duplicates

--CHECK IF THERE ARE ANY WHITESPACES IN TEXT COLUMNS
SELECT cst_firstname
FROM  bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM  bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

--After testing we found there are spaces before or after the records in firstname and lastname column, so we can use TRIM while inserting the data

--CONVERTING THE ABBREVIATED VALUES FOR GENDER AND MARITAL STATUS COLUMN
SELECT
CASE 
WHEN cst_gndr = 'F' THEN 'Female'
WHEN cst_gndr = 'M' THEN 'Male'
ELSE 'n/a'
END cst_gndr
FROM bronze.crm_cust_info;

SELECT
CASE 
WHEN cst_marital_status = 'S' THEN 'Single'
WHEN cst_marital_status = 'M' THEN 'Married'
ELSE 'n/a'
END cst_marital_status
FROM bronze.crm_cust_info;

--Working on crm_prd_info
--CHECK FOR DUPLICATE PRIMARY KEYS
SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY 1
HAVING COUNT(*) >1;

--CHECK FOR WHITESPACES
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

--SPLITING prd_key COLUMN IN CATEGORY KEY AND PRD KEY
SELECT
REPLACE(SUBSTRING(prd_key, 1,5), '-', '_') AS cat_key,
SUBSTRING(prd_key, 7, LENGTH(prd_key)) as prd_key
FROM bronze.crm_prd_info;

--CHECK NEGATIVE OR NULLS IN COST
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL;

--UPDATE prd_line COLUMN TO REMOVE ABBREVIATION
SELECT
CASE 
WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
ELSE 'Unknown'
END prd_line
FROM bronze.crm_prd_info;

--UPDATE DATE COLUMNS
SELECT
DATE(prd_start_dt) as prd_start_dt,
DATE(prd_end_dt) as prd_end_dt
FROM
bronze.crm_prd_info;

--CHECK QUALITY OF DATE COLUMNS
SELECT *,
DATE(prd_start_dt) as prd_start_dt,
DATE(prd_end_dt) as prd_end_dt
FROM
bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

--The end_dt should not be less than start_dt

--Wherever we have this situation we will replace the end_date with the next start_date
SELECT *,
LEAD(DATE(prd_start_dt)) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
FROM bronze.crm_prd_info;

--Working on crm_sales_details

--CHECK IF DATE COLUMNS ARE CORRECT AND THERE ARE NO NEGATIVE ADTES
SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0;

SELECT 
NULLIF(sls_order_dt, 0) as sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0;

--CHECK NUMERIC COLUMNS
--Sales = Quantity * Price, if that's not the case we need to fix the data.  Also if the columns are negative, we need to update them.
SELECT sls_sales, sls_quantity, sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL 
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <=0;

--CONVERT COLUMNS INTO CORRECT VALUES
SELECT  sls_sales, sls_quantity, sls_price,
CASE
WHEN sls_sales IS NULL OR  sls_sales <= 0 OR  sls_sales != sls_quantity* ABS(sls_price)
THEN sls_quantity * sls_price
ELSE sls_sales
END sls_sales,
CASE
WHEN sls_price IS NULL OR  sls_price <= 0 
THEN ABS(sls_sales / NULLIF(sls_quantity,0))
ELSE sls_price
END sls_price
FROM bronze.crm_sales_details
ORDER BY sls_quantity DESC;

--Working on erp_cust_az12

--CHECK CUST ID COLUMN
SELECT cid FROM bronze.erp_cust_az12
WHERE cid NOT LIKE '%NAS%';

--PULL THE CORRECT CUST KEY FORM ID COLUMN
SELECT cid,
CASE WHEN
cid LIKE '%NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
ELSE cid
END
FROM bronze.erp_cust_az12;

--CHECK IF THERE IS A CUSTOMER WITH A FUTURE BIRTH DATE, IF YES REPLCAE IT WITH NULL
SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate > CURRENT_DATE;

SELECT bdate,
CASE WHEN bdate > CURRENT_DATE 
THEN NULL
ELSE bdate
END  bdate
FROM bronze.erp_cust_az12;

--CHECK GENDER COLUMN AND CONVERT IT FROM ABBREVIATED VALUES
SELECT DISTINCT(gen) FROM bronze.erp_cust_az12;

SELECT
CASE 
WHEN gen = 'M' THEN 'Male'
WHEN gen = 'F' THEN 'Female'
ELSE 'n/a'
END gen
FROM bronze.erp_cust_az12;

--Working on erp_loc_a101

--CHECK AND UPDATE CUST ID COLUMN
SELECT 
REPLACE(cid,'-','') AS cid
FROM bronze.erp_loc_a101;

--CHECK COUNTRY COLUMN AND UPDATE ABBREVIATED VALUES
SELECT DISTINCT(cntry)
FROM bronze.erp_loc_a101;

SELECT DISTINCT(cntry),
CASE WHEN cntry IN ('US','USA') THEN 'United States'
WHEN cntry = 'DE' THEN 'Germany'
WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'n/a'
ELSE cntry
END AS cntry
FROM bronze.erp_loc_a101;
