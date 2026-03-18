--INSERT INTO silver.crm_cust_info after all the data cleaning

INSERT INTO silver.crm_cust_info(
cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status,cst_gndr, cst_create_date
)

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname, --Removing unwanted characters to maintain consistency
TRIM(cst_lastname) as cst_lastname,
CASE 
WHEN cst_marital_status = 'S' THEN 'Single'
WHEN cst_marital_status = 'M' THEN 'Married'
ELSE 'n/a'
END cst_marital_status, -- Data normalization/ standarization
CASE 
WHEN cst_gndr = 'F' THEN 'Female'
WHEN cst_gndr = 'M' THEN 'Male'
ELSE 'n/a' --Handling nulls
END cst_gndr,
cst_create_date
FROM
(
SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL 
)t
WHERE flag_last = 1; --Removing duplicates

--INSERT INTO silver.crm_prd_info after all the data cleaning

INSERT INTO silver.crm_prd_info(
prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
)

SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key, 1,5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LENGTH(prd_key)) as prd_key,
TRIM(prd_nm) AS prd_nm,
COALESCE(prd_cost, 0) AS prd_cost,
CASE 
WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
ELSE 'Unknown'
END prd_line,
DATE(prd_start_dt) AS prd_start_dt,
CASE
WHEN prd_end_dt < prd_start_dt
THEN LEAD(DATE(prd_start_dt)) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1
END prd_end_dt
FROM bronze.crm_prd_info
;

--INSERT INTO silver.crm_csales_details after all the data cleaning

INSERT INTO silver.crm_sales_details(
sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt,
sls_sales, sls_quantity, sls_price
)
SELECT
	sls_ord_num  ,
    sls_prd_key ,
    sls_cust_id  ,
    CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) !=8 THEN NULL
	ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
	END sls_order_date,
    CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) !=8 THEN NULL
	ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
	END sls_ship_date,
    CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) !=8 THEN NULL
	ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
	END sls_due_date, 
    CASE
	WHEN sls_sales IS NULL OR  sls_sales <= 0 OR  sls_sales != sls_quantity* ABS(sls_price)
	THEN sls_quantity * sls_price
	ELSE sls_sales
	END sls_sales,
	sls_quantity,
	CASE
	WHEN sls_price IS NULL OR  sls_price <= 0 
	THEN ABS(sls_sales / NULLIF(sls_quantity,0))
	ELSE sls_price
	END sls_price
FROM bronze.crm_sales_details;

--INSERT INTO silver.erp_cust_az12 after all the data cleaning

INSERT INTO silver.erp_cust_az12(
cid,bdate,gen
)

SELECT
CASE WHEN
cid LIKE '%NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
ELSE cid
END AS cid,
CASE WHEN bdate > CURRENT_DATE 
THEN NULL
ELSE bdate
END AS bdate,
CASE 
WHEN gen = 'M' THEN 'Male'
WHEN gen = 'F' THEN 'Female'
ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;

--INSERT INTO silver.erp_loc_a101 after all the data cleaning

INSERT INTO silver.erp_loc_a101(
cid,cntry
)
SELECT
REPLACE(cid,'-','') AS cid,
CASE WHEN cntry IN ('US','USA') THEN 'United States'
WHEN cntry = 'DE' THEN 'Germany'
WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'n/a'
ELSE cntry
END AS cntry
FROM bronze.erp_loc_a101;

--INSERT INTO silver.erp_px_cat_g1v2 after all the data cleaning

INSERT INTO silver.erp_px_cat_g1v2(
id, cat, subcat, maintenance
)
SELECT 
id,
cat,
subcat,
maintenance FROM bronze.erp_px_cat_g1v2;
