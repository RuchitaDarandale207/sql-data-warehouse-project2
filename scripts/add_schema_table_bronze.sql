/*

This script helps to create tables in bronze schema.

In bronze schema the data is pulled from 2 sources crm and erp and each source has 3 tables.

Tables in crm are 
- cust_info
- prd_info
- sales_details

Tables in erp are
- cust_a1z2
- loc_a101
- px_cat_g1v2

Once the tables are create, the data is pulled into the tables
*/

-- Create tables

CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
	);

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);

CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);

-- Import data into tables

COPY bronze.crm_prd_info 
FROM 'D:/Work/Sample Datasets/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
DELIMITER ','
CSV HEADER;

COPY bronze.crm_cust_info 
FROM 'D:/Work/Sample Datasets/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
DELIMITER ','
CSV HEADER;

COPY bronze.crm_sales_details
FROM 'D:/Work/Sample Datasets/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
DELIMITER ','
CSV HEADER;

COPY bronze.erp_cust_az12
FROM 'D:/Work/Sample Datasets/sql-data-warehouse-project/datasets/source_erp/cust_az12.csv'
DELIMITER ','
CSV HEADER;

COPY bronze.erp_px_cat_g1v2
FROM 'D:/Work/Sample Datasets/sql-data-warehouse-project/datasets/source_erp/px_cat_g1v2.csv'
DELIMITER ','
CSV HEADER;

COPY bronze.erp_loc_a101
FROM 'D:/Work/Sample Datasets/sql-data-warehouse-project/datasets/source_erp/loc_a101.csv'
DELIMITER ','
CSV HEADER;

