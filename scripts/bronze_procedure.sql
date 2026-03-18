/*

After creating the tables, a stored procedure is created so that we don't need to run the same queries repeatedly.

A stored procedure in SQL is a pre-compiled collection of one or more SQL statements and optional procedural logic 
(like control flow, variables, and error handling) that is stored in the database management system (DBMS)

Benefits: The same procedure can be called by multiple applications or users, eliminating the need to write the same SQL code repeatedly.

Once stored procedure is created, I have applied the logic to calculate query execution time for each table.

*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE start_time TIMESTAMP;
end_time TIMESTAMP;
batch_start_time TIMESTAMP;
batch_end_time TIMESTAMP;

BEGIN
batch_start_time := CLOCK_TIMESTAMP();
start_time := CLOCK_TIMESTAMP();

TRUNCATE TABLE bronze.crm_prd_info;
COPY bronze.crm_prd_info 
FROM 'D:/Work/Sample Datasets/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
DELIMITER ','
CSV HEADER;

end_time := CLOCK_TIMESTAMP();
RAISE NOTICE 'Execution time for crm_prd_info: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

start_time := CLOCK_TIMESTAMP();

TRUNCATE TABLE bronze.crm_cust_info;
COPY bronze.crm_cust_info 
FROM 'D:/Work/Sample Datasets/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
DELIMITER ','
CSV HEADER;

end_time := CLOCK_TIMESTAMP();
RAISE NOTICE 'Execution time for crm_cust_info: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

start_time := CLOCK_TIMESTAMP();

TRUNCATE TABLE bronze.srm_sales_details;
COPY bronze.crm_sales_details
FROM 'D:/Work/Sample Datasets/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
DELIMITER ','
CSV HEADER;

end_time := CLOCK_TIMESTAMP();
RAISE NOTICE 'Execution time for crm_sales_details: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

start_time := CLOCK_TIMESTAMP();

TRUNCATE TABLE bronze.erp_cust_az12;
COPY bronze.erp_cust_az12
FROM 'D:/Work/Sample Datasets/sql-data-warehouse-project/datasets/source_erp/cust_az12.csv'
DELIMITER ','
CSV HEADER;

end_time := CLOCK_TIMESTAMP();
RAISE NOTICE 'Execution time for cust_az12: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

start_time := CLOCK_TIMESTAMP();

TRUNCATE TABLE bronze.erp_px_cat_g1v2;
COPY bronze.erp_px_cat_g1v2
FROM 'D:/Work/Sample Datasets/sql-data-warehouse-project/datasets/source_erp/px_cat_g1v2.csv'
DELIMITER ','
CSV HEADER;

end_time := CLOCK_TIMESTAMP();
RAISE NOTICE 'Execution time for erp_px_cat_g1v2: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

start_time := CLOCK_TIMESTAMP();

TRUNCATE TABLE bronze.erp_loc_a101;
COPY bronze.erp_loc_a101
FROM 'D:/Work/Sample Datasets/sql-data-warehouse-project/datasets/source_erp/loc_a101.csv'
DELIMITER ','
CSV HEADER;

end_time := CLOCK_TIMESTAMP();
RAISE NOTICE 'Execution time for erp_loc_a101: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
batch_end_time := CLOCK_TIMESTAMP();
RAISE NOTICE 'Execution time for whole batch: % seconds', EXTRACT(EPOCH FROM batch_end_time - batch_start_time);

END;
$$;

CALL bronze.load_bronze();

