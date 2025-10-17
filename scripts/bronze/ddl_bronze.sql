/*
Creating the DDL (Data Definition Langugae) 
and creating a bunch of the tables

WARNING: If these tables already exist, then execution will drop and 
recreate the table resulting in possible DATA LOSS

*/

USE DataWarehouse

    -- If bronze customer info table exists, drop the table
    IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
        DROP TABLE bronze.crm_cust_info;

    --CREATE Bronze customer info table
    CREATE TABLE bronze.crm_cust_info(
        cst_id INT,
        cst_key NVARCHAR(50),
        cst_first_name NVARCHAR(50),
        cst_last_name NVARCHAR(50),
        cst_material_status NVARCHAR(50),
        cst_gender NVARCHAR(50),
        cst_create_date DATE
    );
    GO
    -- If bronze product info table exists, drop the table
    IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
        DROP TABLE bronze.crm_prd_info;

    --CREATE Bronze product info table
    CREATE TABLE bronze.crm_prd_info(
        prd_id INT,
        prd_key NVARCHAR(50),
        prd_nm NVARCHAR(50),
        prd_cost INT,
        prd_line NVARCHAR(50),
        prd_start_dt DATETIME,
        prd_end_dt DATETIME
    );
    GO

    -- If 'bronze.crm_sales_details' table exists, drop the table
    IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
        DROP TABLE bronze.crm_sales_details;

    --CREATE Bronze sales details table
    CREATE TABLE bronze.crm_sales_details(
        sls_ord_num NVARCHAR(50),
        sls_prd_key NVARCHAR(50),
        sls_cust_id INT,
        sls_order_dt INT,
        sls_ship_dt INT,
        sls_due_dt INT,
        sls_sales INT,
        sls_quantity INT,
        sls_price INT
    );
    GO

    -- If 'bronze.erp_cust_az12' table exists, drop the table
    IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
        DROP TABLE bronze.erp_cust_az12;

    --CREATE bronze CUST source table
    CREATE TABLE bronze.erp_cust_az12(
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50)
    );
    GO

    -- If 'bronze.erp_loc_a101' table exists, drop the table
    IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
        DROP TABLE bronze.erp_loc_a101;

    --CREATE bronze LOC source table
    CREATE TABLE bronze.erp_loc_a101(
    cid NVARCHAR(50),
    cntry NVARCHAR(50)
    );
    GO

    -- If 'bronze.erp_px_cat_g1v2' table exists, drop the table
    IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
        DROP TABLE bronze.erp_px_cat_g1v2;

    --CREATE bronze PX CAT source table
    CREATE TABLE bronze.erp_px_cat_g1v2(
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
    );
    GO
