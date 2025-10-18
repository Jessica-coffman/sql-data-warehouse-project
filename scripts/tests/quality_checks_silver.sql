/*
These are a list of the quality checks, notes, and bug fixes
that I worked on / encountered while cleaning up the raw CSV 
files into the bronze layer. Intended purpose of these are 
to be run individually to check quality of data and then action
*/


SELECT
*
FROM bronze.crm_cust_info

--query the data to find any duplicate primary keys and nulls
SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

--===============================================================
--BEGIN DATA CLEANSING
--=============================================================== 



--===============================================================
--BEGIN remove duplicates
--===============================================================
--pick one of the bad values (duplicate primary key)
SELECT
*
FROM bronze.crm_cust_info
WHERE cst_id = 29466

--rank the bad value (duplicate primary key) by updated date (find most recent)
SELECT
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_most_recent
FROM bronze.crm_cust_info
WHERE cst_id = 29466

--isolate all rows where the most recent is 1 (all and duplicate values that are the most recent entry)
SELECT
*
FROM(
SELECT
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_most_recent
FROM bronze.crm_cust_info)t

WHERE flag_most_recent = 1;

--===============================================================
--BEGIN remove extra spaces from strings
--===============================================================


--check columns to see if there are any leading or trailing spaces
SELECT 
cst_first_name
FROM bronze.crm_cust_info
WHERE cst_first_name != TRIM(cst_first_name)

--===============================================================
--BEGIN inserting into SILVER
--===============================================================


INSERT INTO silver.crm_cust_info (
      cst_id,
      cst_key,
      cst_first_name,
      cst_last_name,
      cst_marital_status,
      cst_gndr,
      cst_create_date)

--===============================================================
--BEGIN fully cleaning data
--===============================================================


SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,

CASE WHEN TRIM(UPPER(cst_gndr)) = 'F' THEN 'Female'
	 WHEN TRIM(UPPER(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr,

CASE WHEN TRIM(UPPER(cst_marital_status)) = 'M' THEN 'Married'
	 WHEN TRIM(UPPER(cst_marital_status)) = 'S' THEN 'Single'
	 ELSE 'n/a'

END cst_marital_status,
cst_create_date
FROM (
SELECT
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_most_recent
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
)t
WHERE flag_most_recent = 1;

--===============================================================
--BEGIN cleaning up bronze.crm_prd_info
--===============================================================


SELECT
*
FROM bronze.crm_cust_info;

/*
THINGS TO CHECK

no duplicate prd_id 
no nulls in prd_id 
no extra spaces in prd_nm 
prd_cost is null = n/a
prd_cost is $##.##
prd_end_date null = n/a
*/

SELECT
COUNT(*), 
prd_id
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1;
-- no duplicate prd_ids!

SELECT
prd_id
FROM bronze.crm_prd_info
WHERE prd_id IS NULL;  -- no nulls!

SELECT
COUNT(TRIM(prd_nm)) AS numberchar,
COUNT(prd_nm) AS ognumchar
FROM bronze.crm_prd_info
GROUP BY prd_nm
HAVING COUNT(TRIM(prd_nm)) < COUNT(prd_nm); --tenatively no extra characters??

--other way to do that ^
SELECT
prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)



SELECT
prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL; -- returned NULL values (2) need to clean this up

SELECT
prd_end_dt
FROM bronze.crm_prd_info
WHERE prd_end_dt IS NULL; -- returned NULL values (197) need to clean this up

SELECT
prd_line
FROM bronze.crm_prd_info
WHERE prd_line IS NULL; -- returned NULL values (17) need to clean this up


--check to see if NULLS or NEGITIVE NUYMBERS in PRD_COST
SELECT
prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;


--check to see if the end date is smaller than the start date
SELECT
*
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;
--foudn that the start date is after the end date. 
--a solution to this would be to make the end date after the previous price start date
SELECT
*,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt
FROM bronze.crm_prd_info

--seperate the prd_key into two columns
--creats cat_id
SELECT
prd_key,
SUBSTRING(prd_key, 1, 5) AS cat_id
FROM bronze.crm_prd_info;

SELECT distinct id FROM bronze.erp_px_cat_g1v2

--replace - with _
SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
REPLACE(SUBSTRING(prd_key, 7, lEN(prd_key)), '-', '_') AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
     WHEN 'R' THEN 'Road'
     WHEN 'M' THEN 'Mountian'
     WHEN 'S' THEN 'other Sales'
     WHEN 'T' THEN 'Touring'
     ELSE 'n/a'
END prd_line,
CAST (prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt 
FROM bronze.crm_prd_info


-- finds wwhere the prd_key doesnt match the other prd_key
--WHERE REPLACE(SUBSTRING(prd_key, 7, lEN(prd_key)), '-', '_') IN (SELECT sls_prd_key FROM bronze.crm_sales_details)

--finds all prd_key (with - replaced with _) is not in the category table
--WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') NOT IN (SELECT distinct id FROM bronze.erp_px_cat_g1v2); 

--check to see if our created prd_key matches the sales_details
--SELECT * FROM bronze.crm_sales_details


--check prd_name for any extra sapces

clear()

--===============================================================
--===============================================================
--BEGIN cleaning up bronze.crm_sales_details
--===============================================================
--===============================================================



/*
Begin by looking at the data. First I am goin to query the first 200 rows. 
This allows me to take a sneak peak at the data to identify any possible
problems while also keeping the load low.
*/

SELECT TOP (1000) [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,[sls_order_dt]
      ,[sls_ship_dt]
      ,[sls_due_dt]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_price]
  FROM [DataWarehouse].[bronze].[crm_sales_details]


/*
after querying the data, im now going to look at this data,
compare it with the other tables I may join this one with. 
Step 1 is to check for nulls.

ex:
SELECT
*
FROM [table]
WHERE [column] IS NULL

if this returns empty, then there are no null values in that column

FINDINGS:
both sls_price and sls_price have NULLS. does this make sense for my data (I dont think so
but I should ask the database master to make sure)

*/

--the two queries that revealed NULLs
SELECT
*
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL

SELECT
*
FROM bronze.crm_sales_details
WHERE sls_price IS NULL

/*
Next I am going to make sure that there are no additional 
spaces where there should not be, by using the TRIM() func
*/

SELECT TOP (1000) [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,[sls_order_dt]
      ,[sls_ship_dt]
      ,[sls_due_dt]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_price]
  FROM [DataWarehouse].[bronze].[crm_sales_details]
  WHERE sls_order_num != TRIM(sales_order_num)

  /*
  Next step is to see if the dates look like dates
  if not, cast them as dates. Be careful tho. if there are
  0s in the data, this will not work. to do that, make the 0s
  null by using the NULLIF() function. Also make sure
  that if the format is YYYY MM DD the total leng is 8 always
  */
  NULLIF(sls_order_dt, 0) AS sls_order_dt
  CAST(sls_order_dt) AS DATE
  WHERE LEN(sls_orer_dt) != 8

  INSERT INTO silver.erp_cust_az12(
  cid,
  bdate,
  gen)

  SELECT
  CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
       ELSE cid
  END cid,
    CASE WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
   END AS bdate,
    CASE WHEN TRIM(UPPER(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN TRIM(UPPER(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen
  FROM bronze.erp_cust_az12
  WHERE bdate < '1924-01-01'OR bdate > GETDATE()


  SELECT DISTINCT gen

  FROM bronze.erp_cust_az12
