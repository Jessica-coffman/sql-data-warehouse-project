/*
below is a list of notes and steps I took to create the gold layer of my database
these include queries that gradually build before the final create view.
as these are notes, some queries may not work, as they have been edited 
through trial and error
*/


--advanced data analytics project

--
--task 1: analyze sals performace over time
--

SELECT 
YEAR(order_date) AS order_year,
MONTH(order_date) AS order_month,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date);
GO

-- you can also use the DATETRUNC() function

SELECT 
DATETRUNC(month, order_date) AS order_date,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(month, order_date)
ORDER BY DATETRUNC(month, order_date);
GO
-- you can also formet this to look nicer

SELECT 
FORMAT(order_date, 'yyy-MMM') as order_date,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY FORMAT(order_date, 'yyy-MMM')
ORDER BY FORMAT(order_date, 'yyy-MMM');
GO

--
-- task 2: cumulative analysis
--

--calculate the total sales per month and the running total of sales over time
SELECT
DATETRUNC(month, order_date) AS order_date,
SUM(sales_amount) AS total_sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(month, order_date);
GO

-- now using a subquery make  a cumulative total
SELECT
order_date,
total_sales,
SUM(total_sales) OVER (ORDER BY order_date) AS total_sales
FROM(
	SELECT
	DATETRUNC(month, order_date) AS order_date,
	SUM(sales_amount) AS total_sales
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY DATETRUNC(month, order_date)
)t;
GO

-- partition the data so that it resests at the end of each year
SELECT
order_date,
total_sales,
SUM(total_sales) OVER (PARTITION BY YEAR(order_date) ORDER BY order_date) AS total_sales
FROM(
	SELECT
	DATETRUNC(month, order_date) AS order_date,
	SUM(sales_amount) AS total_sales
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY DATETRUNC(month, order_date)
)t;
GO

--now we want the moving average of the price over the years
SELECT
order_date,
total_sales,
SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales,
AVG(average_price) OVER (ORDER BY order_date) AS running_average_price
FROM(
	SELECT
	DATETRUNC(year, order_date) AS order_date,
	SUM(sales_amount) AS total_sales,
	AVG(price) AS average_price
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY DATETRUNC(year, order_date)
)t;
GO

--
--task 3: performance analysis
--

--analyze the yearly performance of products by
--comparing each product's sales to both its average 
--sales performance and the previous year's sales


SELECT
YEAR(f.order_date) AS order_year,
p.product_name,
SUM(f.sales_amount) AS current_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE f.order_date IS NOT NULL
GROUP BY 
	YEAR(f.order_date), 
	p.product_name;
GO

--now aggergate the data using a CTE
WITH yearly_product_sales AS (
SELECT
YEAR(f.order_date) AS order_year,
p.product_name,
SUM(f.sales_amount) AS current_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE f.order_date IS NOT NULL
GROUP BY 
	YEAR(f.order_date), 
	p.product_name
)

SELECT 
order_year,
product_name,
current_sales
FROM yearly_product_sales
ORDER BY product_name, order_year;
GO

--now get the average of the sales per product

WITH yearly_product_sales AS (
SELECT
YEAR(f.order_date) AS order_year,
p.product_name,
SUM(f.sales_amount) AS current_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE f.order_date IS NOT NULL
GROUP BY 
	YEAR(f.order_date), 
	p.product_name
)

SELECT 
order_year,
product_name,
current_sales,
AVG(current_sales) OVER (PARTITION BY product_name) average_sales
FROM yearly_product_sales
ORDER BY product_name, order_year;
GO

--now get the differnece between the current sales and the average sales

WITH yearly_product_sales AS (
SELECT
YEAR(f.order_date) AS order_year,
p.product_name,
SUM(f.sales_amount) AS current_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE f.order_date IS NOT NULL
GROUP BY 
	YEAR(f.order_date), 
	p.product_name
)

SELECT 
order_year,
product_name,
current_sales,
AVG(current_sales) OVER (PARTITION BY product_name) AS average_sales,
current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS sales_difference, --calculates the difference from the average
CASE WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Average'
	 WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Average'
	 ELSE 'Average'
END average_change
FROM yearly_product_sales
ORDER BY product_name, order_year;
GO

--now compare this to the previous years

WITH yearly_product_sales AS (
SELECT
YEAR(f.order_date) AS order_year,
p.product_name,
SUM(f.sales_amount) AS current_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE f.order_date IS NOT NULL
GROUP BY 
	YEAR(f.order_date), 
	p.product_name
)

SELECT 
order_year,
product_name,
current_sales,
AVG(current_sales) OVER (PARTITION BY product_name) AS average_sales,
current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS sales_difference, --calculates the difference from the average
CASE WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Average'
	 WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Average'
	 ELSE 'Average'
END average_change,
LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS previous_year_sales,
current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS difference_per_year
FROM yearly_product_sales
ORDER BY product_name, order_year;
GO

--creating an indicator to read

WITH yearly_product_sales AS (
SELECT
YEAR(f.order_date) AS order_year,
p.product_name,
SUM(f.sales_amount) AS current_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE f.order_date IS NOT NULL
GROUP BY 
	YEAR(f.order_date), 
	p.product_name
)

SELECT 
order_year,
product_name,
current_sales,
AVG(current_sales) OVER (PARTITION BY product_name) AS average_sales,
current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS sales_difference, --calculates the difference from the average
CASE WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Average'
	 WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Average'
	 ELSE 'Average'
END average_change,
LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS previous_year_sales,
current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS difference_per_year,
CASE WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increasing'
	 WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decreasing'
	 ELSE 'No Change'
END previous_year_change
FROM yearly_product_sales
ORDER BY product_name, order_year;
GO

--
--task 4: part to whole analysis
--

--proportional analysis. which categories contribute the most to overall sales

SELECT
category,
SUM(sales_amount) AS total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY category;
GO

-- now using a CTE, 
WITH category_sales AS (
SELECT
category,
SUM(sales_amount) AS total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY category
)

SELECT
category,
total_sales,
SUM(total_sales) OVER () overall_sales
FROM category_sales;
GO

-- now calcuate the percentage

WITH category_sales AS (
SELECT
category,
SUM(sales_amount) AS total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY category
)

SELECT
category,
total_sales,
SUM(total_sales) OVER () overall_sales,
CONCAT(ROUND((CAST(total_sales AS FLOAT) / SUM(total_sales) OVER ()) * 100, 2), '%') percentage_of_total
FROM category_sales
ORDER BY total_sales DESC;
GO

--
--task 5: data segmentation
--

--segment products into cost ranges and count how 
--many products fall into each segment

--first create a case statement to add a range column
SELECT
product_key,
product_name,
cost,
CASE WHEN cost < 100 THEN 'Below 100'
	 WHEN cost BETWEEN 100 AND 500 THEN '100-500'
	 WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
	ELSE 'Above 1000'
END cost_range
FROM gold.dim_products;
GO
--aggergate base on cost_range
WITH product_segments AS(

SELECT
product_key,
product_name,
cost,
CASE WHEN cost < 100 THEN 'Below 100'
	 WHEN cost BETWEEN 100 AND 500 THEN '100-500'
	 WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
	ELSE 'Above 1000'
END cost_range
FROM gold.dim_products
)

SELECT
cost_range,
COUNT(product_key) AS total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC;
GO

/*
group cusotmers into three segemnts
VIP: customers with at least 12 months of history and spening more than 5,000
regular: cusotmers with at least 12 months of history but spending 5000 or less
new: customers with less than 12 months of history
and find the total number of customers per segment
*/

--finding the values and tables I am targeting
SELECT
c.customer_key,
f.sales_amount,
f.order_date
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key;
GO

--now get the history (in months) of the cutomers
SELECT
c.customer_key,
SUM(f.sales_amount) AS total_spending,
MIN(order_date) AS first_order,
MAX(order_date) AS last_order,
DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
GROUP BY c.customer_key;
GO


--start building segments for over and less than 5000 and then less than 12 months
WITH customer_spending AS(

SELECT
c.customer_key,
SUM(f.sales_amount) AS total_spending,
MIN(order_date) AS first_order,
MAX(order_date) AS last_order,
DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
GROUP BY c.customer_key
)

SELECT
customer_key,
total_spending,
lifespan,
CASE WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular Customer'
	 WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP Cusomter'
	 ELSE 'New Customer'
END customer_type
FROM customer_spending;
GO

--now find total number of customer per category
WITH customer_spending AS(

SELECT
c.customer_key,
SUM(f.sales_amount) AS total_spending,
MIN(order_date) AS first_order,
MAX(order_date) AS last_order,
DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
GROUP BY c.customer_key
)

SELECT
customer_type,
COUNT(customer_key) AS total_customers
FROM(
SELECT
	customer_key,
	CASE WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular Customer'
			WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP Cusomter'
			ELSE 'New Customer'
	END customer_type
	FROM customer_spending
)t
GROUP BY customer_type
ORDER BY total_customers DESC;
GO

/*
TASK: create a customer report
shows names, ages, and transactional details
segements customers into VIP regular and new categories
segements customers into age groups
aggregated customer level metrics, total(orders, sales, quantity purchased products, lifespan (in months)
calculates KPIs
	months since last order
	average order value
	average monthly spend
*/

--step 1: get the core columns from the tables

SELECT 
*
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key;
GO

-- get the core columns for the report

SELECT 
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
c.first_name,
c.last_name,
c.birthdate
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE order_date IS NOT NULL;
GO


-- put first and last name together

SELECT 
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
DATEDIFF(year, c.birthdate, GETDATE()) customer_age
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE order_date IS NOT NULL;
GO

-- create subquery

WITH base_query AS (
SELECT 
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
DATEDIFF(year, c.birthdate, GETDATE()) customer_age
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE order_date IS NOT NULL
)

SELECT
*
FROM base_query;
GO

-- do aggrigations on this now

WITH base_query AS (
SELECT 
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
DATEDIFF(year, c.birthdate, GETDATE()) customer_age
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE order_date IS NOT NULL
)

SELECT
customer_key,
customer_number,
customer_name,
customer_age,
COUNT(DISTINCT order_number) AS total_orders,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS total_quantity,
COUNT(DISTINCT product_key) AS total_products,
MAX(order_date) AS last_order,
DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan

FROM base_query
GROUP by customer_key,customer_number, customer_name, customer_age;
GO

--now segment customers ito category and KPI


WITH base_query AS (
SELECT 
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
DATEDIFF(year, c.birthdate, GETDATE()) customer_age
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE order_date IS NOT NULL
),
customer_aggregation AS (

SELECT
customer_key,
customer_number,
customer_name,
customer_age,
COUNT(DISTINCT order_number) AS total_orders,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS total_quantity,
COUNT(DISTINCT product_key) AS total_products,
MAX(order_date) AS last_order,
DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan

FROM base_query
GROUP by customer_key,customer_number, customer_name, customer_age
)

SELECT
customer_key,
customer_number,
customer_name,
customer_age,
CASE WHEN customer_age < '20' THEN 'Under 20'
	 WHEN customer_age BETWEEN 20 AND 29 THEN '20 - 29'
	 WHEN customer_age BETWEEN 30 AND 39 THEN '30 - 39'
	 WHEN customer_age BETWEEN 40 AND 49 THEN '40 - 49'
	 ELSE '50 and above'
END AS age_group,
CASE WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular Customer'
	 WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP Cusomter'
	 ELSE 'New Customer'
END AS customer_type,
total_quantity,
total_products,
last_order,
lifespan
FROM customer_aggregation;
GO

--now calculate how many months since last order

WITH base_query AS (
SELECT 
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
DATEDIFF(year, c.birthdate, GETDATE()) customer_age
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE order_date IS NOT NULL
),
customer_aggregation AS (

SELECT
customer_key,
customer_number,
customer_name,
customer_age,
COUNT(DISTINCT order_number) AS total_orders,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS total_quantity,
COUNT(DISTINCT product_key) AS total_products,
MAX(order_date) AS last_order,
DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan

FROM base_query
GROUP by customer_key,customer_number, customer_name, customer_age
)

SELECT
customer_key,
customer_number,
customer_name,
customer_age,
CASE WHEN customer_age < '20' THEN 'Under 20'
	 WHEN customer_age BETWEEN 20 AND 29 THEN '20 - 29'
	 WHEN customer_age BETWEEN 30 AND 39 THEN '30 - 39'
	 WHEN customer_age BETWEEN 40 AND 49 THEN '40 - 49'
	 ELSE '50 and above'
END AS age_group,
CASE WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular Customer'
	 WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP Cusomter'
	 ELSE 'New Customer'
END AS customer_type,
last_order,
DATEDIFF(month, last_order, GETDATE()) AS recency,
total_quantity,
total_products,
lifespan
FROM customer_aggregation;
GO

--calculate the average order value


WITH base_query AS (
SELECT 
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
DATEDIFF(year, c.birthdate, GETDATE()) customer_age
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE order_date IS NOT NULL
),
customer_aggregation AS (

SELECT
customer_key,
customer_number,
customer_name,
customer_age,
COUNT(DISTINCT order_number) AS total_orders,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS total_quantity,
COUNT(DISTINCT product_key) AS total_products,
MAX(order_date) AS last_order,
DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan

FROM base_query
GROUP by customer_key,customer_number, customer_name, customer_age
)

SELECT
customer_key,
customer_number,
customer_name,
customer_age,
CASE WHEN customer_age < '20' THEN 'Under 20'
	 WHEN customer_age BETWEEN 20 AND 29 THEN '20 - 29'
	 WHEN customer_age BETWEEN 30 AND 39 THEN '30 - 39'
	 WHEN customer_age BETWEEN 40 AND 49 THEN '40 - 49'
	 ELSE '50 and above'
END AS age_group,
CASE WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular Customer'
	 WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP Cusomter'
	 ELSE 'New Customer'
END AS customer_type,
last_order,
DATEDIFF(month, last_order, GETDATE()) AS recency,
total_quantity,
total_products,
lifespan,
--find average order value
CASE WHEN total_orders = 0 THEN 0 --handling the 0s so dont divide by 0
   	 ELSE total_sales / total_orders
END AS average_order_value
FROM customer_aggregation;
GO

--now calculate the average monthly spend

WITH base_query AS (
SELECT 
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
DATEDIFF(year, c.birthdate, GETDATE()) customer_age
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE order_date IS NOT NULL
),
customer_aggregation AS (

SELECT
customer_key,
customer_number,
customer_name,
customer_age,
COUNT(DISTINCT order_number) AS total_orders,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS total_quantity,
COUNT(DISTINCT product_key) AS total_products,
MAX(order_date) AS last_order,
DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan

FROM base_query
GROUP by customer_key,customer_number, customer_name, customer_age
)

SELECT
customer_key,
customer_number,
customer_name,
customer_age,
CASE WHEN customer_age < '20' THEN 'Under 20'
	 WHEN customer_age BETWEEN 20 AND 29 THEN '20 - 29'
	 WHEN customer_age BETWEEN 30 AND 39 THEN '30 - 39'
	 WHEN customer_age BETWEEN 40 AND 49 THEN '40 - 49'
	 ELSE '50 and above'
END AS age_group,
CASE WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular Customer'
	 WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP Cusomter'
	 ELSE 'New Customer'
END AS customer_type,
last_order,
DATEDIFF(month, last_order, GETDATE()) AS recency,
total_quantity,
total_products,
lifespan,
--find average order value
CASE WHEN total_orders = 0 THEN 0 --handling the 0s so dont divide by 0
   	 ELSE total_sales / total_orders
END AS average_order_value,
--average monthly spend
CASE WHEN lifespan = 0 THEN total_sales --handling the 0s so dont divide by 0
   	 ELSE total_sales / lifespan
END AS average_monthly_spend 
FROM customer_aggregation;
GO

--now put the whole query into the database as a view

CREATE VIEW gold.report_customers AS 
WITH base_query AS (
SELECT 
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
DATEDIFF(year, c.birthdate, GETDATE()) customer_age
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE order_date IS NOT NULL
),
customer_aggregation AS (

SELECT
customer_key,
customer_number,
customer_name,
customer_age,
COUNT(DISTINCT order_number) AS total_orders,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS total_quantity,
COUNT(DISTINCT product_key) AS total_products,
MAX(order_date) AS last_order,
DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan

FROM base_query
GROUP by customer_key,customer_number, customer_name, customer_age
)

SELECT
customer_key,
customer_number,
customer_name,
customer_age,
CASE WHEN customer_age < '20' THEN 'Under 20'
	 WHEN customer_age BETWEEN 20 AND 29 THEN '20 - 29'
	 WHEN customer_age BETWEEN 30 AND 39 THEN '30 - 39'
	 WHEN customer_age BETWEEN 40 AND 49 THEN '40 - 49'
	 ELSE '50 and above'
END AS age_group,
CASE WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular Customer'
	 WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP Cusomter'
	 ELSE 'New Customer'
END AS customer_type,
last_order,
DATEDIFF(month, last_order, GETDATE()) AS recency,
total_quantity,
total_products,
lifespan,
--find average order value
CASE WHEN total_orders = 0 THEN 0 --handling the 0s so dont divide by 0
   	 ELSE total_sales / total_orders
END AS average_order_value,
--average monthly spend
CASE WHEN lifespan = 0 THEN total_sales --handling the 0s so dont divide by 0
   	 ELSE total_sales / lifespan
END AS average_monthly_spend 
FROM customer_aggregation


--
-- product report
/* task: consoliddate key product metrics and behaviiors
	gather essential fields (name, category, subcategory, and cost.)
	segments products by revenus to identify high performers, mid range, and low perfomers
	also 
	total orders
	total sales
	total quantity sold
	total customers (unique)
	lifespan in months
	and KPIs
		recency 
		agerage order revenuy
		average monthly revenue
*/
CREATE VIEW gold.report_products AS
WITH base_query AS (
SELECT
	f.order_number,
	f.order_date,
	f.customer_key,
	f.sales_amount,
	f.quantity,
	f.product_key,
	p.product_name, 
	p.category,
	p.subcategory,
	p.cost

FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE order_date IS NOT NULL
),

product_aggregations AS (
	SELECT
		product_key,
		product_name,
		category,
		subcategory,
		cost,
		DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan,
		MAX(order_date) AS last_sale_date,
		COUNT(DISTINCT order_number) AS total_orders,
		COUNT(DISTINCT customer_key) AS total_customers,
		SUM(sales_amount) AS total_sales,
		SUM(quantity) AS total_quantity,
		ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)),1) AS average_selling_price
	FROM base_query

	GROUP BY product_key, product_name, category, subcategory, cost
	)
	SELECT 
		product_key,
		product_name,
		category,
		subcategory, 
		cost,
		last_sale_date,
		DATEDIFF(month, last_sale_date, GETDATE()) AS recency_in_months,
		CASE 
			WHEN total_sales > 50000 THEN 'High Performer'
			WHEN total_sales <= 10000 THEN 'High Performer'
			ELSE 'Low Perfomrer'
		END AS product_segment,
		lifespan,
		total_orders,
		total_sales,
		total_quantity,
		total_customers,
		average_selling_price,
		CASE
			WHEN total_orders = 0 THEN 0
			ELSE total_sales / total_orders 
		END AS average_order_revenue,

		CASE
			WHEN lifespan = 0 THEN total_sales
			ELSE total_sales / lifespan
		END AS average_monthly_revenue

		FROM product_aggregations;
GO
