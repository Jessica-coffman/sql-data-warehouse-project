--=================================================================================
--=================================================================================
/* TASK: create a customer report
shows names, ages, and transactional details
segements customers into VIP regular and new categories
segements customers into age groups
aggregated customer level metrics, total(orders, sales, quantity purchased products, lifespan (in months)
calculates KPIs
	months since last order
	average order value
	average monthly spend
*/

--=================================================================================
--=================================================================================

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
FROM customer_aggregation;
GO

--=================================================================================
--=================================================================================
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
--=================================================================================
--=================================================================================

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
