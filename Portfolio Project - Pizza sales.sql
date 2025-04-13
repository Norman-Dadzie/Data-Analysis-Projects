SELECT *
FROM pizza_sales;

-- creating a working table named as pizza_sales_staging
CREATE TABLE pizza_sales_staging
LIKE pizza_sales;

-- copying everything into our working table
INSERT INTO pizza_sales_staging
SELECT *
FROM pizza_sales;

-- creating row number
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY pizza_id, order_id, pizza_name_id, quantity, order_date, order_time, unit_price, 
			 total_price, pizza_size, pizza_category, pizza_ingredients, pizza_name) AS row_num
FROM pizza_sales_staging;

-- CTE
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
				PARTITION BY pizza_id, order_id, pizza_name_id, quantity, order_date, order_time, unit_price, 
							total_price, pizza_size, pizza_category, pizza_ingredients, pizza_name) AS row_num
FROM pizza_sales_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


SELECT order_date, 
       STR_TO_DATE(order_date, '%d-%m-%Y') AS formatted_date
FROM pizza_sales_staging;

UPDATE pizza_sales_staging 
SET order_date = STR_TO_DATE(order_date, '%d-%m-%Y');



-- FINDING BLANKS AND UPDATING IT TO NULL SO WE CAN DELETE THEM LATER
SELECT *
FROM pizza_sales_staging
WHERE pizza_name = ' ';

SELECT *
FROM pizza_sales_staging
WHERE pizza_name = NULL;

SELECT *
FROM pizza_sales_staging
WHERE pizza_id = ' ';

SELECT *
FROM pizza_sales_staging
WHERE pizza_id = NULL;

SELECT *
FROM pizza_sales_staging
WHERE quantity = ' ';

SELECT *
FROM pizza_sales_staging
WHERE quantity = NULL;

SELECT *
FROM pizza_sales_staging
WHERE unit_price = NULL;

SELECT *
FROM pizza_sales_staging;

ALTER TABLE pizza_sales_staging
DROP COLUMN row_num;

ALTER TABLE pizza_sales_staging
DROP COLUMN pizza_ingredients;

-- EXPLORATORY DATA ANALYSIS
SELECT *
FROM pizza_sales_staging;

SELECT SUM(total_price) 
AS Total_Revenue 
 FROM pizza_sales_staging;
 
 SELECT pizza_category, SUM(quantity) AS Total_Quantity_Sold
FROM pizza_sales_staging
GROUP BY pizza_category
ORDER BY Total_Quantity_Sold DESC;

SELECT (SUM(total_price) / COUNT(DISTINCT order_id)) 
AS Avg_order_Value 
FROM pizza_sales_staging;

SELECT SUM(quantity)
AS total_pizza_sold 
FROM pizza_sales_staging;

SELECT COUNT(DISTINCT order_id)
AS total_orders
FROM pizza_sales_staging;

SELECT CAST(CAST(SUM(quantity) AS DECIMAL(10,2)) / 
CAST(COUNT(DISTINCT order_id) AS DECIMAL(10,2)) AS DECIMAL(10,2))
AS Avg_Pizzas_per_order
FROM pizza_sales_staging;

-- daily trends for total orders
SELECT *
FROM pizza_sales_staging;

SELECT DAYNAME(order_date) AS order_day, COUNT(DISTINCT order_id) AS total_orders 
FROM pizza_sales_staging
GROUP BY DAYNAME(order_date)
ORDER BY FIELD(order_day, 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday');


-- monthly trends for monthly order
SELECT *
FROM pizza_sales_staging;

SELECT MONTHNAME(order_date) AS Month_Name, 
       COUNT(DISTINCT order_id) AS Total_Orders
FROM pizza_sales_staging
GROUP BY MONTH(order_date), Month_Name
ORDER BY MONTH(order_date);

-- PCTs
SELECT pizza_category, 
       CAST(SUM(total_price) AS DECIMAL(10,2)) AS total_revenue,
       CAST((SUM(total_price) * 100) / (SELECT SUM(total_price) FROM pizza_sales_staging) AS DECIMAL(10,2)) AS PCT
FROM pizza_sales_staging
GROUP BY pizza_category
ORDER BY PCT;

SELECT pizza_size, 
       CAST(SUM(total_price) AS DECIMAL(10,2)) AS total_revenue,
       CAST((SUM(total_price) * 100) / (SELECT SUM(total_price) FROM pizza_sales_staging) AS DECIMAL(10,2)) AS PCT
FROM pizza_sales_staging
GROUP BY pizza_size
ORDER BY PCT;

-- TOP 5 and LAST 5 
-- by revenue
SELECT pizza_name, 
       CAST(SUM(total_price) AS DECIMAL(10,2)) AS Total_Revenue
FROM pizza_sales
GROUP BY pizza_name
ORDER BY Total_Revenue DESC
LIMIT 5;

SELECT pizza_name, 
       CAST(SUM(total_price) AS DECIMAL(10,2)) AS Total_Revenue
FROM pizza_sales
GROUP BY pizza_name
ORDER BY Total_Revenue ASC
LIMIT 5;

-- by quantity
SELECT pizza_name, 
       CAST(SUM(quantity) AS DECIMAL(10,2)) AS Total_Quantity
FROM pizza_sales
GROUP BY pizza_name
ORDER BY Total_Quantity DESC
LIMIT 5;

SELECT pizza_name, 
       CAST(SUM(quantity) AS DECIMAL(10,2)) AS Total_Quantity
FROM pizza_sales
GROUP BY pizza_name
ORDER BY Total_Quantity ASC
LIMIT 5;

-- by order_id
SELECT pizza_name, 
       COUNT(DISTINCT order_id) AS Total_Orders
FROM pizza_sales
GROUP BY pizza_name
ORDER BY Total_Orders DESC
LIMIT 5;

SELECT pizza_name, 
       COUNT(DISTINCT order_id) AS Total_Orders
FROM pizza_sales
GROUP BY pizza_name
ORDER BY Total_Orders ASC
LIMIT 5;
