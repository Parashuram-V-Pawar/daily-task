CREATE DATABASE superstore_db;

use superstore_db;

-- Load data from csv.
BULK INSERT orders_data
FROM 'train.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n'
);

SELECT TOP(5) * FROM orders_data;

/* 
Task 1: High-Value Order Identification
Finance wants to review unusually high-value orders.
Write a query to fetch all orders where sales are greater than the overall average sales.
*/
SELECT Order_ID
FROM orders_data
WHERE sales > (
    SELECT AVG(sales)
    FROM orders_data
    );
GO

/*
Task 2: City-Level Revenue Concentration
Management believes that revenue is concentrated in a small number of cities.
Write a query to retrieve the top 5 cities by total sales, ordered from highest to lowest.
*/
SELECT TOP(5) City 
FROM orders_data
ORDER BY sales DESC;
GO

/*
Task 3: Customer Purchase Behavior
Marketing wants to identify repeat customers.
Write a query to find customers who have placed more than 5 orders, along with their total sales.
*/
SELECT Customer_ID, Customer_Name, SUM(Sales) as Total_Sales
FROM orders_data
GROUP BY Customer_ID, Customer_Name
HAVING count(*) > 5;
GO

/*
Task 4: Segment Performance Analysis
Leadership wants to compare customer segments.
Write a query to calculate total sales and total number of orders for each segment, sorted by total sales.
*/
SELECT Segment, COUNT(*) AS Total_Orders, SUM(sales) AS Total_Sales
FROM orders_data
GROUP BY Segment
ORDER BY Total_Sales DESC;
GO

/*
Task 5: Shipping Delay Detection
Operations wants to detect shipment delays.
Write a query to identify orders where the shipping duration exceeds 4 days
(Ship_Date minus Order_Date greater than 4).
*/
SELECT Order_ID, DATEDIFF(DAY, Order_Date, Ship_Date) AS DateDiff
FROM orders_data
WHERE DATEDIFF(DAY, Order_Date, Ship_Date) > 4;

/*
Task 6: Ship Mode Utilization
Logistics wants to understand how shipping modes are being used.
Write a query to calculate the percentage contribution of each ship mode based on the total number of orders.
*/
WITH total_orders AS (
    SELECT COUNT(*) as order_count
    FROM orders_data
),
order_group AS (
    SELECT Ship_mode, COUNT(*) As group_count
    FROM orders_data
    GROUP BY Ship_mode
)
SELECT Ship_mode, (CAST(group_count AS FLOAT)/CAST(order_count AS FLOAT) * 100) AS Order_Percentage
FROM order_group, total_orders


/* 
Task 7: City-Level Sales Ranking
Sales leadership wants comparative insights at the city level.
Write a query to rank cities within each country based on total sales using a window function.
*/
SELECT 
Country, City, SUM(Sales),
DENSE_RANK() OVER(
    PARTITION BY Country Order BY SUM(Sales) DESC
) AS City_Rank
FROM orders_data
GROUP BY Country, City

/*
Task 8: Monthly Order Trend
Management wants to analyze order volume trends over time.
Write a query to calculate the number of orders per month, grouped by year and month using Order_Date.
*/
SELECT YEAR(Order_Date) AS Year_Order, MONTH(Order_Date) AS Month_Order, COUNT(*) AS Number_of_Orders
FROM orders_data
GROUP BY YEAR(Order_Date), MONTH(Order_Date) 



/*
Task 9: Data Quality Validation
Data engineering suspects data inconsistencies.
Write a query to identify orders where the ship date is earlier than the order date.
*/
SELECT * 
FROM orders_data
WHERE Ship_Date < Order_Date;



-- Queries asked in meet to solve
/* Query to print first 5 records in a table*/
SELECT TOP(5) *
FROM orders_data


/* Display data of customers who have more than 5 order and their combined sales is above 500*/
SELECT Customer_ID, SUM(sales) AS Total_sales
FROM orders_data
GROUP BY Customer_ID
HAVING COUNT(*) > 5 AND AVG(Sales) > 500








CREATE TABLE orders (
    Row ID,Order ID,Order Date,Ship Date,Ship Mode,Customer ID,Customer Name,Segment,Country,City,State,Postal Code,Region,Product ID,Category,Sub-Category,Product Name,Sales
)

BULK INSERT orders_data
FROM 'train.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n'
);

