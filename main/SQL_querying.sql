
-- Task 1. How many stores does each country has?
/*
The Operations team would like to know which countries we currently operate in and which country now has the most stores. Perform a query on the database to get the information, it should return the following information:

+----------+-----------------+
| country  | total_no_stores |
+----------+-----------------+
| GB       |             265 |
| DE       |             141 |
| US       |              34 |
+----------+-----------------+
Note: DE is short for Deutschland(Germany)
*/
    SELECT
        country_code AS country,
        COUNT(*) AS total_no_stores
    FROM
        dim_store_details
    GROUP BY
        country_code
    ORDER BY
        total_no_stores DESC;

-- Task 2. which locations currently have most stores?
/*
The business stakeholders would like to know which locations currently have the most stores.

They would like to close some stores before opening more in other locations.

Find out which locations have the most stores currently. The query should return the following:
```
+-------------------+-----------------+
|     locality      | total_no_stores |
+-------------------+-----------------+
| Chapletown        |              14 |
| Belper            |              13 |
| Bushley           |              12 |
| Exeter            |              11 |
| High Wycombe      |              10 |
| Arbroath          |              10 |
| Rutherglen        |              10 |
+-------------------+-----------------+
*/
    SELECT
        locality,
        COUNT(*) AS total_no_stores
    FROM
        dim_store_details
    GROUP BY
        locality
    ORDER BY
        total_no_stores DESC
    LIMIT 7;

-- Task 3. which months produce largest amount of sales?
/*
Query the database to find out which months have produced the most sales. The query should return the following information:

+-------------+-------+
| total_sales | month |
+-------------+-------+
|   673295.68 |     8 |
|   668041.45 |     1 |
|   657335.84 |    10 |
|   650321.43 |     5 |
|   645741.70 |     7 |
|   645463.00 |     3 |
+-------------+-------+
*/

    SELECT 
    SUM(CAST(dp.product_price AS NUMERIC) * ot.product_quantity) AS total_sales,
    dt.month
    FROM 
        orders_table ot
    JOIN 
        dim_products dp ON ot.product_code = dp.product_code
    JOIN 
        dim_date_times dt ON ot.date_uuid = dt.date_uuid
    GROUP BY 
        dt.month
    ORDER BY 
        total_sales DESC
    LIMIT 6;

-- Task 4. how much sales from online and offline?
/*
The company is looking to increase its online sales.

They want to know how many sales are happening online vs offline.

Calculate how many products were sold and the amount of sales made for online and offline purchases.

You should get the following information:

+------------------+-------------------------+----------+
| numbers_of_sales | product_quantity_count  | location |
+------------------+-------------------------+----------+
|            26957 |                  107739 | Web      |
|            93166 |                  374047 | Offline  |
+------------------+-------------------------+----------+
*/

SELECT 
    COUNT(*) AS numbers_of_sales, 
    SUM(ot.product_quantity) AS product_quantity_count, 
    CASE 
        WHEN ds.store_code LIKE 'WEB%' THEN 'Web' 
        ELSE 'Offline' 
    END AS location
FROM 
    orders_table ot
JOIN 
    dim_store_details ds ON ot.store_code = ds.store_code
GROUP BY 
    location;

-- Task 5. what percentage of sales come from each type of stores?
/*
The sales team wants to know which of the different store types is generated the most revenue so they know where to focus.

Find out the total and percentage of sales coming from each of the different store types.

The query should return:

+-------------+-------------+---------------------+
| store_type  | total_sales | percentage_total(%) |
+-------------+-------------+---------------------+
| Local       |  3440896.52 |               44.87 |
| Web portal  |  1726547.05 |               22.44 |
| Super Store |  1224293.65 |               15.63 |
| Mall Kiosk  |   698791.61 |                8.96 |
| Outlet      |   631804.81 |                8.10 |
+-------------+-------------+---------------------+
*/

SELECT 
    ds.store_type,
    SUM(CAST(dp.product_price AS NUMERIC) * ot.product_quantity) AS total_sales,
    ROUND(
        (SUM(CAST(dp.product_price AS NUMERIC) * ot.product_quantity) / 
        (SELECT SUM(CAST(dp.product_price AS NUMERIC) * ot.product_quantity)
         FROM orders_table ot
         JOIN dim_products dp ON ot.product_code = dp.product_code)) * 100, 2
    ) AS percentage_total
FROM 
    orders_table ot
JOIN 
    dim_products dp ON ot.product_code = dp.product_code
JOIN 
    dim_store_details ds ON ot.store_code = ds.store_code
GROUP BY 
    ds.store_type
ORDER BY 
    total_sales DESC;

-- Task 6. Which month in each year produce biggest cost sales?
/*
The company stakeholders want assurances that the company has been doing well recently.

Find which months in which years have had the most sales historically.

The query should return the following information:

+-------------+------+-------+
| total_sales | year | month |
+-------------+------+-------+
|    27936.77 | 1994 |     3 |
|    27356.14 | 2019 |     1 |
|    27091.67 | 2009 |     8 |
|    26679.98 | 1997 |    11 |
|    26310.97 | 2018 |    12 |
|    26277.72 | 2019 |     8 |
|    26236.67 | 2017 |     9 |
|    25798.12 | 2010 |     5 |
|    25648.29 | 1996 |     8 |
|    25614.54 | 2000 |     1 |
+-------------+------+-------+
*/

SELECT 
    SUM(CAST(dp.product_price AS NUMERIC) * ot.product_quantity) AS total_sales,
    dt.year,
    dt.month
FROM 
    orders_table ot
JOIN 
    dim_products dp ON ot.product_code = dp.product_code
JOIN 
    dim_date_times dt ON ot.date_uuid = dt.date_uuid
GROUP BY 
    dt.year, dt.month
ORDER BY 
    total_sales DESC
LIMIT 10;

-- Task 7. Staff headcount?
/*
The operations team would like to know the overall staff numbers in each location around the world. Perform a query to determine the staff numbers in each of the countries the company sells in.

The query should return the values:

+---------------------+--------------+
| total_staff_numbers | country_code |
+---------------------+--------------+
|               13307 | GB           |
|                6123 | DE           |
|                1384 | US           |
+---------------------+--------------+
*/

SELECT 
    country_code,
    SUM(staff_numbers) AS total_staff
FROM 
    dim_store_details
GROUP BY 
    country_code
ORDER BY 
    total_staff DESC;

-- Task 8. which German store type selling the most?
/*
The sales team is looking to expand their territory in Germany. Determine which type of store is generating the most sales in Germany.

The query will return:

+--------------+-------------+--------------+
| total_sales  | store_type  | country_code |
+--------------+-------------+--------------+
|   198373.57  | Outlet      | DE           |
|   247634.20  | Mall Kiosk  | DE           |
|   384625.03  | Super Store | DE           |
|  1109909.59  | Local       | DE           |
+--------------+-------------+--------------+
*/
SELECT 
    SUM(CAST(dp.product_price AS NUMERIC) * ot.product_quantity) AS total_sales,
    ds.store_type,
    ds.country_code
FROM 
    orders_table ot
JOIN 
    dim_products dp ON ot.product_code = dp.product_code
JOIN 
    dim_store_details ds ON ot.store_code = ds.store_code
WHERE 
    ds.country_code = 'DE'
GROUP BY 
    ds.store_type, ds.country_code
ORDER BY 
    total_sales ASC;

-- Task 9. How quickly the company make sales?
/*
Sales would like the get an accurate metric for how quickly the company is making sales.

Determine the average time taken between each sale grouped by year, the query should return the following information:

 +------+-------------------------------------------------------+
 | year |                           actual_time_taken           |
 +------+-------------------------------------------------------+
 | 2013 | "hours": 2, "minutes": 17, "seconds": 12, "millise... |
 | 1993 | "hours": 2, "minutes": 15, "seconds": 35, "millise... |
 | 2002 | "hours": 2, "minutes": 13, "seconds": 50, "millise... | 
 | 2022 | "hours": 2, "minutes": 13, "seconds": 6,  "millise... |
 | 2008 | "hours": 2, "minutes": 13, "seconds": 2,  "millise... |
 +------+-------------------------------------------------------+
*/

WITH sales_with_times AS (
    SELECT 
        dt.year::int AS year,
        TO_TIMESTAMP(dt.year || '-' || dt.month || '-' || dt.day || ' ' || dt.timestamp, 'YYYY-MM-DD HH24:MI:SS') AS sale_timestamp
    FROM orders_table ot
    JOIN dim_date_times dt ON ot.date_uuid = dt.date_uuid
),
sales_with_diff AS (
    SELECT 
        year,
        sale_timestamp,
        LEAD(sale_timestamp) OVER (PARTITION BY year ORDER BY sale_timestamp) AS next_sale_timestamp
    FROM sales_with_times
),
time_differences AS (
    SELECT 
        year,
        sale_timestamp,
        next_sale_timestamp,
        next_sale_timestamp - sale_timestamp AS time_diff
    FROM sales_with_diff
    WHERE next_sale_timestamp IS NOT NULL
),
avg_time_per_year AS (
    SELECT 
        year,
        AVG(time_diff) AS avg_time_diff
    FROM time_differences
    GROUP BY year
)
SELECT 
    year,
    TO_CHAR(avg_time_diff, '"hours": HH24, "minutes": MI, "seconds": SS, "milliseconds": MS') AS actual_time_taken
FROM avg_time_per_year
ORDER BY avg_time_diff DESC
LIMIT 5;






































