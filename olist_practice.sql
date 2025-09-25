--- 1  Total Revenue per Seller
-- Find the total revenue (price + freight_value) per seller. Show only sellers who made more than 50,000 BRL in sales.

select 
    oi.seller_id,
    sum(oi.price + oi.freight_value) as total_revenue
from olist_order_items oi
group by oi.seller_id
having sum(oi.price + oi.freight_value) > 50000
order by total_revenue desc;

-- 2 - Monthly Orders Trend
-- Count the number of orders per month. Use the order_purchase_timestamp column and order results chronologically.

select FORMAT(o.order_purchase_timestamp, 'yyyy-mm') as month, count(o.order_id) as total_orders
from olist_orders o
group by FORMAT(o.order_purchase_timestamp, 'yyyy-mm')
order by month asc

-- 3 - Top 5 Products by Sales
-- Get the top 5 products by total revenue (price + freight_value). Use a window function (RANK() or DENSE_RANK()).

with product_sales as (
    select 
        oi.product_id,
        sum(oi.price + oi.freight_value) as total_sales
    from olist_order_items oi
    group by oi.product_id
)
select product_id, total_sales, sales_rank
from (
    select 
        product_id,
        total_sales,
        rank() over (order by total_sales desc) as sales_rank
    from product_sales
) ranked
where sales_rank <= 5;


-- 4 - Repeat Customers
-- Find customers who placed more than 3 orders. Show their customer ID and number of orders.

select 
    customer_id,
    count(order_id) as order_count
from olist_orders
group by customer_id
having count(order_id) > 3
order by order_count desc;


-- 5 -Average Review Score per Product
-- For each product, calculate the average review score. Return only products with at least 20 reviews.

select 
    oi.product_id,
    avg(cast(r.review_score as float)) as avg_review_score,
    count(r.review_id) as review_count
from olist_order_items oi
join olist_order_reviews r 
    on oi.order_id = r.order_id
group by oi.product_id
having count(r.review_id) >= 20
order by avg_review_score desc;

-- 6 - Order Ranking per Customer
-- Rank each order of a customer based on its purchase amount (price + freight_value), highest to lowest.

with order_amounts as (
    select 
        o.order_id,
        o.customer_id,
        sum(oi.price + oi.freight_value) as order_amount
    from olist_orders o
    join olist_order_items oi 
        on o.order_id = oi.order_id
    group by o.order_id, o.customer_id
)
select 
    customer_id,
    order_id,
    order_amount,
    rank() over (
        partition by customer_id order by order_amount desc
    ) as order_rank
FROM order_amounts
ORDER BY customer_id, order_rank;

-- 7 - Delivery Time Analysis
-- Calculate the average delivery days per state (customer_state). Use order_delivered_customer_date - order_purchase_timestamp.

select 
 c.customer_state,
 AVG(datediff(day, o.order_purchase_timestamp, o.order_delivered_customer_date)) as avg_del_days
 from olist_orders o
 join olist_customers c
 on o.customer_id = c.customer_id
WHERE o.order_delivered_customer_date IS NOT NULL
group by c.customer_state
order by avg_del_days desc

-- 8 - CTE for Late Deliveries
-- Using a CTE, find the percentage of orders that were delivered after the estimated delivery date.

with late_delivery as (
   select 
        order_id,
		case 
		  when order_estimated_delivery_date < order_delivered_customer_date then 1 else 0 end as late_del
        from olist_orders 
		where  order_delivered_customer_date is not null
)
select 
 CAST(sum(late_del)*100.0 / COUNT(*) as decimal(5,2)) as perc_late_del
from late_delivery

-- 9 - Top Sellers by Category
-- For each product category, find the seller with the highest total sales. Use a CTE + ROW_NUMBER() to return only the top seller per category.

with category_sales as (
    select 
        p.product_category_name,
        oi.seller_id,
        sum(oi.price + oi.freight_value) as total_sales
    from olist_order_items oi
    join olist_products p 
        on oi.product_id = p.product_id
    group by p.product_category_name, oi.seller_id
),
ranked as (
    select 
        product_category_name,
        seller_id,
        total_sales,
        row_number() over (
            partition by product_category_name order by total_sales desc
        ) as rn
    from category_sales
)
select product_category_name, seller_id, total_sales
from ranked
where rn = 1
order by total_sales desc


-- 10 - Customer Lifetime Value (CLV)
-- For each customer, calculate their total spend, number of orders, and average order value. Order results by total spend descending.

with customer_spend as (
    select 
        o.customer_id,
        sum(oi.price + oi.freight_value) as total_spend,
        count(distinct o.order_id) as total_orders
    from olist_orders o
    join olist_order_items oi 
        on o.order_id = oi.order_id
    group by o.customer_id
)
select 
    customer_id,
    total_spend,
    total_orders,
    cast(total_spend * 1.0 / total_orders as decimal(10,2)) as avg_order_value
from customer_spend
order by total_spend desc

--- 🛒 Product / Sales Related ---------------------------------------------------------------------------------------

-- 11 - Top 10 best-selling product categories by total sales (price + freight_value).

--- Solve once using TOP 10 and once using RANK().
-- Business use: Marketing team wants to know where to focus campaigns.

--- using top 10 ----

select top 10 
       p.product_category_name,
	   SUM(oi.price + oi.freight_value) as total_sales 
from olist_order_items oi
  join olist_products p on p.product_id = oi.product_id
group by product_category_name
order by total_sales desc

with sales as(
select 
       p.product_category_name,
	   SUM(oi.price + oi.freight_value) as total_sales 
	from olist_order_items oi
	  join olist_products p on p.product_id = oi.product_id  
	  group by 	product_category_name
)
SELECT product_category_name, total_sales, sales_rank
FROM (
    SELECT 
        product_category_name,
        total_sales,
        RANK() OVER (ORDER BY total_sales DESC) AS sales_rank
    FROM sales
) ranked
WHERE sales_rank <= 10

-- 12 - Top 5 most expensive products (by average unit price).
-- Solve with TOP 5.
--- Use case: Product team wants to review pricing strategy.

select top 5 product_id, avg(price) as avg_price
from olist_order_items
group by product_id
order by avg_price desc

-- 13 - Bottom 5 products by sales (low-performing products).
--- Business use: Identify items to discontinue.

select top 5 product_id, sum(freight_value+price) as sales
from olist_order_items
group by product_id
order by sales asc

-- 👤 Customer Related -------------------------------------------------------------------------------------------------

-- 14 - Top 5 customers by total lifetime spend.
-- Useful for loyalty program targeting.

select top 5 
    o.customer_id,
    sum(oi.price + oi.freight_value) as lifetime_spend
from olist_orders o
join olist_order_items oi 
    on o.order_id = oi.order_id
group by o.customer_id
order by lifetime_spend desc

-- 15 - Top 5 customers in each state by total orders.
-- Solve using ROW_NUMBER() per state.
--- Use case: Regional sales managers want to recognize key customers.

with cust_order as(
select 
   c.customer_state,
   o.customer_id,
   count(o.order_id) as order_count
from olist_orders o
 join olist_customers c on o.customer_id = c.customer_id
 group by c.customer_state, o.customer_id
)
select customer_state, customer_id, order_count
from (
      select 
	       customer_state,
           customer_id,
           order_count,
		   ROW_NUMBER() over(partition by customer_state order by order_count desc) as rn
		from cust_order
		) as ranked
	where rn<=5

-- 16 - Find customers who only bought once vs. customers who bought 5+ times.
--- Business use: Compare churn vs. loyal buyers 

select 
  customer_id, 
  count(order_id) as total_orders,
  case 
     when count(order_id)=1 then 'one_time_buyer'
	 when count(order_id) >= 5 then 'loyal'
  else 'regular buyer'
 end as customer_type
from olist_orders
group by customer_id
order by total_orders desc

-- 📦 Seller Related ----------------------------------------------------------------------------------------------------

-- 17 - Top 3 sellers by total sales in each category.
-- Business use: Seller relationship management.

with sales as (
 select 
   oi.seller_id,
   p.product_category_name,
   sum(oi.freight_value + oi.price) as total_sales
from olist_order_items oi 
 join olist_products p on p.product_id = oi.product_id
group by oi.seller_id, p.product_category_name
)
select 
   seller_id,
   product_category_name,
   total_sales 
 from (
     select 
	      seller_id,
          product_category_name,
          total_sales,
		  row_number() over(partition by product_category_name order by total_sales desc) as rn
		  from sales
		  ) as ranked
	where rn<=3

-- 18 - Find the seller with the highest average delivery delay.
-- Business use: Seller performance audit.

with seller_delays as (
    select 
        oi.seller_id,
        avg(datediff(day, o.order_estimated_delivery_date, o.order_delivered_customer_date)) AS avg_delay
    from olist_orders o
    join olist_order_items oi 
        on o.order_id = oi.order_id
    where o.order_delivered_customer_date IS NOT NULL
    group by oi.seller_id
)
select top 1 seller_id, avg_delay
from seller_delays
order by avg_delay desc

-- 19 - 🚚 Logistics Related ------------------------------------------------------------------------------------------------

-- Top 5 states with the highest average delivery time.
--- Solve with TOP 5 and ORDER BY.
-- Use case: Operations wants to optimize logistics.

select top 5 
    c.customer_state,
    avg(datediff(day, o.order_purchase_timestamp, o.order_delivered_customer_date)) AS avg_delivery_days
from olist_orders o
join olist_customers c 
    on o.customer_id = c.customer_id
where o.order_delivered_customer_date IS NOT NULL
group by c.customer_state
order by avg_delivery_days desc

-- 20 - Orders delivered after the estimated delivery date 
--— list top 5 customers who faced the most late deliveries.
-- Business use: Customer service team wants to improve satisfaction.

with cust_orders as(
select 
    o.customer_id,
	count(o.order_id) as late_orders
  from olist_orders o 
  where order_delivered_customer_date > order_estimated_delivery_date
  group by o.customer_id
)
select top 5 
    customer_id,
	late_orders
from cust_orders
  order by late_orders desc

----------------------------------------------------------------------------------------------------------------------------------
-- 1. Window Functions -----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

-- 21 - Find the rank of each order’s purchase amount (price + freight_value) per customer, ordered from highest to lowest.

select 
    c.customer_id,
    o.order_id,
    sum(oi.price + oi.freight_value) as order_purchase_amount,
    rank() over (
        partition by c.customer_id 
        order by sum(oi.price + oi.freight_value) desc
    ) as order_rank
from olist_orders o
join olist_customers c 
    on o.customer_id = c.customer_id
join olist_order_items oi 
    on o.order_id = oi.order_id
group by c.customer_id, o.order_id
order by c.customer_id, order_rank

-- 22 - For each product category, calculate the 3-day moving average of total daily sales (based on order_purchase_timestamp).

with daily_cat_sales as (
    select
        p.product_category_name,
        cast(o.order_purchase_timestamp as date) as sale_date,
        sum(oi.price + oi.freight_value) as daily_sales
    from olist_orders o
    join olist_order_items oi on o.order_id = oi.order_id
    join olist_products p on oi.product_id = p.product_id
    group by p.product_category_name, cast(o.order_purchase_timestamp as date)
)
select
    product_category_name,
    sale_date,
    daily_sales,
    cast(
      avg(cast(daily_sales as float)) 
      over (
        partition by product_category_name
        order by sale_date
        rows between 2 preceding and current row
      ) as decimal(18,2)
    ) as moving_avg_3day
from daily_cat_sales
order by product_category_name, sale_date


-- 23 - For each seller, find their first order date and last order date.

select 
  oi.seller_id, 
  MIN(o.order_purchase_timestamp) first_order_date,
  MAX(o.order_purchase_timestamp) last_order_date
from olist_order_items oi
 left join olist_orders o on o.order_id = oi.order_id
group by oi.seller_id

-- Show each order along with the cumulative total sales per customer up to that order date.

with order_sales AS 
(
select 
     o.order_id,
        o.customer_id,
        CAST(o.order_purchase_timestamp AS DATE) AS order_date,
        SUM(oi.price + oi.freight_value) AS order_amount
    FROM olist_orders o
    JOIN olist_order_items oi 
        ON o.order_id = oi.order_id
    GROUP BY o.order_id, o.customer_id, CAST(o.order_purchase_timestamp AS DATE)
) 
select 
 order_id,
 customer_id,
 sum(order_amount)
   over 
     (partition by customer_id order by order_date, order_id rows between unbounded preceding and current row) as cumulative_sales
from order_sales
ORDER BY customer_id, order_date, order_id;

-- 24 - Calculate the time gap (in days) between each customer’s consecutive purchases.

with cust_details as (
select
   customer_id,
   order_id,
   cast(order_purchase_timestamp as date) as order_date
from olist_orders 
group by customer_id, order_id, cast(order_purchase_timestamp as date) 
order by order_date desc
)
select 
  customer_id,
  order_id,
  order_date,
  lag(order_date) over (partition by customer_id order by order_date asc) as previous_order_date,
   DATEDIFF(
        DAY, 
        LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date), 
        order_date
    ) AS days_between_orders
from cust_details
ORDER BY customer_id, order_date

-------------------------------------------------------------------------------------------------------------------------------
-- 2. Joins -------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------
--25 - List all customers and their corresponding order IDs, including customers who never placed an order.

select 
 c.customer_id,
 o.order_id
 from olist_customers c
  left join olist_orders o 
   on c.customer_id = o.customer_id
  order by c.customer_id

-- Find all sellers who never sold any product in the eletrodomesticos category.

select 
  distinct s.seller_id
  from olist_sellers s
  left join olist_order_items oi on s.seller_id = oi.seller_id
  left join olist_products p on p.product_id = oi.product_id
  and p.product_category_name = 'eletrodomesticos'
where p.product_id is null


SELECT s.seller_id
FROM olist_sellers s
WHERE NOT EXISTS (
    SELECT seller_id
    FROM olist_order_items oi
    JOIN olist_products p ON oi.product_id = p.product_id
    WHERE oi.seller_id = s.seller_id
      AND p.product_category_name = 'eletrodomesticos'
);

-- 26 - For each order, show order ID, customer city, and seller city (requires joining customers, orders, order_items, and sellers).

SELECT 
    o.order_id,
    c.customer_city,
    s.seller_city
FROM olist_orders o
JOIN olist_customers c 
    ON o.customer_id = c.customer_id
JOIN olist_order_items oi 
    ON o.order_id = oi.order_id
JOIN olist_sellers s 
    ON oi.seller_id = s.seller_id
ORDER BY o.order_id;

-- 27 - List all products along with the number of orders they were included in, including products that were never sold.

SELECT 
    p.product_id,
    p.product_category_name,
    COUNT(DISTINCT oi.order_id) AS order_count
FROM olist_products p
LEFT JOIN olist_order_items oi 
    ON p.product_id = oi.product_id
GROUP BY p.product_id, p.product_category_name
ORDER BY order_count DESC;

-- 28 - Find the total freight value for each order by joining the order_items and orders tables.

SELECT 
    o.order_id,
    SUM(oi.freight_value) AS total_freight
FROM olist_orders o
JOIN olist_order_items oi 
    ON o.order_id = oi.order_id
GROUP BY o.order_id
ORDER BY o.order_id;

-- 29 -. CTE (Common Table Expressions) ------------------------------------------------------------------------------------------

-- Using a CTE, calculate the total sales per month, then find the month with the highest sales.

with total_sales as (
select 
   year(order_purchase_timestamp) as year,
   month(order_purchase_timestamp) as  month,
   sum(oi.price + oi.freight_value) as Sales
from olist_order_items oi join olist_orders o on o.order_id = oi.order_id
group by year(order_purchase_timestamp), month(order_purchase_timestamp)
),
rank as(
select 
  month,
  Sales,
  rank() over(partition by month order by Sales desc ) as rnk
from total_sales
) 
select month, Sales
from rank
 where rnk =1

-----------------------

 WITH MonthlySales AS (
    SELECT 
        YEAR(o.order_purchase_timestamp) AS sales_year,
        MONTH(o.order_purchase_timestamp) AS sales_month,
        sum(oi.price + oi.freight_value) AS total_sales
    FROM olist_orders o
    JOIN olist_order_items oi 
        ON o.order_id = oi.order_id
    GROUP BY YEAR(o.order_purchase_timestamp), MONTH(o.order_purchase_timestamp)
)
SELECT TOP 1 
    sales_year,
    sales_month,
    total_sales
FROM MonthlySales
ORDER BY total_sales DESC;

-- 30 - Create a CTE to find the average review score per product, then select the top 10 products with the best ratings.

with avg_review_score as(
select 
  oi.product_id,
  avg(cast(r.review_score as float)) as avg_score
from olist_order_items oi
	 join olist_order_reviews r on oi.order_id = r.order_id
group by oi.product_id
)
select top 10 
product_id,
avg_score
from avg_review_score
order by avg_score desc

-- Build a CTE that calculates sales per seller, then use it to find sellers earning above the average seller revenue.



-- 31 - Create a CTE to find customers whose order count is above the 90th percentile.

;WITH CustomerOrderCounts AS (
    SELECT 
        o.customer_id,
        COUNT(o.order_id) AS order_count
    FROM olist_orders o
    GROUP BY o.customer_id
),
PercentileCalc AS (
    SELECT DISTINCT 
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY order_count) 
        OVER () AS p90
    FROM CustomerOrderCounts
)
SELECT 
    c.customer_id,
    c.order_count
FROM CustomerOrderCounts c
CROSS JOIN PercentileCalc p
WHERE c.order_count > p.p90
ORDER BY c.order_count DESC;


-- 32 - Using a CTE, calculate average delivery time per seller, then rank sellers by speed.

with avg_del_time as(
select 
       oi.seller_id,
	   avg(datediff(day, o.order_purchase_timestamp, o.order_delivered_customer_date)) as avg_diff
	from olist_orders o 
	join olist_order_items oi on o.order_id = oi.order_id
	group by oi.seller_id
)
select  
   seller_id,
   avg_diff,
   RANK() OVER (ORDER BY avg_diff ASC) AS speed_rank
FROM avg_del_time
ORDER BY speed_rank;

-- 4. Subqueries ------------------------------------------------------------------------------------------------------------------


-- 33 -Find the order(s) with the maximum total payment value using a subquery.

select 
  p.order_id,
  sum(p.payment_value) as total_payment
from olist_order_payments p 
group by p.order_id
having sum(p.payment_value) = (
  select max(total_payment) 
   from (
         select sum(payment_value) as total_payment
		 from olist_order_payments 
		 group by order_id
  ) as order_totals
)

-- 34 - List all products whose sales are above the average sales per product.

select 
  product_id,
  sum(freight_value + price) as sales
from olist_order_items
group by product_id
having sum(freight_value + price) > (
    select avg(sales) 
	  from (
select 
  product_id,
  sum(freight_value + price) as sales
from olist_order_items
group by product_id
) as product_sales
)

-- 35 -Find sellers whose total number of orders is above the median seller order count.

WITH seller_order_count AS (
    SELECT 
        seller_id,
        COUNT(order_id) AS total_orders
    FROM olist_order_items
    GROUP BY seller_id
),
median_calc AS (
    SELECT 
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_orders) 
        OVER () AS median_val
    FROM seller_order_count
)
SELECT 
    soc.seller_id,
    soc.total_orders
FROM seller_order_count soc
CROSS JOIN median_calc mc
WHERE soc.total_orders > mc.median_val;

-- 36 -Find all customers who have spent more than the highest-spending customer from 2017.

-- Step 1: Find the max spending in 2017

with customer_spend as (
    select 
        c.customer_id,
        sum(p.payment_value) as total_spent
    from olist_customers c
    join olist_orders o 
        on c.customer_id = o.customer_id
    join olist_order_payments p 
        on o.order_id = p.order_id
    where year(o.order_purchase_timestamp) = 2017
    group by c.customer_id
),
max_2017 as (
    select max(total_spent) as max_spent_2017
    from customer_spend
),
all_time_spend as (
    select 
        c.customer_id,
        sum(p.payment_value) as total_spent
    from olist_customers c
    join olist_orders o 
        on c.customer_id = o.customer_id
    join olist_order_payments p 
        on o.order_id = p.order_id
    group by c.customer_id
)
-- step 2: get customers who beat that threshold
select ats.customer_id, ats.total_spent
from all_time_spend ats
join max_2017 m 
    on ats.total_spent > m.max_spent_2017
order by ats.total_spent desc


-- 5. Aggregates ------------------------------------------------------------------------------------------------------------------


-- 37 - Calculate the average order value (AOV) per year.

select 
 year(o.order_purchase_timestamp) as yr,
 sum(oi.freight_value + oi.price)*1.0 / count(distinct(o.order_id)) as AOV
from olist_order_items oi join olist_orders o on o.order_id = oi.order_id
group by year(o.order_purchase_timestamp)
order by AOV desc

-- 38 - For each state, find total sales, total orders, and average freight cost.

SELECT 
    c.customer_state,
    SUM(oi.price + oi.freight_value) AS total_sales,
    COUNT(DISTINCT o.order_id) AS total_orders,
    AVG(oi.freight_value) AS avg_freight_cost
FROM olist_customers c
JOIN olist_orders o 
    ON c.customer_id = o.customer_id
JOIN olist_order_items oi 
    ON o.order_id = oi.order_id
GROUP BY c.customer_state
ORDER BY total_sales DESC;

-- 39 - Count the number of distinct customers who ordered in both 2017 and 2018.

select count(distinct customer_id) as cust_in_both_yrs
from (
    select customer_id, year(order_purchase_timestamp) as order_years
	from olist_orders 
	where year(order_purchase_timestamp) in (2017, 2018)
	group by customer_id, year(order_purchase_timestamp)
) as yrs_orders
group by customer_id
having count(distinct order_years) = 2

-------------------------------------------------------------------------------------------

alter PROCEDURE GetCustomersInBothYears
    @Year1 INT,
    @Year2 INT
AS
BEGIN
    SELECT COUNT(DISTINCT customer_id) AS customers_in_both_years
    FROM (
        SELECT customer_id, YEAR(order_purchase_timestamp) AS order_year
        FROM olist_orders
        WHERE YEAR(order_purchase_timestamp) IN (@Year1, @Year2)
        GROUP BY customer_id, YEAR(order_purchase_timestamp)
    ) AS yearly_orders
    GROUP BY customer_id
    HAVING COUNT(DISTINCT order_year) = 2;
END

EXEC GetCustomersInBothYears @Year1 = 2017, @Year2 = 2016;

-- 40 - Find the average review score per month and the month with the lowest rating.

SELECT TOP 1 WITH TIES
    review_year,
    review_month,
    avg_review_score
FROM (
    SELECT 
        YEAR(review_creation_date) AS review_year,
        MONTH(review_creation_date) AS review_month,
        AVG(review_score) AS avg_review_score
    FROM olist_order_reviews
    GROUP BY YEAR(review_creation_date), MONTH(review_creation_date)
) AS MonthlyReviews
ORDER BY avg_review_score ASC;

-- 41 - Calculate the percentage of orders delivered late (order_delivered_customer_date > order_estimated_delivery_date) per year.

SELECT 
    YEAR(order_purchase_timestamp) AS order_year,
    COUNT(CASE WHEN order_delivered_customer_date > order_estimated_delivery_date THEN 1 END) * 100.0 
        / COUNT(*) AS late_delivery_percentage
FROM olist_orders
WHERE order_delivered_customer_date IS NOT NULL 
  AND order_estimated_delivery_date IS NOT NULL
GROUP BY YEAR(order_purchase_timestamp)
ORDER BY order_year;


-- 42 -. For each customer, calculate their average order value (AOV), then rank customers within their state by AOV. Show top 3 per state.

with CustomerAOV as (
select 
        c.customer_id,
        c.customer_state,
        sum(oi.price + oi.freight_value) * 1.0 / count(distinct o.order_id) as aov
    from olist_customers c
    join olist_orders o 
        on c.customer_id = o.customer_id
    join olist_order_items oi 
        on o.order_id = oi.order_id
    group by c.customer_id, c.customer_state
),
CustomerRank as (
select 
        customer_id,
        customer_state,
        aov,
        rank() over (partition by customer_state order by aov desc) as state_rank
    from CustomerAOV
)
select *
from CustomerRank
where state_rank <= 3
order by customer_state, state_rank

-- 43-. Find the product category that had the largest increase in sales (total payment value) between 2017 and 2018.

-- sales with product_category
-- year(2017, 2018)

with catyearsales as(
select 
   p.product_category_name,
   year(o.order_purchase_timestamp) as order_year,
   sum(oi.freight_value + oi.price) as sales
from olist_products p 
 join olist_order_items oi on p.product_id = oi.product_id
 join olist_orders o on o.order_id = oi.order_id
   where year(o.order_purchase_timestamp) in (2017, 2018)
group by product_category_name, year(o.order_purchase_timestamp)
),
cat_growth as(
 select 
   cat.product_category_name,
   isnull(sum(case when cat.order_year = 2017 then cat.sales end), 0) as sales_2017,
   isnull(sum(case when cat.order_year = 2018 then cat.sales end), 0) as sales_2018
from catyearsales cat
group by cat.product_category_name
),
catsales_diff as  (
select 
   product_category_name,
   sales_2017,
   sales_2018,
   (sales_2018 - sales_2017) as sales_increase
from cat_growth
)
select * from catsales_diff
order by sales_increase desc

-- 44 -. Using a CTE, calculate monthly sales per customer. 
-- Then, for each customer, find the month where they had their maximum spending.

with monthly_sales as (
  select  
    o.customer_id,
	format(o.order_purchase_timestamp, 'yyyy-mm') as order_month,
	sum(oi.freight_value + oi.price) as Sales
  from olist_orders o 
     join olist_order_items oi on o.order_id = oi.order_id
group by o.customer_id, format(o.order_purchase_timestamp, 'yyyy-mm') 
),
salesrank as(
select 
  m.customer_id,
  m.order_month,
  m.Sales,
  rank() over(partition by customer_id order by Sales desc) as rnk
from monthly_sales m
)
select customer_id, order_month, Sales
from salesrank
where rnk = 1
order by customer_id

-- 45 -. Find the 90th percentile delivery time (days) per product category, and list categories where this is greater than 10 days.

with deliverytimes as (
    select 
        p.product_category_name,
        datediff(day, o.order_purchase_timestamp, o.order_delivered_customer_date) as delivery_days
    from olist_orders o
    join olist_order_items oi 
        on o.order_id = oi.order_id
    join olist_products p 
        on oi.product_id = p.product_id
    where o.order_delivered_customer_date is not null
)
select 
    product_category_name,
    percentile_cont(0.9) within group (order by delivery_days) 
        over (partition by product_category_name) as p90_delivery_days
from deliverytimes
group by product_category_name
having percentile_cont(0.9) within group (order by delivery_days) 
        over (partition by product_category_name) > 10
order by p90_delivery_days desc;

-- 46 -. For each seller, calculate their average review score and the number of unique customers they served. Rank sellers by review score, breaking ties by customer count.

with sellerstats as (
    select 
        oi.seller_id,
        avg(r.review_score * 1.0) as avg_review_score,
        count(distinct o.customer_id) as unique_customers
    from olist_order_items oi
    join olist_orders o 
        on oi.order_id = o.order_id
    join olist_order_reviews r 
        on o.order_id = r.order_id
    group by oi.seller_id
)
select 
    seller_id,
    avg_review_score,
    unique_customers,
    rank() over (order by avg_review_score desc, unique_customers desc) as seller_rank
from sellerstats
order by seller_rank

-- 47 -. Identify the top 5 products in each product category based on total sales value. 

with productsales as (
select 
 p.product_category_name,
 p.product_id,
 sum(oi.price + oi.freight_value) as total_sales
    from olist_order_items oi
    join olist_products p 
        on oi.product_id = p.product_id
    group by p.product_category_name, p.product_id
),
rankedproducts as (
    select 
     product_category_name,
     product_id,
	 total_sales,
     row_number() over ( partition by product_category_name order by total_sales desc ) as sales_rank
    from productsales
)
select 
    product_category_name,
    product_id,
    total_sales,
    sales_rank
from rankedproducts
where sales_rank <= 5
order by product_category_name, sales_rank;

-- 48 -. For each order, calculate the previous order date and next order date of that customer. Then calculate the average gap in days between purchases.

with custorder_date as (
select 
    o.customer_id,
	o.order_id,
	o.order_purchase_timestamp,
	lag(o.order_purchase_timestamp) over (partition by customer_id order by order_purchase_timestamp asc) as prev_order_date,
	lead(o.order_purchase_timestamp) over (partition by customer_id order by order_purchase_timestamp asc) as next_order_date
from olist_orders o
),
ordergaps as (
    select 
        customer_id,
        order_id,
        order_purchase_timestamp,
        prev_order_date,
        next_order_date,
        datediff(day, prev_order_date, order_purchase_timestamp) as gap_from_prev,
        datediff(day, order_purchase_timestamp, next_order_date) as gap_to_next
    from custorder_date
)
select 
    customer_id,
    avg(gap_from_prev) as avg_gap_days
from ordergaps
where gap_from_prev is not null 
group by customer_id
order by avg_gap_days desc

-- For each seller, calculate their monthly sales trend and use a window function to flag months where sales dropped by more than 30% compared to the previous month.

with sellersales as(
select 
  oi.seller_id,
  sum(oi.freight_value + oi.price) as sales,
  format(o.order_purchase_timestamp, 'yyyy-mm') as sale_month
from olist_order_items oi 
   join olist_orders o on o.order_id = oi.order_id
   group by oi.seller_id, format(o.order_purchase_timestamp, 'yyyy-mm')
),
saleswithprev as (
  select 
   seller_id,
   sale_month,
   sales,
   lag(sales) over ( partition by seller_id order by sale_month) as prev_month_sales
  from sellersales
)
select 
    seller_id,
    sale_month,
    sales,
    prev_month_sales,
    case 
        when prev_month_sales is not null 
        and sales < prev_month_sales * 0.7 
        then 1 else 0 
    end as sales_dropped_flag
from saleswithprev
order by seller_id, sale_month


--- Find the product category that had the largest increase in sales (total payment value) between 2017 and 2018.

with catyearsales as(
select 
   p.product_category_name,
   year(o.order_purchase_timestamp) as order_year,
   sum(oi.freight_value + oi.price) as sales
from olist_products p 
 join olist_order_items oi on p.product_id = oi.product_id
 join olist_orders o on o.order_id = oi.order_id
   where year(o.order_purchase_timestamp) in (2017, 2018)
group by product_category_name, year(o.order_purchase_timestamp)
),
cat_growth as(
 select 
   cat.product_category_name,
   isnull(sum(case when cat.order_year = 2017 then cat.sales end), 0) as sales_2017,
   isnull(sum(case when cat.order_year = 2018 then cat.sales end), 0) as sales_2018
from catyearsales cat
group by cat.product_category_name
),
catsales_diff as  (
select 
   product_category_name,
   sales_2017,
   sales_2018,
   (sales_2018 - sales_2017) as sales_increase
from cat_growth
)
select * from catsales_diff
order by sales_increase desc
