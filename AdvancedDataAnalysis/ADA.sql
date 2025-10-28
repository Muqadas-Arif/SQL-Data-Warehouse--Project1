-- ==========================================================
-- Advanced Data Analytics for Data Warehouse (Sales Analysis)
-- Description: A collection of analytical SQL queries designed 
-- to explore sales trends, performance metrics, and customer behavior.
-- Dataset: gold.fact_sales and supporting dimension tables.
-- ==========================================================


-- Change Over Time (Yearly)
-- This query analyzes total sales, unique customers, and quantities sold per year.
-- It helps visualize annual growth and evaluate long-term sales performance trends.
_____________________________________________________________________________________________________________________________________________________________
select year(order_date) as order_year,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by year(order_date)
order by year(order_date);


-- Change Over Time (Monthly)
-- This query measures sales, customer count, and quantity by both month and year.
-- It provides detailed monthly insights to detect seasonality and recurring sales patterns.
_____________________________________________________________________________________________________________________________________________________________
select month(order_date) as order_month,
year(order_date) as order_year,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by month(order_date),year(order_date)
order by month(order_date),year(order_date);


-- Cumulative Analysis
-- This query calculates a running total of sales, aggregated monthly.
-- It shows how revenue accumulates over time, highlighting continuous business growth.
___________________________________________________________________________________________________________________________________________________________
select
order_date,
total_sales,
sum(total_sales) over (partition by order_date order by order_date) as running_total_sales
from(
select
datetrunc(month,order_date) as order_date,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by datetrunc(month,order_date)
)t;


-- Moving Average
-- This query computes yearly running totals and moving average prices.
-- It helps smooth out short-term variations to observe long-term performance trends.
___________________________________________________________________________________________________________________________________________________________
select
order_date,
total_sales,
sum(total_sales) over (order by order_date) as running_total_sales,
avg(avg_price) over (order by order_date) as moving_average_price
from(
select
datetrunc(year,order_date) as order_date,
sum(sales_amount) as total_sales,
avg(price) as avg_price
from gold.fact_sales
where order_date is not null
group by datetrunc(year,order_date)
)t;


-- Performance Analysis (Current vs Target)
-- This query compares each product’s yearly sales to its average and previous year.
-- It identifies above-average and below-average performers, offering insights into product-level performance trends.
__________________________________________________________________________________________________________________________________________________________
with yearly_product_sales as (
select
year(f.order_date) as order_year,
p.product_name,
sum(f.sales_amount) as current_sales
from gold.fact_sales f
left join gold.dim_products p
on f.product_key=p.product_key
where f.order_date is not null
group by
year(f.order_date),
p.product_name
)
________________________________________________
select
order_year,
product_name,
current_sales,
avg(current_sales) over (partition by product_name) as avg_sales,
current_sales - avg(current_sales) over (partition by product_name) as fii_avg,
case when current_sales -avg(current_sales) over (partition by product_name)>0 then 'above average'
     when current_sales - avg(current_sales) over (partition by product_name)<0 then 'Below average'
	 else 'Avg'
	 end avg_sales,
lag(current_sales) over(partition by product_name order by order_year) as py_sales,
current_sales - lag(current_sales) over(partition by product_name order by order_year) as diff_py
from yearly_product_sales
order by product_name,order_year;


-- Part-to-Whole (Proportional Analysis)
-- This query calculates the percentage contribution of each product category to overall sales.
-- It helps identify top-performing categories and evaluate product mix efficiency.
_______________________________________________________________________________________________________________________________________________________
with category_sales as (
 select 
 category,
 sum(sales_amount) total_sales
 from gold.fact_sales f
 left join gold.dim_products p
 on p.product_key= f.product_key
 group by category)
 select
 category,
 total_sales,sum(total_sales) over () overall_sales,
 concat(round((cast(total_sales as float)/sum(total_sales) over ())*100,2),'%') as percentage_of_sales
 from category_sales
 order by total_sales desc;


-- Data Segmentation (By Cost Range)
-- This query groups products into cost-based segments such as ‘Below 100’, ‘100–500’, etc.
-- It provides insights into pricing structure and product distribution across cost tiers.
_____________________________________________________________________________________________________________________________________________________________
with product_segments as (
 select
 product_key,
 product_name,
 cost,
 case when cost < 100 then 'Below 100'
      when cost between 100 and 500 then '100-500'
	  when cost between 500 and 1000 then '500-1000'
	  else 'Above 1000'
end cost_range
from gold.dim_products
	  )
select 
cost_range,
count(product_key) as total_products
from product_segments
group by cost_range 
order by total_products desc;


-- Customer Segmentation (By Spending and Lifespan)
-- This query classifies customers into groups like ‘VIP’, ‘Regular’, and ‘New’.
-- It combines total spending and active lifespan to reveal customer value tiers and loyalty behavior.
______________________________________________________________________________________________________________________________________________________________
with customer_spending as (
select
c.customer_key,
sum(f.sales_amount) as total_spending,
min(order_date) as first_date,
max(order_date) as last_order,
datediff(month,min(order_date),max(order_date)) as lifespan
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key=c.customer_key
group by c.customer_key
)
 select 
 customer_segment,
count(customer_key)as total_customers
from(
select
customer_key,
 case when lifespan >=12 and total_spending > 5000 then 'VIP'
      when lifespan >=12 and total_spending <= 5000 then 'Regular'
	  else 'New'
	  end customer_segment
from customer_spending )t
group by customer_segment
order by total_customers desc;
