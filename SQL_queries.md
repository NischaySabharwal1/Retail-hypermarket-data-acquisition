## • Data type of columns in a table

• JSON of the column data types copied from Bigquery table descriptions
• pd.json_normalize() function takes in a JSON file and converts it to clean data FRAME

## • Time period for which data is given


SELECT MIN(order_purchase_timestamp) as First_order_date,
       MAX(order_delivered_customer_date) as Last_delivery_date,
       ROUND(DATETIME_DIFF(MAX(order_delivered_customer_date),MIN(order_purchase_timestamp),HOUR)/(24*30),0) AS Time_period_months
FROM `***.orders` 


## • Cities and States covered in the dataset

-----------
SELECT DISTINCT geolocation_state, 
    COUNT(DISTINCT geolocation_city) as City_count
FROM `*.location*`
GROUP BY geolocation_state;
-----------

## • Is there a growing trend on e-commerce in Brazil? How can we describe a complete scenario? Can we see some seasonality with peaks at specific months?

-----------
WITH date_arranged AS (
  SELECT EXTRACT(YEAR FROM order_purchase_timestamp) as year,
       EXTRACT(MONTH FROM order_purchase_timestamp) as month
FROM `**.orders`
)
SELECT 
       year, month,
       COUNT(month) as orders_in_a_month
FROM date_arranged
GROUP BY year,month
ORDER BY year,month 
-----------

y2016 = time_based_ordercount.groupby(by = 'year').get_group(2016)
y2017 = time_based_ordercount.groupby(by = 'year').get_group(2017)
y2018 = time_based_ordercount.groupby(by = 'year').get_group(2018)
sns.lineplot(data = y2016, x = 'month' ,y = 'orders_in_a_month')
sns.lineplot(data = y2017, x = 'month' ,y = 'orders_in_a_month')
sns.lineplot(data = y2018, x = 'month' ,y = 'orders_in_a_month')
plt.legend(['2016','2017','2018'])
plt.show()

## • What time do Brazilian customers tend to buy (Dawn, Morning, Afternoon or Night)?

-----------
WITH hour_data as (
  SELECT 
  EXTRACT(HOUR FROM order_purchase_timestamp) as hour
  FROM `**.orders`), 
day_time as
(
  SELECT hour,
  CASE 
  WHEN hour BETWEEN 5 and 7
  THEN "Dawn"
  WHEN hour BETWEEN 8 and 12
  THEN "Morning" 
  WHEN hour BETWEEN 13 and 17
  THEN "Afternoon"
  WHEN hour BETWEEN 18 and 20
  THEN "Evening"
  WHEN hour<5 OR hour>20
  THEN "Night"
  END AS Time_of_day
  FROM hour_data
)
SELECT Time_of_day, 
       Count(*) as no_of_orders
FROM day_time 
GROUP BY Time_of_day
-----------

## Month on month orders by region, states

-----------
WITH pay_values as (
  SELECT EXTRACT(YEAR FROM ors.order_purchase_timestamp) as year,
         EXTRACT(MONTH FROM ors.order_purchase_timestamp) as month,
         pay.payment_value,
         c.customer_state
  FROM `**.orders` as ors
  JOIN `**.payments` as pay
  ON ors.order_id = pay.order_id
  JOIN `**.customers` as c
  ON ors.customer_id = c.customer_id
),monthly_values as (
SELECT customer_state,year, month,
       SUM(payment_value) as collected_sum,
       LAG(SUM(payment_value),1) OVER(ORDER BY customer_state,year,month) as prev_sum
FROM pay_values
GROUP BY customer_state,year,month
ORDER BY customer_state,year,month
)
SELECT *,
       ROUND(((collected_sum-prev_sum)/prev_sum),2)*100 as pct_growth
FROM monthly_values
-----------

## • Distribution of Customers in Brazil

-----------
SELECT customer_state,
       COUNT(customer_unique_id) as no_of_cust 
FROM `**.customers`
GROUP BY customer_state
ORDER BY no_of_cust DESC
-----------

## % increase in cost of orders from 2017 to 2018 (Jan-Aug)

-----------
WITH year_month AS (
  SELECT oi.product_id,
         EXTRACT(MONTH FROM o.order_purchase_timestamp) as month,
         EXTRACT(YEAR FROM o.order_purchase_timestamp) as year,
         oi.price
  FROM  `**.orders` as o
  JOIN `**.order_items` as oi
  ON o.order_id = oi.order_id
),price_sum AS
(
  SELECT month, year,
         AVG(price) as avg_product_price
  FROM year_month
  WHERE (year BETWEEN 2017 and 2018) and (month BETWEEN 1 and 8)
  GROUP BY month, year
  ORDER BY month, year
),
--SELECT * FROM price_sum
growth_calc as 
(
SELECT month, year, avg_product_price,
       LAG(avg_product_price, 1) OVER(ORDER BY month,year) as prev_avg
FROM price_sum
ORDER BY month,year
)
SELECT month, year, avg_product_price,
        CASE 
        WHEN year = 2018
        THEN ROUND((avg_product_price-prev_avg)/prev_avg,2)*100
        ELSE NULL
        END as pct_increase
FROM growth_calc
-----------

## • Mean & Sum of price and freight value by customer state

-----------
SELECT c.customer_state,
       SUM(oi.price) as price_sum,
       AVG(oi.price) as avg_price,
       SUM(oi.freight_value) as freight_value_sum,
       AVG(oi.freight_value) as freight_value_avg
FROM `**.customers` as c
LEFT JOIN `**.orders` as o
ON c.customer_id = o.customer_id
LEFT JOIN `**.order_items` as oi
ON oi.order_id = o.order_id
GROUP BY c.customer_state
ORDER BY price_sum DESC, freight_value_sum DESC
-----------

## • Days between purchasing, delivering and estimated delivery

-- time_to_delivery = order_delivered_customer_date-order_purchase_timestamp
-- diff_estimated_delivery = order_delivered_customer_date-order_estimated_delivery_date
-- estimated_delivery_time = order_estimated_delivery_date-order_purchase_timestamp

-----------
SELECT order_id,
       order_purchase_timestamp,
       order_delivered_customer_date,
       order_estimated_delivery_date,
       DATE_DIFF(order_delivered_customer_date,order_purchase_timestamp,DAY) as time_to_delivery,
       DATE_DIFF(order_delivered_customer_date, order_estimated_delivery_date, DAY) as diff_estimated_delivery,
       ROUND(DATE_DIFF(order_estimated_delivery_date,order_purchase_timestamp, HOUR)/24,2) as estimated_delivery_time
FROM `**.orders`
-----------

## • Grouping data by state, take mean of freight_value, time_to_delivery, diff_estimated_delivery

-----------
WITH delivery_times as (
  SELECT o.order_id,o.customer_id,
       AVG(DATE_DIFF(order_delivered_customer_date,order_purchase_timestamp,DAY)) as time_to_delivery,
       AVG(DATE_DIFF(order_delivered_customer_date, order_estimated_delivery_date, DAY)) as diff_estimated_delivery,
       SUM(oi.freight_value) as freight_value_sum
FROM `**.orders` as o
JOIN `**.order_items` as oi
ON o.order_id = oi.order_id
GROUP BY o.order_id,o.customer_id
) 
  SELECT c.customer_state,
         ROUND(AVG(dt.freight_value_sum),0) as mean_freight_value, 
         ROUND(AVG(dt.time_to_delivery),0) as mean_time_to_delivery,
         ROUND(AVG(dt.diff_estimated_delivery),0) as mean_diff_estimated_delivery
  FROM delivery_times as dt
  JOIN `**.customers` as c
  ON dt.customer_id = c.customer_id
  GROUP BY c.customer_state

-----------

## • Top 5 states with highest/lowest average freight value
### States with highest average freight value

-----------
SELECT customer_state as state,mean_freight_value
FROM `**.state_mean_deliverytime_freight_values`
ORDER BY mean_freight_value DESC 
LIMIT 5
-----------

### States with lowest average freight value

-----------
SELECT customer_state as state,mean_freight_value
FROM `**.state_mean_deliverytime_freight_values`
ORDER BY mean_freight_value ASC
LIMIT 5
-----------

## • Top 5 states with highest/lowest average time to deliver
### States with highest average time to delivery

-----------
SELECT customer_state as state,mean_time_to_delivery
FROM `**.state_mean_deliverytime_freight_values`
ORDER BY mean_time_to_delivery DESC
LIMIT 5
-----------

### States with lowest average time to delivery

-----------
SELECT customer_state as state,mean_time_to_delivery
FROM `**.state_mean_deliverytime_freight_values`
ORDER BY mean_time_to_delivery ASC
LIMIT 5
-----------

## • Top 5 states where delivery is really fast/ not so fast compared to estimated date
### States with Really fast delivery times compared to estimated time

-----------
SELECT customer_state as state, mean_diff_estimated_delivery
FROM `**.state_mean_deliverytime_freight_values`
ORDER BY mean_diff_estimated_delivery ASC
LIMIT 5
-----------

### States with not so fast delivery times compared to estimated time

-----------
SELECT customer_state as state, mean_diff_estimated_delivery
FROM `**.state_mean_deliverytime_freight_values`
ORDER BY mean_diff_estimated_delivery DESC
LIMIT 5
-----------

## • Month over Month count of orders for different payment types

-----------
WITH month_payment_type AS (
  SELECT EXTRACT(YEAR FROM o.order_purchase_timestamp) as year,
       EXTRACT(MONTH FROM o.order_purchase_timestamp) as month,
       pay.payment_type
FROM `**.orders` as o
JOIN `**.payments` as pay
ON pay.order_id = o.order_id
), order_counts as (
SELECT DISTINCT payment_type, month, COUNT(*) as order_count,
       LAG(COUNT(*),1) OVER(PARTITION BY payment_type ORDER BY payment_type,month) as prev_order_count
FROM month_payment_type
GROUP BY payment_type, month
ORDER BY payment_type, month
)
SELECT *,
      order_count-prev_order_count as month_over_month_growth
FROM order_counts
-----------

## • Distribution of payment installments and count of orders

-----------
WITH month_payment_type AS (
  SELECT EXTRACT(YEAR FROM o.order_purchase_timestamp) as year,
       EXTRACT(MONTH FROM o.order_purchase_timestamp) as month,
       pay.payment_installments
FROM `**.orders` as o
JOIN `**.payments` as pay
ON pay.order_id = o.order_id
), order_counts as (
SELECT DISTINCT payment_installments, month, COUNT(*) as order_count,
       LAG(COUNT(*),1) OVER(PARTITION BY payment_installments ORDER BY payment_installments,month) as prev_order_count
FROM month_payment_type
GROUP BY payment_installments, month
ORDER BY payment_installments, month
)
SELECT *,
      order_count-prev_order_count as month_over_month_growth
FROM order_counts
-----------

