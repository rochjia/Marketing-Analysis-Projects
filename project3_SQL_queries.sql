# Filter the products that accounts for top 80% sales
# Save as trans_tmp
CREATE TABLE mkt.trans_tmp AS(
WITH sales_by_product AS (
  SELECT prod_id, SUM(sales_amt) AS total_sales,
  RANK() OVER (ORDER BY SUM(sales_amt) DESC) AS sales_rank
  FROM `mkt.transactions`
  GROUP BY prod_id
), 
sales_percentage AS (
  SELECT prod_id, total_sales, total_sales / SUM(total_sales) OVER () AS perc
  FROM sales_by_product
  WHERE sales_rank <= (
    SELECT COUNT(DISTINCT prod_id) FROM sales_by_product) * 0.8
),
top_products AS (
  SELECT prod_id FROM sales_percentage
  WHERE perc >= 0.8
  )
SELECT t.* FROM `mkt.transactions` t
JOIN top_products tp ON t.prod_id = tp.prod_id);

# Positive sales
# Excluding Extreme Values
# Save as trans_tmp_1
CREATE TABLE mkt.trans_tmp_1 AS(
WITH trans_pos AS (
  SELECT * FROM `mkt.trans_tmp`
  WHERE sales_amt > 0),
data_stat AS (
  SELECT prod_id, AVG(sales_amt/sales_qty) as mean_price, STDDEV(sales_amt/sales_qty) as stddev_price
  FROM `mkt.trans_tmp`
  GROUP BY prod_id
)
SELECT t.* FROM trans_pos t
JOIN data_stat d
ON t.prod_id = d.prod_id
WHERE sales_qty > 0 AND
mean_price != 0 AND
stddev_price != 0 AND
ABS(t.sales_amt/t.sales_qty - mean_price) / stddev_price <= 3);

# Exclude products that are not sold within 6 months
CREATE TABLE mkt.trans_tmp_2 AS (
SELECT * FROM `mkt.trans_tmp_1`
WHERE prod_id IN (
  SELECT DISTINCT prod_id FROM `mkt.transactions`
  WHERE trans_dt >= '2020-06-01'
));
# Save as trans_tmp_2

# Filter the products that account for top 50% sales
# sales_by_product
SELECT prod_id, SUM(sales_amt) AS total_sales
FROM `mkt.transactions`
GROUP BY prod_id
ORDER BY total_sales DESC;

# sales_percentage
SELECT prod_id, perc, SUM(perc) OVER (ORDER BY perc DESC) AS cumulative_perc
FROM (SELECT prod_id, total_sales / SUM(total_sales) OVER() as perc FROM `mkt.sales_by_product`
);

# prod_filter_1
SELECT prod_id FROM `mkt.sales_percentage`
WHERE cumulative_perc <= 0.5;

# Exclude products that are not sold within 6 months
# prod_filter_2
SELECT DISTINCT prod_id FROM `mkt.transactions`
WHERE trans_dt >= '2020-06-01';

# prod_filtered
# Get the intersected prod_id list
SELECT prod_id FROM `mkt.prod_filter_1`
INTERSECT DISTINCT
SELECT prod_id FROM `mkt.prod_filter_2`;

# trans_tmp
# Join the transaction table
# Only include the positive sales
SELECT * FROM `mkt.transactions`
WHERE prod_id IN (SELECT prod_id FROM `mkt.prod_filtered`) 
AND sales_amt > 0;

# prod_stat
SELECT prod_id, AVG(sales_amt/CAST(sales_qty AS NUMERIC)) AS mean_price, STDDEV(sales_amt/CAST(sales_qty AS NUMERIC)) AS stddev_price
FROM `mkt.trans_tmp`
WHERE sales_qty IS NOT NULL AND CAST(sales_qty AS NUMERIC) != 0
GROUP BY prod_id;

# trans_proj3
CREATE TABLE mkt.trans_proj3 AS (
SELECT t.* FROM `mkt.trans_tmp` t
JOIN `mkt.prod_stat` p ON t.prod_id = p.prod_id
WHERE mean_price != 0 AND
stddev_price != 0 AND
t.sales_qty IS NOT NULL AND
CAST(t.sales_qty AS NUMERIC) != 0 AND
ABS(t.sales_amt/CAST(t.sales_qty AS NUMERIC) - mean_price) / stddev_price <= 3);


WITH weekly_data AS (
  SELECT 
    prod_id,
    EXTRACT(WEEK FROM trans_dt) AS week_num,
    AVG(sales_amt / IF(sales_wgt = 0, sales_qty, sales_qty*sales_wgt)) AS avg_price,
    SUM(IF(sales_wgt = 0, sales_qty, sales_qty*sales_wgt)) AS total_demand
  FROM `mkt.trans_proj3`
  WHERE trans_dt BETWEEN '2019-01-01' AND '2019-12-31'
  GROUP BY prod_id, week_num
),
log_transformed_data AS (
  SELECT 
    prod_id,
    LOG(avg_price) AS log_price,
    LOG(total_demand) AS log_demand
  FROM weekly_data
),
aggregated_data AS (
  SELECT
    prod_id,
    COUNT(*) AS n,
    SUM(log_price) AS sum_log_price,
    SUM(log_demand) AS sum_log_demand,
    SUM(log_price * log_demand) AS sum_log_price_log_demand,
    SUM(log_price * log_price) AS sum_log_price_squared
  FROM log_transformed_data
  GROUP BY prod_id
)
SELECT
  prod_id,
  (n * sum_log_price_log_demand - sum_log_price * sum_log_demand) / (n * sum_log_price_squared - sum_log_price * sum_log_price) AS beta
FROM aggregated_data
WHERE n * sum_log_price_squared - sum_log_price * sum_log_price != 0
ORDER BY beta DESC;
# Save as prod_elasticity

# Select 200 inelastic products
SELECT prod_id, beta FROM `mkt.prod_elasticity`
WHERE beta < 0 AND beta > -1
ORDER BY beta DESC
LIMIT 200;
# Save as prod_inelastic

WITH
  quartiles AS (
    SELECT
      PERCENTILE_CONT(beta, 0.25) OVER() AS Q1,
      PERCENTILE_CONT(beta, 0.75) OVER() AS Q3
    FROM
      `mkt.prod_elasticity`
    LIMIT 1
  ),
  bounds AS (
    SELECT
      Q1 AS lower_bound,
      Q3 AS upper_bound
    FROM
      quartiles
  ),
without_outlier AS (SELECT
  e.prod_id,
  e.beta
FROM
  `mkt.prod_elasticity` e
CROSS JOIN
  bounds b
WHERE
  e.beta BETWEEN b.lower_bound AND b.upper_bound)


# Select 40 elastic products
SELECT prod_id, beta FROM without_outlier
WHERE beta < -1
ORDER BY beta
LIMIT 40;
# Save as prod_elastic

# Save as max_price
WITH CTE AS (SELECT prod_id, EXTRACT(WEEK FROM trans_dt) AS week_num, sales_amt, sales_qty,
ROUND(sales_amt / (IF(sales_wgt = 0, sales_qty, sales_qty*sales_wgt)),2) AS sales_unit_price FROM `capstone2023-homedepot.mkt.trans_proj3`
WHERE trans_dt BETWEEN '2019-01-01' AND '2019-12-31'
ORDER BY prod_id,week_num)

SELECT prod_id, MAX(sales_unit_price) AS max_unit_price, ROUND(0.7 * MAX(sales_unit_price),2) AS threshold FROM CTE
GROUP BY prod_id;

# Save as promo
WITH CTE AS (SELECT prod_id, EXTRACT(WEEK FROM trans_dt) AS week_num, sales_amt, sales_qty,
ROUND(sales_amt / (IF(sales_wgt = 0, sales_qty, sales_qty*sales_wgt)),2) AS sales_unit_price FROM `capstone2023-homedepot.mkt.trans_proj3`
WHERE trans_dt BETWEEN '2019-01-01' AND '2019-12-31'
ORDER BY prod_id,week_num)

SELECT CTE.prod_id, week_num, (AVG(ROUND(IF(1 - (sales_unit_price / threshold) < 0, 0, 1 - (sales_unit_price / threshold)),2)) + 0.01) AS promo_discount 
FROM CTE LEFT JOIN `mkt.max_price` t1
ON CTE.prod_id = t1.prod_id
WHERE t1.prod_id IN (SELECT prod_id FROM `mkt.prod_inelastic`) OR t1.prod_id IN (SELECT prod_id FROM `mkt.prod_elastic`)
GROUP BY CTE.prod_id, week_num
ORDER BY CTE.prod_id, week_num;

# Save as affected_seasonality
with affected as(
WITH
  quartiles AS (
    SELECT
      PERCENTILE_CONT(sv, 0.25) OVER() AS Q1,
      PERCENTILE_CONT(sv, 0.75) OVER() AS Q3
    FROM
      `mkt.seasonality`
    LIMIT 1
  ),
  bounds AS (
    SELECT
      Q1 - 1.5 * (Q3 - Q1) AS lower_bound,
      Q3 + 1.5 * (Q3 - Q1) AS upper_bound
    FROM
      quartiles
  ),
upper_q AS (SELECT
  e.prod_id,
  e.sv
FROM
  `mkt.seasonality` e
CROSS JOIN
  bounds b
WHERE
  e.sv > b.upper_bound)
SELECT prod_id, sv FROM upper_q)
select prod_id, log(sv) log_sv from affected a order by log(sv);

-- TABLE WITH PROMOTION PERCENTAGE --
WITH CTE AS (SELECT DATE_TRUNC(trans_dt, WEEK) AS week_start_date, store_id, prod_id, sales_amt, sales_qty, IF(sales_wgt = 0, sales_qty, sales_qty*sales_wgt) AS actual_volume,
ROUND(sales_amt / (IF(sales_wgt = 0, sales_qty, sales_qty*sales_wgt)),2) AS sales_unit_price FROM `capstone2023-homedepot.mkt.trans_proj3`
WHERE sales_amt >0 AND sales_qty >0
ORDER BY week_start_date, prod_id)

SELECT *, ROUND(1- (sales_unit_price / max_price),2) AS promotion_percentage FROM (
(SELECT prod_id, MAX(sales_unit_price) AS max_price
FROM CTE
GROUP BY prod_id) t1
JOIN CTE t2
ON  t1.prod_id = t2.prod_id)
ORDER BY t2.prod_id;


-- TABLE WITH SUBSTITUTE PRICE --
WITH CTE AS (SELECT prod_subcategory, ROUND(AVG(sales_unit_price),2) AS avg_price FROM 
(SELECT *, IF(sales_wgt = 0, sales_qty, sales_qty*sales_wgt) AS actual_volume,
ROUND(sales_amt / (IF(sales_wgt = 0, sales_qty, sales_qty*sales_wgt)),2) AS sales_unit_price FROM `capstone2023-homedepot.mkt.trans_proj3` t1
LEFT JOIN `capstone2023-homedepot.mkt.products` t2
ON t1.prod_id = t2.prod_id)
GROUP BY prod_subcategory)

SELECT * FROM (SELECT * FROM `capstone2023-homedepot.mkt.trans_proj3` t1
LEFT JOIN `capstone2023-homedepot.mkt.products` t2 ON t1.prod_id = t2.prod_id) t_all
LEFT JOIN CTE t3 ON t_all.prod_subcategory = t3.prod_subcategory; 

# Substitute
SELECT product, substitute_or_complement AS substitute
FROM `mkt.prod_relation`
WHERE relation = "substitute" AND
product IN (
  SELECT DISTINCT prod_id FROM `mkt.trans_proj3`
)
ORDER BY product;

# Complement
SELECT product, substitute_or_complement AS complement
FROM `mkt.prod_relation`
WHERE relation = "complement" AND
product IN (
  SELECT DISTINCT prod_id FROM `mkt.trans_proj3`
);

# Save as sub_filtered
SELECT product AS prod_id, substitute FROM `mkt.substitute`
WHERE product IN (SELECT prod_id FROM `mkt.prod_elastic`) OR product IN (SELECT prod_id FROM `mkt.prod_inelastic`);

# Save as com_filtered
SELECT product AS prod_id, complement FROM `mkt.complement`
WHERE product IN (SELECT prod_id FROM `mkt.prod_elastic`) OR product IN (SELECT prod_id FROM `mkt.prod_inelastic`);

# Save as prod_test
WITH weekly_data AS (
  SELECT 
    prod_id,
    EXTRACT(WEEK FROM trans_dt) AS week_num,
    AVG(sales_amt / IF(sales_wgt = 0, sales_qty, sales_qty * sales_wgt)) AS avg_price,
    SUM(IF(sales_wgt = 0, sales_qty, sales_qty * sales_wgt)) AS total_demand
  FROM `mkt.transactions`
  WHERE trans_dt BETWEEN '2019-01-01' AND '2019-12-31' AND
  sales_amt > 0 AND sales_qty > 0
  GROUP BY prod_id, week_num
),
sub_com_prices AS (
  SELECT
    w.prod_id,
    w.week_num,
    COALESCE(LOG(AVG(IFNULL(st.avg_price, 0))), 0) AS sub_log_prices,
    COALESCE(LOG(AVG(IFNULL(ct.avg_price, 0))), 0) AS com_log_prices
  FROM (
    SELECT 
      prod_id,
      week_num
    FROM weekly_data
    WHERE prod_id IN (SELECT prod_id FROM `mkt.prod_elastic`) OR prod_id IN (SELECT prod_id FROM `mkt.prod_inelastic`)
  ) w
  JOIN `mkt.sub_filtered` sf ON w.prod_id = sf.prod_id
  JOIN weekly_data st ON st.prod_id = sf.substitute
  JOIN `mkt.com_filtered` cf ON w.prod_id = cf.prod_id
  JOIN weekly_data ct ON ct.prod_id = cf.complement
  WHERE w.week_num = st.week_num AND w.week_num = ct.week_num
  GROUP BY w.prod_id, w.week_num
)
SELECT
  o.prod_id,
  o.week_num,
  LOG(o.avg_price) AS log_price,
  LOG(o.total_demand) AS log_demand,
  scp.sub_log_prices,
  scp.com_log_prices
FROM (
  SELECT *
  FROM weekly_data
  WHERE prod_id IN (SELECT prod_id FROM `mkt.prod_elastic`) OR prod_id IN (SELECT prod_id FROM `mkt.prod_inelastic`)
) o
JOIN sub_com_prices scp ON o.prod_id = scp.prod_id AND o.week_num = scp.week_num;

# Save as sub_filtered in mkt2
SELECT product AS prod_id, substitute FROM `mkt.substitute`
WHERE product IN (SELECT prod_id FROM `mkt.prod_price_reduction`) OR product IN (SELECT prod_id FROM `mkt.prod_price_increase`);

# Save as com_filtered in mkt2
SELECT product AS prod_id, complement FROM `mkt.complement`
WHERE product IN (SELECT prod_id FROM `mkt.prod_price_reduction`) OR product IN (SELECT prod_id FROM `mkt.prod_price_increase`);

# Save as prod_test
WITH weekly_data AS (
  SELECT 
    prod_id,
    store_id,
    EXTRACT(WEEK FROM trans_dt) AS week_num,
    AVG(sales_amt / IF(sales_wgt = 0, sales_qty, sales_qty * sales_wgt)) AS avg_price,
    SUM(IF(sales_wgt = 0, sales_qty, sales_qty * sales_wgt)) AS total_demand
  FROM `mkt.transactions`
  WHERE trans_dt BETWEEN '2019-01-01' AND '2019-12-31' AND
  sales_amt > 0 AND sales_qty > 0
  GROUP BY prod_id, store_id, week_num
),
sub_com_prices AS (
  SELECT
    w.prod_id,
    w.store_id,
    w.week_num,
    COALESCE(LOG(AVG(IFNULL(st.avg_price, 0))), 0) AS sub_log_prices,
    COALESCE(LOG(AVG(IFNULL(ct.avg_price, 0))), 0) AS com_log_prices
  FROM (
    SELECT 
      prod_id,
      store_id,
      week_num
    FROM weekly_data
    WHERE prod_id IN (SELECT prod_id FROM `mkt.prod_price_reduction`) OR prod_id IN (SELECT prod_id FROM `mkt.prod_price_increase`)
  ) w
  JOIN `mkt2.sub_filtered` sf ON w.prod_id = sf.prod_id
  JOIN weekly_data st ON st.prod_id = sf.substitute
  JOIN `mkt2.com_filtered` cf ON w.prod_id = cf.prod_id
  JOIN weekly_data ct ON ct.prod_id = cf.complement
  WHERE w.week_num = st.week_num AND w.week_num = ct.week_num AND w.store_id = st.store_id AND w.store_id = ct.store_id
  GROUP BY w.prod_id, w.store_id, w.week_num
)
SELECT
  o.prod_id,
  o.store_id,
  o.week_num,
  LOG(o.avg_price) AS log_price,
  LOG(o.total_demand) AS log_demand,
  scp.sub_log_prices,
  scp.com_log_prices
FROM (
  SELECT *
  FROM weekly_data
  WHERE prod_id IN (SELECT prod_id FROM `mkt.prod_price_reduction`) OR prod_id IN (SELECT prod_id FROM `mkt.prod_price_increase`)
) o
JOIN sub_com_prices scp ON o.prod_id = scp.prod_id AND o.store_id = scp.store_id AND o.week_num = scp.week_num;

# Save as max_price
WITH CTE AS (SELECT prod_id, store_id, EXTRACT(WEEK FROM trans_dt) AS week_num, sales_amt, sales_qty,
ROUND(sales_amt / (IF(sales_wgt = 0, sales_qty, sales_qty*sales_wgt)),2) AS sales_unit_price FROM `capstone2023-homedepot.mkt.trans_proj3`
WHERE trans_dt BETWEEN '2019-01-01' AND '2019-12-31'
ORDER BY prod_id, week_num)

SELECT prod_id, store_id, MAX(sales_unit_price) AS max_unit_price, ROUND(0.7 * MAX(sales_unit_price),2) AS threshold FROM CTE
GROUP BY prod_id, store_id;

# Save as promo
WITH CTE AS (SELECT prod_id, store_id, EXTRACT(WEEK FROM trans_dt) AS week_num, sales_amt, sales_qty,
ROUND(sales_amt / (IF(sales_wgt = 0, sales_qty, sales_qty*sales_wgt)),2) AS sales_unit_price FROM `capstone2023-homedepot.mkt.trans_proj3`
WHERE trans_dt BETWEEN '2019-01-01' AND '2019-12-31'
ORDER BY prod_id, store_id, week_num)

SELECT CTE.prod_id, CTE.store_id, week_num, (AVG(ROUND(IF(1 - (sales_unit_price / threshold) < 0, 0, 1 - (sales_unit_price / threshold)),2)) + 0.01) AS promo_discount 
FROM CTE LEFT JOIN `mkt2.max_price` t1
ON CTE.prod_id = t1.prod_id
WHERE t1.prod_id IN (SELECT prod_id FROM `mkt.prod_price_reduction`) OR t1.prod_id IN (SELECT prod_id FROM `mkt.prod_price_increase`)
GROUP BY CTE.prod_id, CTE.store_id, CTE.week_num
ORDER BY CTE.prod_id, CTE.store_id, CTE.week_num;

# Save as seasonality
with agg_season_data as(
with season_data as(
WITH weekly_data AS (
  SELECT 
    t.prod_id, t.store_id,
    EXTRACT(WEEK FROM trans_dt) AS week_num,
    AVG(sales_amt / IF(sales_wgt = 0, sales_qty, sales_qty*sales_wgt)) AS avg_price,
    SUM(IF(sales_wgt = 0, sales_qty, sales_qty*sales_wgt)) AS total_demand
  FROM mkt.transactions t inner join
  `mkt.products` p on
  t.prod_id = p.prod_id
  WHERE trans_dt BETWEEN '2019-01-01' AND '2019-12-31' and (sales_qty>0 or sales_wgt>0)
  GROUP BY prod_id, store_id, week_num
)
select (case when week_num >=0 and week_num <= 13
                then 'spring'
            when week_num >13 and week_num <= 26
                then 'summer'
            when week_num >26 and week_num <=39
                then 'fall'
            when week_num >39
                then 'winter' end) as season,* from weekly_data)
select prod_id, store_id, season,sum(total_demand) as agg_demand from season_data group by prod_id, store_id, season)
select prod_id, store_id, var_pop(agg_demand) as sv from agg_season_data group by prod_id, store_id;

# Save as affected_seasonality
with affected as(
WITH
  quartiles AS (
    SELECT
      PERCENTILE_CONT(sv, 0.25) OVER() AS Q1,
      PERCENTILE_CONT(sv, 0.75) OVER() AS Q3
    FROM
      `mkt.seasonality`
    LIMIT 1
  ),
  bounds AS (
    SELECT
      Q1 - 1.5 * (Q3 - Q1) AS lower_bound,
      Q3 + 1.5 * (Q3 - Q1) AS upper_bound
    FROM
      quartiles
  ),
upper_q AS (SELECT
  e.prod_id,
  e.store_id,
  e.sv
FROM
  `mkt2.seasonality` e
CROSS JOIN
  bounds b
WHERE
  e.sv > b.upper_bound)
SELECT prod_id, store_id, sv FROM upper_q)
select prod_id, store_id, log(sv) log_sv from affected a order by log(sv);

# Combine the work
# Save as pricing_data
SELECT a.prod_id, a.store_id, a.week_num, log_demand, log_price, sub_log_prices, com_log_prices, log_sv, log(promo_discount) log_promo FROM `mkt2.prod_test` a JOIN
`mkt2.affected_seasonality` b ON a.prod_id = b.prod_id AND a.store_id = b.store_id
JOIN `mkt2.promo` c ON a.prod_id = c.prod_id AND a.store_id = c.store_id AND a.week_num = c.week_num;

# Save as pricing_data2
SELECT prod_id, store_id, week_num, log_demand, EXP(log_price)/EXP(log_demand) AS price_div_demand, EXP(sub_log_prices) AS sub_price, EXP(com_log_prices) AS com_prices, log_sv, EXP(log_promo)-0.01 AS promo_perc
FROM mkt2.pricing_data;

# Check the range of price/demand
SELECT price_div_demand FROM mkt2.pricing_data2 
ORDER BY price_div_demand DESC;

# Save as optimal_compare_avg_store (weekly basis)
WITH weekly_data AS (
  SELECT 
    prod_id, store_id,
    AVG(sales_amt / IF(sales_wgt = 0, sales_qty, sales_qty*sales_wgt)) AS avg_price,
    MAX(sales_amt / IF(sales_wgt = 0, sales_qty, sales_qty*sales_wgt)) AS max_price,
    SUM(IF(sales_wgt = 0, sales_qty, sales_qty*sales_wgt)) AS total_demand,
    SUM(sales_amt) AS total_revenue
  FROM `mkt.trans_proj3`
  WHERE trans_dt BETWEEN '2019-01-01' AND '2019-12-31'
  GROUP BY prod_id, store_id)
SELECT a.prod_id, a.store_id, optimal_price, avg_price, max_price, optimal_demand, total_demand/53 AS avg_demand, optimal_revenue, total_revenue/53 AS avg_revenue 
FROM `mkt2.optimal_store` a JOIN weekly_data b ON a.prod_id = b.prod_id AND a.store_id = b.store_id;

# Save as prod_price_increase_store
SELECT * FROM `mkt2.optimal_compare_avg_store`
WHERE prod_id IN (SELECT prod_id FROM `mkt.prod_price_increase`)
ORDER BY prod_id, store_id;

# Save as prod_price_reduction_store
SELECT * FROM `mkt2.optimal_compare_avg_store`
WHERE prod_id IN (SELECT prod_id FROM `mkt.prod_price_reduction`)
ORDER BY prod_id, store_id;

# Save as optimal_compare_avg (weekly basis)
WITH weekly_data AS (
  SELECT 
    prod_id,
    AVG(sales_amt / IF(sales_wgt = 0, sales_qty, sales_qty*sales_wgt)) AS avg_price,
    MAX(sales_amt / IF(sales_wgt = 0, sales_qty, sales_qty*sales_wgt)) AS max_price,
    SUM(IF(sales_wgt = 0, sales_qty, sales_qty*sales_wgt)) AS total_demand,
    SUM(sales_amt) AS total_revenue
  FROM `mkt.trans_proj3`
  WHERE trans_dt BETWEEN '2019-01-01' AND '2019-12-31'
  GROUP BY prod_id)
SELECT a.prod_id, optimal_price, avg_price, max_price, optimal_demand, total_demand/53 AS avg_demand, optimal_revenue, total_revenue/53 AS avg_revenue FROM `capstone2023-homedepot.mkt.optimal` a JOIN weekly_data b ON a.prod_id = b.prod_id;

# Save as prod_price_increase
WITH prod_inelastic_filtered AS (
SELECT *
FROM `mkt.optimal_compare_avg`
WHERE prod_id IN (SELECT prod_id FROM `mkt.prod_inelastic`) AND
optimal_price/avg_price <= 2 AND optimal_price/avg_price > 1 AND
avg_demand/optimal_demand <= 2 AND avg_demand/optimal_demand > 1 AND
optimal_revenue/avg_revenue > 1
ORDER BY optimal_revenue/avg_revenue DESC
)

SELECT a.prod_id, prod_desc, optimal_price, avg_price, FORMAT('%.2f%%', (optimal_revenue/avg_revenue - 1) * 100) AS rev_increase_perc 
FROM prod_inelastic_filtered a LEFT JOIN mkt.products b ON a.prod_id = b.prod_id
ORDER BY optimal_revenue/avg_revenue DESC
LIMIT 50;

# Save as prod_price_reduction
WITH prod_elastic_filtered AS (
SELECT *
FROM `mkt.optimal_compare_avg`
WHERE prod_id IN (SELECT prod_id FROM `mkt.prod_elastic`) AND
#avg_price/optimal_price <= 2 AND 
#max_price/optimal_price > 1 AND
#optimal_demand/avg_demand <= 2 AND optimal_demand/avg_demand > 1 AND
optimal_revenue/avg_revenue > 1
ORDER BY optimal_price/avg_price, optimal_revenue/avg_revenue DESC
)
SELECT a.prod_id, prod_desc, optimal_price, avg_price, FORMAT('%.2f%%', (optimal_revenue/avg_revenue - 1) * 100) AS rev_increase_perc 
FROM prod_elastic_filtered a LEFT JOIN mkt.products b ON a.prod_id = b.prod_id
ORDER BY optimal_price/avg_price, optimal_revenue/avg_revenue DESC
LIMIT 10;

SELECT a.prod_id, optimal_price, max_price FROM `mkt.prod_price_reduction` a JOIN (
  SELECT prod_id, MAX(sales_amt	 / IF(sales_wgt = 0, sales_qty, sales_qty*sales_wgt)) AS max_price
  FROM `mkt.trans_proj3` GROUP BY prod_id
) b ON a.prod_id = b.prod_id;

# Save as optimal_compare_avg_store (weekly basis)
WITH weekly_data AS (
  SELECT 
    prod_id, store_id,
    AVG(sales_amt / IF(sales_wgt = 0, sales_qty, sales_qty*sales_wgt)) AS avg_price,
    MAX(sales_amt / IF(sales_wgt = 0, sales_qty, sales_qty*sales_wgt)) AS max_price,
    SUM(IF(sales_wgt = 0, sales_qty, sales_qty*sales_wgt)) AS total_demand,
    SUM(sales_amt) AS total_revenue
  FROM `mkt.trans_proj3`
  WHERE trans_dt BETWEEN '2019-01-01' AND '2019-12-31'
  GROUP BY prod_id, store_id)
SELECT a.prod_id, a.store_id, optimal_price, avg_price, max_price, optimal_demand, total_demand/53 AS avg_demand, optimal_revenue, total_revenue/53 AS avg_revenue 
FROM `mkt2.optimal_store` a JOIN weekly_data b ON a.prod_id = b.prod_id AND a.store_id = b.store_id;

# Save as prod_price_increase_store
SELECT * FROM `mkt2.optimal_compare_avg_store`
WHERE prod_id IN (SELECT prod_id FROM `mkt.prod_price_increase`)
ORDER BY prod_id, store_id;

# Save as prod_price_reduction_store
SELECT * FROM `mkt2.optimal_compare_avg_store`
WHERE prod_id IN (SELECT prod_id FROM `mkt.prod_price_reduction`)
ORDER BY prod_id, store_id;
