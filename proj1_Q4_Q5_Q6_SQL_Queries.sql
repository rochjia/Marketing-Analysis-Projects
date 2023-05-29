##### Q4 #####

WITH prod_max AS (SELECT prod_id, MAX(unit_sales) AS max_price
FROM `mkt.trans_unit`
GROUP BY prod_id)
SELECT cust_id, t.prod_id, unit_sales, max_price FROM mkt.trans_unit t INNER JOIN prod_max p
ON t.prod_id = p.prod_id;
# Save as trans_max

SELECT cust_id, prod_id, IF(unit_sales/max_price = 1, 0, 1) AS promotion
FROM mkt.trans_max;
# Save as cust_promo

SELECT cust_id, SUM(promotion)/COUNT(promotion) AS promo_perc
FROM mkt.cust_promo GROUP BY cust_id;
# Save as cust_promo_perc

# Most valuable customers
SELECT * FROM mkt.cust_promo_perc
ORDER BY promo_perc;

# Cherry pickers
SELECT * FROM mkt.cust_promo_perc
ORDER BY promo_perc DESC;


##### Q5 #####

# Combine the data
CREATE TABLE capstone2023-homedepot.mkt.comb AS 
(SELECT t.prod_id, prod_desc, prod_category, trans_id, sales_qty, sales_amt, sales_amt/sales_qty AS unit_sales 
FROM mkt.transactions t LEFT JOIN mkt.products p 
ON t.prod_id = p.prod_id WHERE sales_amt > 0 AND sales_qty > 0);

# Get the trans records with maximum price
SELECT c.prod_id, prod_category, unit_sales AS p_max 
FROM mkt.comb c INNER JOIN 
(SELECT prod_id, MAX(unit_sales) AS max_price FROM mkt.comb WHERE unit_sales >= 0 GROUP BY prod_id) sub 
ON c.prod_id = sub.prod_id 
AND c.unit_sales = sub.max_price;
# Save as comb_max

# Get the trans records with minimum price
SELECT c.prod_id, prod_category, unit_sales AS p_min
FROM mkt.comb c INNER JOIN 
(SELECT prod_id, MIN(unit_sales) AS min_price FROM mkt.comb WHERE unit_sales >= 0 GROUP BY prod_id) sub 
ON c.prod_id = sub.prod_id 
AND c.unit_sales = sub.min_price;
# Save as comb_min

# Get the trans count at maximum price for each prod
SELECT prod_id, prod_category, p_max, COUNT(*) AS cnt_max
FROM mkt.comb_max
GROUP BY prod_id, prod_category, p_max;
# Save as max_qty

# Get the trans count at minimum price for each prod
SELECT prod_id, prod_category, p_min, COUNT(*) AS cnt_min
FROM mkt.comb_min
GROUP BY prod_id, prod_category, p_min;
# Save as min_qty

# Combine the two tables, get the price elasticity for each prod
WITH maxmin AS (SELECT max_qty.prod_id, max_qty.prod_category, p_max, p_min, cnt_max, cnt_min FROM mkt.max_qty INNER JOIN mkt.min_qty
ON max_qty.prod_id = min_qty.prod_id)
SELECT prod_id, prod_category, ((cnt_min - cnt_max)/cnt_max)/((p_min - p_max)/p_max) AS elasticity FROM maxmin
WHERE p_max != 0 AND cnt_max != 0 AND
p_min != p_max AND cnt_min != cnt_max # Exclude the completely inelastic products
ORDER BY elasticity DESC;
# Save as kvi

# KVI: Rank by value of elasticity
# Exclude the outlier & the abnormal values
SELECT * FROM mkt.kvi
WHERE elasticity <= 100 AND
elasticity > 0;

# KVC: Rank by value of avg. elasticity
SELECT prod_category, AVG(elasticity) AS avg_elasticity
FROM mkt.kvi
WHERE elasticity <= 100 AND
elasticity > 0
GROUP BY prod_category
ORDER BY avg_elasticity DESC;

# Most promoted products
SELECT prod_id, SUM(promotion)/COUNT(promotion) AS promo_perc 
FROM mkt.cust_promo 
GROUP BY prod_id
ORDER BY promo_perc DESC;
# Save as prod_promo

# Percentage of "Always Promoted" products
# Define it as the products with promotion percentage >= 90%
SELECT SUM(always_promo)/COUNT(always_promo) always_promo_perc FROM (
  SELECT IF(promo_perc >= 0.9, 1, 0) AS always_promo FROM mkt.prod_promo
);


# Least promoted
SELECT prod_id, SUM(promotion)/COUNT(promotion) AS promo_perc 
FROM mkt.cust_promo 
GROUP BY prod_id
ORDER BY promo_perc;

# Percentage of "Never promoted" products
# Define it as the products with promotion percentage = 0
SELECT SUM(never_promo)/COUNT(never_promo) never_promo_perc FROM (
  SELECT IF (promo_perc = 0, 1, 0) AS never_promo FROM mkt.prod_promo
);



##### Q6 #####

# filter cherry-pickers as customers with promotion percentage >= 80%
SELECT cust_id FROM mkt.cust_promo_perc
WHERE promo_perc >= 0.8;
# Save as cherry_picker_id

# Combine the cherry_picker_id with trans table
SELECT store_id, COUNT(DISTINCT cust_id) AS cherry_picker_cnt
FROM (SELECT store_id, t.cust_id FROM mkt.transactions t INNER JOIN mkt.cherry_picker_id c
ON t.cust_id = c.cust_id)
GROUP BY store_id
ORDER BY cherry_picker_cnt DESC;

# Check Average visits per customer per store
SELECT AVG(visit) FROM (SELECT cust_id, store_id, COUNT(trans_id) AS visit
FROM `mkt.transactions`
GROUP BY cust_id, store_id);
# 82 visit

# Define Loyal Customers as visiting the stores for more than 150 times
SELECT store_id, COUNT(DISTINCT cust_id) AS loyal_cust_cnt FROM
(SELECT cust_id, store_id, COUNT(trans_id) AS visit
FROM `mkt.transactions`
GROUP BY cust_id, store_id
HAVING visit > 150)
GROUP BY store_id
ORDER BY loyal_cust_cnt DESC;