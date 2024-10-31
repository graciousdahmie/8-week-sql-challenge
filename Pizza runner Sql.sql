-- Create the pizza_runner schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS pizza_runner;

-- Switch to the pizza_runner schema (this step is implicit in MySQL)

-- Drop the runners table if it exists
DROP TABLE IF EXISTS pizza_runner.runners;

-- Create the runners table
CREATE TABLE pizza_runner.runners (
  runner_id INTEGER PRIMARY KEY,
  registration_date DATE
);

-- Insert data into the runners table
INSERT INTO pizza_runner.runners (runner_id, registration_date)
VALUES
  (1, '2021-01-01'),
  (2, '2021-01-03'),
  (3, '2021-01-08'),
  (4, '2021-01-15');

-- Drop the customer_orders table if it exists
DROP TABLE IF EXISTS pizza_runner.customer_orders;

-- Create the customer_orders table
CREATE TABLE pizza_runner.customer_orders (
  order_id INTEGER,
  customer_id INTEGER,
  pizza_id INTEGER,
  exclusions VARCHAR(4),
  extras VARCHAR(4),
  order_time TIMESTAMP
);

-- Insert data into the customer_orders table
INSERT INTO pizza_runner.customer_orders (order_id, customer_id, pizza_id, exclusions, extras, order_time)
VALUES
  (1, 101, 1, '', '', '2020-01-01 18:05:02'),
  (2, 101, 1, '', '', '2020-01-01 19:00:52'),
  (3, 102, 1, '', '', '2020-01-02 23:51:23'),
  (4, 102, 2, '', NULL, '2020-01-02 23:51:23'),
  (5, 103, 1, '4', '', '2020-01-04 13:23:46'),
  (6, 103, 1, '4', '', '2020-01-04 13:23:46'),
  (7, 103, 2, '4', '', '2020-01-04 13:23:46'),
  (8, 104, 1, NULL, '1', '2020-01-08 21:00:29'),
  (9, 101, 2, NULL, NULL, '2020-01-08 21:03:13'),
  (10, 105, 2, NULL, '1', '2020-01-08 21:20:29'),
  (11, 102, 1, NULL, NULL, '2020-01-09 23:54:33'),
  (12, 103, 1, '4', '1, 5', '2020-01-10 11:22:59'),
  (13, 104, 1, NULL, NULL, '2020-01-11 18:34:49'),
  (14, 104, 1, '2, 6', '1, 4', '2020-01-11 18:34:49');

-- Drop the runner_orders table if it exists
DROP TABLE IF EXISTS pizza_runner.runner_orders;

-- Create the runner_orders table
CREATE TABLE pizza_runner.runner_orders (
  order_id INTEGER,
  runner_id INTEGER,
  pickup_time VARCHAR(19),
  distance VARCHAR(7),
  duration VARCHAR(10),
  cancellation VARCHAR(23)
);

-- Insert data into the runner_orders table
INSERT INTO pizza_runner.runner_orders (order_id, runner_id, pickup_time, distance, duration, cancellation)
VALUES
  (1, 1, '2020-01-01 18:15:34', '20km', '32 minutes', ''),
  (2, 1, '2020-01-01 19:10:54', '20km', '27 minutes', ''),
  (3, 1, '2020-01-03 00:12:37', '13.4km', '20 mins', NULL),
  (4, 2, '2020-01-04 13:53:03', '23.4km', '40', NULL),
  (5, 3, '2020-01-08 21:10:57', '10km', '15', NULL),
  (6, 3, NULL, NULL, NULL, 'Restaurant Cancellation'),
  (7, 2, '2020-01-08 21:30:45', '25km', '25mins', NULL),
  (8, 2, '2020-01-10 00:15:02', '23.4 km', '15 minute', NULL),
  (9, 2, NULL, NULL, NULL, 'Customer Cancellation'),
  (10, 1, '2020-01-11 18:50:20', '10km', '10minutes', NULL);

-- Drop the pizza_names table if it exists
DROP TABLE IF EXISTS pizza_runner.pizza_names;

-- Create the pizza_names table
CREATE TABLE pizza_runner.pizza_names (
  pizza_id INTEGER,
  pizza_name TEXT
);

-- Insert data into the pizza_names table
INSERT INTO pizza_runner.pizza_names (pizza_id, pizza_name)
VALUES
  (1, 'Meatlovers'),
  (2, 'Vegetarian');

-- Drop the pizza_recipes table if it exists
DROP TABLE IF EXISTS pizza_runner.pizza_recipes;

-- Create the pizza_recipes table
CREATE TABLE pizza_runner.pizza_recipes (
  pizza_id INTEGER,
  toppings TEXT
);

-- Insert data into the pizza_recipes table
INSERT INTO pizza_runner.pizza_recipes (pizza_id, toppings)
VALUES
  (1, '1, 2, 3, 4, 5, 6, 8, 10'),
  (2, '4, 6, 7, 9, 11, 12');

-- Drop the pizza_toppings table if it exists
DROP TABLE IF EXISTS pizza_runner.pizza_toppings;

-- Create the pizza_toppings table
CREATE TABLE pizza_runner.pizza_toppings (
  topping_id INTEGER,
  topping_name TEXT
);

-- Insert data into the pizza_toppings table
INSERT INTO pizza_runner.pizza_toppings (topping_id, topping_name)
VALUES
  (1, 'Bacon'),
  (2, 'BBQ Sauce'),
  (3, 'Beef'),
  (4, 'Cheese'),
  (5, 'Chicken'),
  (6, 'Mushrooms'),
  (7, 'Onions'),
  (8, 'Pepperoni'),
  (9, 'Peppers'),
  (10, 'Salami'),
  (11, 'Tomatoes'),
  (12, 'Tomato Sauce');
  
  -- How many pizzas were ordered?
  SELECT
    r.runner_id,
    r.registration_date,
    COUNT(DISTINCT ro.order_id) AS orders
FROM pizza_runner.runners r
INNER JOIN pizza_runner.runner_orders ro
    ON r.runner_id = ro.runner_id
WHERE ro.cancellation IS NOT NULL
GROUP BY
    r.runner_id,
    r.registration_date;
    
-- How many unique customer orders were made?
-- NUmber of unique orders
SELECT COUNT(DISTINCT order_id) AS unique_orders
FROM customer_orders;

-- How many successful orders were delivered by each runner?
-- Sucesseful orders
SELECT runner_id, COUNT(order_id) AS successful_orders
FROM runner_orders
WHERE cancellation IS NULL
GROUP BY runner_id;

-- How many of each type of pizza was delivered?
-- Types of pizza delivered
SELECT pn.pizza_name, COUNT(ro.order_id) AS pizza_delivered
FROM runner_orders ro
JOIN customer_orders co ON ro.order_id = co.order_id
JOIN pizza_names pn ON co.pizza_id = pn.pizza_id
WHERE ro.cancellation IS NULL
GROUP BY pn.pizza_name;

-- How many Vegetarian and Meatlovers were ordered by each customer?
-- vegetarian and meat orders
SELECT co.customer_id, 
       SUM(CASE WHEN pn.pizza_name = 'Meatlovers' THEN 1 ELSE 0 END) AS meatlovers_count,
       SUM(CASE WHEN pn.pizza_name = 'Vegetarian' THEN 1 ELSE 0 END) AS vegetarian_count
FROM customer_orders co
JOIN pizza_names pn ON co.pizza_id = pn.pizza_id
GROUP BY co.customer_id;

-- What was the maximum number of pizzas delivered in a single order?
-- Maximum numbers of pizzas delivered in an order
SELECT MAX(order_count) AS max_pizzas_in_single_order
FROM (
    SELECT order_id, COUNT(pizza_id) AS order_count
    FROM customer_orders
    GROUP BY order_id
) AS order_counts;

-- For each customer, how many delivered pizzas had at least 1 change and how many had no changes?
-- Pizza with changes and pizza without changes
SELECT customer_id,
       SUM(CASE WHEN exclusions IS NOT NULL OR extras IS NOT NULL THEN 1 ELSE 0 END) AS with_changes,
       SUM(CASE WHEN exclusions IS NULL AND extras IS NULL THEN 1 ELSE 0 END) AS without_changes
FROM customer_orders
WHERE order_id IN (SELECT order_id FROM runner_orders WHERE cancellation IS NULL)
GROUP BY customer_id;

-- How many pizzas were delivered that had both exclusions and extras?
-- Pizza with exclusions and extras
SELECT COUNT(*) AS pizzas_with_exclusions_and_extras
FROM customer_orders
WHERE exclusions IS NOT NULL AND extras IS NOT NULL
AND order_id IN (SELECT order_id FROM runner_orders WHERE cancellation IS NULL);

-- What was the total volume of pizzas ordered for each hour of the day?
-- Total number of pizza ordered per hour
SELECT EXTRACT(HOUR FROM order_time) AS order_hour,
       COUNT(*) AS total_pizzas
FROM customer_orders
GROUP BY order_hour
ORDER BY order_hour;

-- What was the volume of orders for each day of the week?
-- Volume of order for each day of the week
SELECT DAYNAME(order_time) AS day_of_week,
       COUNT(*) AS total_orders
FROM customer_orders
GROUP BY day_of_week
ORDER BY FIELD(day_of_week, 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday');

-- How many runners signed up for each 1 week period? (i.e. week starts 2021-01-01)
-- Number of runner that signed up for each week
SELECT 
    YEAR(registration_date) AS year,
    WEEK(registration_date, 1) AS week,
    COUNT(*) AS runners_signed_up
FROM 
    runners
GROUP BY 
    year, week
ORDER BY 
    year, week;
-- What was the average time in minutes it took for each runner to arrive at the Pizza Runner HQ to pickup the order?
-- Average time it took each runner 
SELECT 
    runner_id,
    AVG(TIMESTAMPDIFF(MINUTE, pickup_time, order_time)) AS avg_time_minutes
FROM 
    runner_orders
JOIN 
    customer_orders ON runner_orders.order_id = customer_orders.order_id
WHERE 
    runner_orders.cancellation IS NULL
GROUP BY 
    runner_id;
    
    -- Is there any relationship between the number of pizzas and how long the order takes to prepare?
    -- Relationship between the number of pizzas time it took
    
    SELECT
    co.order_id,
    COUNT(co.pizza_id) AS number_of_pizzas,
    AVG(TIMESTAMPDIFF(MINUTE, co.order_time, ro.pickup_time)) AS average_preparation_time_minutes
FROM
    customer_orders co
LEFT JOIN
    runner_orders ro ON co.order_id = ro.order_id
GROUP BY
    co.order_id;

-- What was the average distance travelled for each customer?
-- Average distance travelled by each customer
SELECT
    co.customer_id,
    AVG(
        CASE
            WHEN ro.distance IS NOT NULL AND ro.distance NOT LIKE 'null' THEN
                CAST(REPLACE(ro.distance, 'km', '') AS DECIMAL)
            ELSE
                NULL
        END
    ) AS average_distance_km
FROM
    customer_orders co
JOIN
    runner_orders ro ON co.order_id = ro.order_id
GROUP BY
    co.customer_id;

-- What was the difference between the longest and shortest delivery times for all orders?
-- Longest and shortest delivery time
SELECT
    MAX(TIMESTAMPDIFF(SECOND, co.order_time, ro.pickup_time)) AS longest_delivery_time,
    MIN(TIMESTAMPDIFF(SECOND, co.order_time, ro.pickup_time)) AS shortest_delivery_time,
    MAX(TIMESTAMPDIFF(SECOND, co.order_time, ro.pickup_time)) - MIN(TIMESTAMPDIFF(SECOND, co.order_time, ro.pickup_time)) AS time_difference_seconds
FROM
    customer_orders co
JOIN
    runner_orders ro ON co.order_id = ro.order_id;
    
    -- What was the average speed for each runner for each delivery and do you notice any trend for these values?
    -- Average speed for each runner
    SELECT
    ro.runner_id,
    ro.order_id,
    AVG(
        CASE
            WHEN ro.distance IS NOT NULL AND ro.distance NOT LIKE 'null' THEN
                CAST(REPLACE(ro.distance, 'km', '') AS DECIMAL) / 
                (TIMESTAMPDIFF(SECOND, co.order_time, ro.pickup_time) / 3600) -- Convert time to hours
            ELSE
                NULL
        END
    ) AS average_speed_kmh
FROM
    customer_orders co
JOIN
    runner_orders ro ON co.order_id = ro.order_id
GROUP BY
    ro.runner_id, ro.order_id;

-- What is the successful delivery percentage for each runner?
-- Successful delivery percentage
WITH order_summary AS (
    SELECT 
        customer_orders.order_id,
        COUNT(*) AS number_of_pizzas,
        TIMESTAMPDIFF(MINUTE, order_time, runner_orders.pickup_time) AS preparation_time
    FROM 
        customer_orders 
    JOIN 
        runner_orders ON customer_orders.order_id = runner_orders.order_id
    GROUP BY 
        customer_orders.order_id
),
delivery_percentage AS (
    SELECT 
        runner_id,
        (SUM(CASE WHEN cancellation IS NULL THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS successful_delivery_percentage
    FROM 
        runner_orders
    GROUP BY 
        runner_id
)
SELECT 
    runner_id,
    (SUM(CASE WHEN cancellation IS NULL THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS successful_delivery_percentage
FROM 
    runner_orders
GROUP BY 
    runner_id;

-- What are the standard ingredients for each pizza?
-- Standard ingriedent for each pizza
SELECT 
    pn.pizza_name,
    GROUP_CONCAT(pt.topping_name ORDER BY pt.topping_name SEPARATOR ', ') AS standard_ingredients
FROM 
    pizza_names pn
JOIN 
    pizza_recipes pr ON pn.pizza_id = pr.pizza_id
JOIN 
    pizza_toppings pt ON FIND_IN_SET(pt.topping_id, pr.toppings) > 0
GROUP BY 
    pn.pizza_name;

-- What was the most commonly added extra? 
-- Most commonly added extra
SELECT 
    extras,
    COUNT(*) AS count
FROM 
    customer_orders
WHERE 
    extras IS NOT NULL
GROUP BY 
    extras
ORDER BY 
    count DESC
LIMIT 1;

-- What was the most common exclusion?
-- Most common exclusion
SELECT 
    exclusions,
    COUNT(*) AS count
FROM 
    customer_orders
WHERE 
    exclusions IS NOT NULL
GROUP BY 
    exclusions
ORDER BY 
    count DESC
LIMIT 1;

-- Generate an order item for each record in the customers_orders table in the format of one of the following:
-- Meat Lovers
-- Meat Lovers - Exclude Beef
-- Meat Lovers - Extra Bacon
-- Meat Lovers - Exclude Cheese, Bacon - Extra Mushroom, Peppers
-- Order item
SELECT 
    pn.pizza_name,
    CONCAT(
        pn.pizza_name, 
        CASE 
            WHEN co.exclusions IS NOT NULL THEN CONCAT(' - Exclude ', co.exclusions) 
            ELSE ''
        END,
        CASE 
            WHEN co.extras IS NOT NULL THEN CONCAT(' - Extra ', co.extras) 
            ELSE ''
        END
    ) AS order_item
FROM 
    customer_orders co
JOIN 
    pizza_names pn ON co.pizza_id = pn.pizza_id;
    
-- Generate an alphabetically ordered comma separated ingredient list for each pizza order from the customer_orders table and add a 2x in front of any relevant ingredients
-- For example: "Meat Lovers: 2xBacon, Beef, ... , Salami"
-- 1, create table first
SELECT * FROM customer_orders;
SELECT * FROM pizza_ingredients;

SELECT DISTINCT pizza_id FROM customer_orders;
SELECT DISTINCT pizza_id FROM pizza_ingredients;

SELECT * FROM customer_orders;
SELECT * FROM pizza_ingredients;
SELECT * FROM runner_orders;

SELECT DISTINCT pizza_id FROM customer_orders;
SELECT DISTINCT pizza_id FROM pizza_ingredients;

SELECT * FROM runner_orders WHERE cancellation IS NULL;

-- What is the total quantity of each ingredient used in all delivered pizzas sorted by most frequent first?
SELECT
    pi.ingredient,
    COUNT(*) AS total_quantity
FROM
    pizza_ingredients pi
GROUP BY
    pi.ingredient
ORDER BY
    total_quantity DESC;

-- If a Meat Lovers pizza costs $12 and Vegetarian costs $10 and there were no charges for changes - how much money has Pizza Runner made so far if there are no delivery fees?
SELECT
    SUM(CASE WHEN pi.pizza_id = 1 THEN 1 ELSE 0 END) AS meat_lovers_count,
    SUM(CASE WHEN pi.pizza_id = 2 THEN 1 ELSE 0 END) AS vegetarian_count
FROM
    customer_orders co
JOIN
    pizza_ingredients pi ON co.pizza_id = pi.pizza_id
JOIN
    runner_orders ro ON co.order_id = ro.order_id
WHERE
    ro.cancellation IS NULL;  -- Only consider successful deliveries
    
    -- Total earnings
    SELECT
    (SUM(CASE WHEN pi.pizza_id = 1 THEN 1 ELSE 0 END) * 12 +
     SUM(CASE WHEN pi.pizza_id = 2 THEN 1 ELSE 0 END) * 10) AS total_earnings
FROM
    customer_orders co
JOIN
    pizza_ingredients pi ON co.pizza_id = pi.pizza_id
JOIN
    runner_orders ro ON co.order_id = ro.order_id
WHERE
    ro.cancellation IS NULL;

-- What if there was an additional $1 charge for any pizza extras?
-- Add cheese is $1 extra
-- Earning with extras
SELECT
    (SUM(CASE WHEN pi.pizza_id = 1 THEN 1 ELSE 0 END) * 12 +
     SUM(CASE WHEN pi.pizza_id = 2 THEN 1 ELSE 0 END) * 10 +
     SUM(CASE WHEN co.extras IS NOT NULL THEN 1 ELSE 0 END)) AS total_earnings_with_extras
FROM
    customer_orders co
JOIN
    pizza_ingredients pi ON co.pizza_id = pi.pizza_id
JOIN
    runner_orders ro ON co.order_id = ro.order_id
WHERE
    ro.cancellation IS NULL;
    
    CREATE TABLE order_ratings (
    order_id INTEGER,
    runner_id INTEGER,
    rating INTEGER CHECK (rating BETWEEN 1 AND 5),
    FOREIGN KEY (order_id) REFERENCES customer_orders(order_id),
    FOREIGN KEY (runner_id) REFERENCES runners(runner_id)
);

INSERT INTO order_ratings (order_id, runner_id, rating) VALUES
(1, 1, 5),
(2, 1, 4),
(3, 2, 3),
(4, 2, 4),
(5, 3, 5),
(6, 3, 5),
(7, 2, 4),
(8, 2, 3),
(9, 1, 5),
(10, 1, 4);


-- Total revenue
-- Assuming you have the counts
SELECT 
    COUNT(CASE WHEN pizza_id = 1 THEN 1 END) * 12 + 
    COUNT(CASE WHEN pizza_id = 2 THEN 1 END) * 10 AS total_revenue
FROM customer_orders;

-- Total revenues with extra
   SELECT 
    COUNT(CASE WHEN pizza_id = 1 THEN 1 END) * 12 + 
    COUNT(CASE WHEN pizza_id = 2 THEN 1 END) * 10 + 
    SUM(CASE WHEN extras IS NOT NULL THEN 1 ELSE 0 END) AS total_revenue_with_extras
FROM 
    customer_orders;

-- insert Table
INSERT INTO runner_ratings (order_id, runner_id, customer_id, rating)
VALUES 
    (1, 1, 101, 5),
    (2, 1, 101, 4),
    (3, 2, 102, 3),
    (4, 2, 103, 5),
    (5, 3, 104, 2);
-- Using your newly generated table - can you join all of the information together to form a table which has the following information for successful deliveries?
-- Table
SELECT 
    co.customer_id,
    co.order_id,
    ro.runner_id,
    rr.rating,
    co.order_time,
    ro.pickup_time,
    TIMESTAMPDIFF(MINUTE, co.order_time, ro.pickup_time) AS time_between_order_and_pickup,
    TIMESTAMPDIFF(MINUTE, ro.pickup_time, NOW()) AS delivery_duration,
    (ro.distance / TIMESTAMPDIFF(MINUTE, ro.pickup_time, NOW())) AS average_speed,
    COUNT(co.pizza_id) AS total_number_of_pizzas
FROM 
    customer_orders co
JOIN 
    runner_orders ro ON co.order_id = ro.order_id
LEFT JOIN 
    runner_ratings rr ON co.order_id = rr.order_id
WHERE 
    ro.cancellation IS NULL
GROUP BY 
    co.customer_id, 
    co.order_id, 
    ro.runner_id, 
    rr.rating, 
    co.order_time, 
    ro.pickup_time,
    ro.distance;  -- Added ro.distance to the GROUP BY clause
    
   -- Total revenue|total runner payment 
    SELECT 
    SUM(CASE WHEN pizza_id = 1 THEN 12 
             WHEN pizza_id = 2 THEN 10 
             ELSE 0 END) AS total_revenue,
    SUM(DISTINCT ro.distance * 0.30) AS total_runner_payment
FROM 
    customer_orders co
JOIN 
    runner_orders ro ON co.order_id = ro.order_id
WHERE 
    ro.cancellation IS NULL;


-- If a Meat Lovers pizza was $12 and Vegetarian $10 fixed prices with no cost for extras and each runner is paid $0.30 per kilometre traveled - how much money does Pizza Runner have left over after these deliveries?
-- Total money left
SELECT 
    (SELECT SUM(CASE WHEN pizza_id = 1 THEN 12 
                     WHEN pizza_id = 2 THEN 10 
                     ELSE 0 END) 
     FROM customer_orders co 
     JOIN runner_orders ro ON co.order_id = ro.order_id 
     WHERE ro.cancellation IS NULL) -
    (SELECT SUM(DISTINCT ro.distance * 0.30) 
     FROM runner_orders ro 
     WHERE ro.cancellation IS NULL) AS remaining_money;


INSERT INTO pizza_names (pizza_id, pizza_name)
VALUES (3, 'Supreme');


INSERT INTO pizza_recipes (pizza_id, toppings)
VALUES (3, '1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12');  -- Assuming these are the IDs of all toppings


-- Insert the new Supreme pizza into the pizza_names table
INSERT INTO pizza_names (pizza_id, pizza_name)
VALUES (3, 'Supreme');

-- Insert the toppings for the Supreme pizza into the pizza_recipes table
INSERT INTO pizza_recipes (pizza_id, toppings)
VALUES (3, '1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12');  -- IDs of all toppings



