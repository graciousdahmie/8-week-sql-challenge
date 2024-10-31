USE dannys_diner;
SELECT s.customer_id, SUM(m.price) AS total_spent
FROM sales s
JOIN menu m ON s.product_id = m.product_id
GROUP BY s.customer_id;
-- What is the total amount each customer spent at the restaurant?
-- Customer visit days
SELECT 
    customer_id, 
    COUNT(DISTINCT order_date) AS visit_days
FROM 
    dannys_diner.sales
GROUP BY 
    customer_id;
    -- How many days has each customer visited the restaurant?
    -- order date and product name
    SELECT 
    s.customer_id, 
    MIN(s.order_date) AS first_order_date,
    m.product_name
FROM 
    dannys_diner.sales s
JOIN 
    dannys_diner.menu m ON s.product_id = m.product_id
GROUP BY 
    s.customer_id, m.product_name
HAVING 
    MIN(s.order_date) = (SELECT MIN(order_date) 
                          FROM dannys_diner.sales 
                          WHERE customer_id = s.customer_id);

-- What was the first item from the menu purchased by each customer?
-- Purchase count
SELECT 
    m.product_name, 
    COUNT(s.product_id) AS purchase_count
FROM 
    dannys_diner.sales s
JOIN 
    dannys_diner.menu m ON s.product_id = m.product_id
GROUP BY 
    m.product_name
ORDER BY 
    purchase_count DESC
LIMIT 1;

-- What is the most purchased item on the menu and how many times was it purchased by all customers?
-- Proudct name, purchase count
SELECT 
    s.customer_id, 
    m.product_name, 
    COUNT(s.product_id) AS purchase_count
FROM 
    dannys_diner.sales s
JOIN 
    dannys_diner.menu m ON s.product_id = m.product_id
GROUP BY 
    s.customer_id, m.product_name
ORDER BY 
    s.customer_id, purchase_count DESC;
   
   -- Which item was the most popular for each customer?
   -- first order date 
   SELECT 
    s.customer_id,
    MIN(s.order_date) AS first_order_date,
    m.product_name
FROM 
    dannys_diner.sales s
JOIN 
    dannys_diner.menu m ON s.product_id = m.product_id
JOIN 
    dannys_diner.members mem ON s.customer_id = mem.customer_id
WHERE 
    s.order_date > mem.join_date
GROUP BY 
    s.customer_id, m.product_name
ORDER BY 
    s.customer_id, first_order_date
    limit 1;
-- Which item was purchased first by the customer after they became a member?
-- last order date
SELECT 
    s.customer_id, 
    MAX(s.order_date) AS last_order_date, 
    m.product_name 
FROM 
    dannys_diner.sales s 
JOIN 
    dannys_diner.menu m ON s.product_id = m.product_id 
JOIN 
    dannys_diner.members mem ON s.customer_id = mem.customer_id 
WHERE 
    s.order_date < mem.join_date 
GROUP BY 
    s.customer_id, 
    m.product_name;
    
-- Which item was purchased just before the customer became a member?
-- Total items/total spent
SELECT 
    mem.customer_id,
    COUNT(s.product_id) AS total_items,
    SUM(m.price) AS total_spent
FROM 
    dannys_diner.members mem
LEFT JOIN 
    dannys_diner.sales s ON mem.customer_id = s.customer_id
LEFT JOIN 
    dannys_diner.menu m ON s.product_id = m.product_id
WHERE 
    s.order_date < mem.join_date
GROUP BY 
    mem.customer_id;

-- What is the total items and amount spent for each member before they became a member?
-- Total item/ total spent
SELECT 
    s.customer_id,
    SUM(CASE 
        WHEN m.product_name = 'sushi' THEN m.price * 2 * 10
        ELSE m.price * 10 
    END) AS total_points
FROM 
    dannys_diner.sales s
JOIN 
    dannys_diner.menu m ON s.product_id = m.product_id
GROUP BY 
    s.customer_id;
    
   --  If each $1 spent equates to 10 points and sushi has a 2x points multiplier - how many points would each customer have?
   -- Total points of each customer
   SELECT
    s.customer_id,
    SUM(
        CASE
            WHEN m.product_id = 1 THEN m.price * 10 * 2  -- Sushi with 2x multiplier
            ELSE m.price * 10  -- Regular points
        END
    ) AS total_points
FROM
    sales s
JOIN
    menu m ON s.product_id = m.product_id
GROUP BY
    s.customer_id;

    
   -- In the first week after a customer joins the program (including their join date) they earn 2x points on all items, not just sushi - how many points do customer A and B have at the end of January?
    -- total points
    SELECT 
    s.customer_id, 
    SUM(CASE 
        WHEN s.order_date BETWEEN mem.join_date AND DATE_ADD(mem.join_date, INTERVAL 6 DAY) THEN m.price * 20 -- Double points for the first week 
        ELSE m.price * 10 -- Normal points after the first week 
    END) AS total_points 
FROM 
    dannys_diner.sales s 
JOIN 
    dannys_diner.menu m ON s.product_id = m.product_id 
JOIN 
    dannys_diner.members mem ON s.customer_id = mem.customer_id 
WHERE 
    s.customer_id IN ('A', 'B') 
    AND s.order_date <= '2021-01-31' -- Only include orders up to the end of January 
GROUP BY 
    s.customer_id;

-- Recreate the following table output using the available data:
-- Customer Table
SELECT 
    s.customer_id,
    s.order_date,
    m.product_name,
    m.price,
    CASE 
        WHEN mem.customer_id IS NOT NULL THEN 'Y'
        ELSE 'N'
    END AS member
FROM 
    dannys_diner.sales s
JOIN 
    dannys_diner.menu m ON s.product_id = m.product_id
LEFT JOIN 
    dannys_diner.members mem ON s.customer_id = mem.customer_id
ORDER BY 
    s.customer_id, s.order_date;

-- Rank all things
-- Rankiung 
SELECT 
    s.customer_id,
    s.order_date,
    m.product_name,
    m.price,
    CASE 
        WHEN mem.customer_id IS NOT NULL THEN 'Y'
        ELSE 'N'
    END AS member,
    CASE 
        WHEN mem.customer_id IS NOT NULL THEN RANK() OVER (PARTITION BY s.customer_id ORDER BY s.order_date, m.product_name)
        ELSE NULL
    END AS ranking
FROM 
    dannys_diner.sales s
JOIN 
    dannys_diner.menu m ON s.product_id = m.product_id
LEFT JOIN 
    dannys_diner.members mem ON s.customer_id = mem.customer_id
ORDER BY 
    s.customer_id, s.order_date;


