SELECT
    o.order_id,
    c.customer_name,
    CASE
        WHEN o.amount > 1000 THEN 'Premium'
        WHEN o.amount > 100 THEN 'Standard'
        ELSE 'Basic'
    END AS order_tier,
    COALESCE(o.discount, 0) AS discount,
    IF(c.region = 'US', 'Domestic', 'International') AS market
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.status = 'active' AND o.amount > 50