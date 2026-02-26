SELECT
    o.order_id,
    c.customer_name,
    p.product_name,
    CASE
        WHEN o.amount > 1000 THEN 'High Value'
        WHEN o.amount > 100 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS order_tier,
    CASE
        WHEN c.region = 'US' THEN 'Domestic'
        ELSE 'International'
    END AS customer_type,
    COALESCE(o.discount, 0) AS effective_discount
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
WHERE o.status = 'active'
    AND o.amount > 50;

SELECT
    customer_id,
    COUNT(*) AS order_count,
    SUM(amount) AS total_spent
FROM orders
WHERE status IN ('active', 'completed')
GROUP BY customer_id
HAVING COUNT(*) > 5;
