SELECT
    order_id,
    customer_name,
    CASE
        WHEN status = 'active' THEN 'Active'
        WHEN status = 'pending' THEN 'Pending'
        ELSE 'Unknown'
    END AS status_label
FROM orders
WHERE amount > 50;

SELECT
    o.order_id,
    c.customer_name,
    o.amount
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.amount > 100;

SELECT
    product_name,
    CASE
        WHEN price > 1000 THEN 'Premium'
        WHEN price > 100 THEN 'Standard'
        ELSE 'Budget'
    END AS tier
FROM products
WHERE active = true;
