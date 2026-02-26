SELECT order_id, amount
FROM orders
WHERE status = 'active' AND amount > 100 AND region = 'US'