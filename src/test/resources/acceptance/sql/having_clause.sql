SELECT customer_id, COUNT(*) AS order_count, SUM(amount) AS total
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 5 AND SUM(amount) > 1000