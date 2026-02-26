-- This query fetches active orders
-- with their customer information
SELECT
    order_id,
    customer_name
FROM orders
WHERE status = 'active';

/* Multi-line block comment:
   This query calculates order totals
   grouped by customer */
SELECT
    customer_id,
    SUM(amount) AS total_amount
FROM orders
GROUP BY customer_id
HAVING SUM(amount) > 500;
