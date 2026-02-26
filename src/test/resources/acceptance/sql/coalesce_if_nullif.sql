SELECT
    order_id,
    COALESCE(discount, rebate, 0) AS effective_discount,
    IF(amount > 500, 'high', 'low') AS value_tier,
    NULLIF(status, 'cancelled') AS active_status
FROM orders