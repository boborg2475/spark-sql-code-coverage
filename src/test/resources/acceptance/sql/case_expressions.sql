SELECT
    order_id,
    CASE
        WHEN status = 'active' THEN 'Active'
        WHEN status = 'pending' THEN 'Pending'
        WHEN status = 'cancelled' THEN 'Cancelled'
        ELSE 'Unknown'
    END AS status_label
FROM orders