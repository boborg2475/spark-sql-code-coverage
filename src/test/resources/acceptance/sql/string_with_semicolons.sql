SELECT
    order_id,
    CASE
        WHEN notes LIKE '%shipped; delivered%' THEN 'Completed'
        WHEN notes = 'pending; review' THEN 'In Review'
        ELSE 'Open'
    END AS parsed_status
FROM orders
WHERE description != 'N/A; not applicable';
