SELECT 
    p."orderID" as order_id, 
    p.amount
FROM 
    raw.stripe.payment p