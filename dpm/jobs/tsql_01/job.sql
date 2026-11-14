SELECT
    product,
    SUM(premium) AS total_premium
FROM policy
GROUP BY product;
