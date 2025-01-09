-- 1. Количество заказов и средняя сумма по годам
SELECT
    dd.year AS order_year,
    COUNT(fo.order_id) AS total_orders,
    AVG(fo.total_order_amount) AS average_order_amount
FROM
    FactOrders fo
        JOIN
    DimDates dd ON fo.order_date_id = dd.date_id
GROUP BY
    dd.year
ORDER BY
    dd.year;


-- 2. Топ-5 продуктов по количеству отзывов и их средний рейтинг
SELECT
    dp.name AS product_name,
    COUNT(fr.review_id) AS total_reviews,
    AVG(fr.rating) AS average_rating
FROM
    FactReviews fr
        JOIN
    DimProducts dp ON fr.product_id = dp.product_id
GROUP BY
    dp.name
ORDER BY
    total_reviews DESC
    LIMIT 5;

-- 3. Общая выручка по странам происхождения продуктов
SELECT
    dos.country_name AS country,
    SUM(fo.total_order_amount) AS total_revenue
FROM
    FactOrders fo
        JOIN DimProducts dp ON fo.user_id = dp.product_id
        JOIN DimOrigins dos ON dp.origin_id = dos.origin_id
GROUP BY
dos.country_name
ORDER BY
total_revenue DESC;


-- 4. Ежеквартальные продажи по странам происхождения
SELECT
    dos.country_name AS country,
    dd.year AS sale_year,
    dd.quarter AS sale_quarter,
    SUM(fo.total_order_amount) AS total_sales
FROM
    FactOrders fo
        JOIN DimProducts dp ON fo.user_id = dp.product_id
        JOIN DimOrigins dos ON dp.origin_id = dos.origin_id
    JOIN DimDates dd ON fo.order_date_id = dd.date_id
GROUP BY
dos.country_name, dd.year, dd.quarter
ORDER BY
dd.year, dd.quarter, total_sales DESC;
