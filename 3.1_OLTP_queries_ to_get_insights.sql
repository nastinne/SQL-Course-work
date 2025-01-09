-- 1. Топ-5 самых продаваемых продуктов
SELECT
    p.name AS product_name,
    SUM(od.quantity) AS total_sold
FROM
    Products p
        JOIN
    OrderDetails od ON p.product_id = od.product_id
GROUP BY
    p.name
ORDER BY
    total_sold DESC
    LIMIT 5;


-- 2. Выручка по категориям
SELECT
    c.name AS category_name,
    SUM(od.price * od.quantity) AS total_revenue
FROM
    Categories c
        JOIN
    Products p ON c.category_id = p.category_id
        JOIN
    OrderDetails od ON p.product_id = od.product_id
GROUP BY
    c.name
ORDER BY
    total_revenue DESC;

-- 3. Топ-5 пользователей с наибольшим количеством заказов
SELECT
    u.username AS user_name,
    COUNT(o.order_id) AS total_orders
FROM
    Users u
        JOIN
    Orders o ON u.user_id = o.user_id
GROUP BY
    u.username
ORDER BY
    total_orders DESC
LIMIT 5;

-- 4. Средний чек по месяцам
SELECT
    DATE_TRUNC('month', o.order_date) AS order_month,
    AVG(o.total_amount) AS average_order_amount
FROM
    Orders o
GROUP BY
    order_month
ORDER BY
    order_month DESC;
