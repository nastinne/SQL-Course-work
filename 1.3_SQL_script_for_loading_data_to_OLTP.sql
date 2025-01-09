-- Использование созданной базы данных
-- \c specialty_teas_coffees_spices;

-- Создаем временные таблицы для загрузки данных из CSV файлов
CREATE TEMP TABLE temp_users (
    username VARCHAR,
    email VARCHAR,
    password VARCHAR,
    created_at VARCHAR,
    updated_at VARCHAR
);

CREATE TEMP TABLE temp_user_profiles (
    username VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    phone_number VARCHAR,
    address VARCHAR
);

CREATE TEMP TABLE temp_categories (
    name VARCHAR,
    description VARCHAR
);

CREATE TEMP TABLE temp_flavor_profiles (
    name VARCHAR
);

CREATE TEMP TABLE temp_origins (
   country_name VARCHAR
);

CREATE TEMP TABLE temp_products (
    name VARCHAR,
    description VARCHAR,
    price VARCHAR,
    stock_quantity VARCHAR,
    category_name VARCHAR,
    origin_country VARCHAR,
    flavor_name VARCHAR,
    created_at VARCHAR,
    updated_at VARCHAR
);

CREATE TEMP TABLE temp_orders (
    username VARCHAR,
    order_date VARCHAR,
    status VARCHAR,
    total_amount VARCHAR,
    delivery_address VARCHAR,
    payment_method VARCHAR,
    order_detail_product_name VARCHAR,
    order_detail_quantity VARCHAR,
    order_detail_price VARCHAR
);

CREATE TEMP TABLE temp_suppliers (
    name VARCHAR,
    contact_name VARCHAR,
    phone_number VARCHAR,
    email VARCHAR,
    address VARCHAR
);

CREATE TEMP TABLE temp_product_suppliers (
    product_name VARCHAR,
    supplier_name VARCHAR,
    supply_price VARCHAR,
    supply_date VARCHAR
);

CREATE TEMP TABLE temp_reviews (
    product_name VARCHAR,
    username VARCHAR,
    rating VARCHAR,
    comment VARCHAR,
    review_date VARCHAR
);

CREATE TEMP TABLE temp_wishlists (
    username VARCHAR,
    created_at VARCHAR,
    wishlist_product_name VARCHAR
);

CREATE TEMP TABLE temp_product_views (
       username VARCHAR,
       product_name VARCHAR,
       view_date VARCHAR
);

CREATE TEMP TABLE temp_roles (
    role_name VARCHAR
);

CREATE TEMP TABLE temp_user_roles (
    username VARCHAR,
    role_name VARCHAR
);

-- Загружаем данные из CSV файлов во временные таблицы
-- COPY temp_users FROM '/path/to/users.csv' WITH CSV HEADER;
COPY temp_users FROM '/csvs/users.csv' WITH CSV HEADER;
COPY temp_user_profiles FROM '/csvs/user_profiles.csv' WITH CSV HEADER;
COPY temp_categories FROM '/csvs/categories.csv' WITH CSV HEADER;
COPY temp_flavor_profiles FROM '/csvs/flavor_profiles.csv' WITH CSV HEADER;
COPY temp_origins FROM '/csvs/origins.csv' WITH CSV HEADER;
COPY temp_products FROM '/csvs/products.csv' WITH CSV HEADER;
COPY temp_orders FROM '/csvs/orders.csv' WITH CSV HEADER;
COPY temp_suppliers FROM '/csvs/suppliers.csv' WITH CSV HEADER;
COPY temp_product_suppliers FROM '/csvs/product_suppliers.csv' WITH CSV HEADER;
COPY temp_reviews FROM '/csvs/reviews.csv' WITH CSV HEADER;
COPY temp_wishlists FROM '/csvs/wishlists.csv' WITH CSV HEADER;
COPY temp_product_views FROM '/csvs/product_views.csv' WITH CSV HEADER;
COPY temp_roles FROM '/csvs/roles.csv' WITH CSV HEADER;
COPY temp_user_roles FROM '/csvs/user_roles.csv' WITH CSV HEADER;

-- Загружаем данные из временных таблиц в OLTP таблицы
-- Users таблица с очисткой данных
INSERT INTO Users (username, email, password, created_at, updated_at)
SELECT DISTINCT username, email, password,
       to_timestamp(created_at, 'YYYY-MM-DD HH24:MI:SS')::timestamp,
       to_timestamp(updated_at, 'YYYY-MM-DD HH24:MI:SS')::timestamp
FROM temp_users
WHERE email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$'
  AND username IS NOT NULL AND email IS NOT NULL AND password IS NOT NULL
  AND created_at IS NOT NULL AND updated_at IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM Users u WHERE u.email = temp_users.email
);

-- UserProfiles таблица с очисткой данных
INSERT INTO UserProfiles (user_id, first_name, last_name, phone_number, address)
SELECT u.user_id, t.first_name, t.last_name, t.phone_number, t.address
FROM temp_user_profiles t
JOIN Users u ON u.username = t.username
WHERE t.first_name IS NOT NULL AND t.last_name IS NOT NULL
  AND t.phone_number IS NOT NULL AND t.address IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM UserProfiles p WHERE p.user_id = u.user_id
);

-- Categories таблица с очисткой данных
INSERT INTO Categories (name, description)
SELECT DISTINCT name, description
FROM temp_categories
WHERE name IS NOT NULL AND description IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM Categories c WHERE c.name = temp_categories.name
);

-- FlavorProfiles таблица с очисткой данных
INSERT INTO FlavorProfiles (name)
SELECT DISTINCT name
FROM temp_flavor_profiles
WHERE name IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM FlavorProfiles f WHERE f.name = temp_flavor_profiles.name
);

-- Origins таблица с очисткой данных
INSERT INTO Origins (country_name)
SELECT DISTINCT country_name
FROM temp_origins
WHERE country_name IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM Origins o WHERE o.country_name = temp_origins.country_name
);

-- Products таблица с очисткой данных
INSERT INTO Products (name, description, price, stock_quantity, category_id, origin_id, flavor_id, created_at, updated_at)
SELECT t.name, t.description,
       t.price::numeric,
        t.stock_quantity::integer,
        c.category_id,
       o.origin_id,
       f.flavor_id,
       to_timestamp(t.created_at, 'YYYY-MM-DD HH24:MI:SS')::timestamp,
       to_timestamp(t.updated_at, 'YYYY-MM-DD HH24:MI:SS')::timestamp
FROM temp_products t
LEFT JOIN Categories c ON c.name = t.category_name
LEFT JOIN Origins o ON o.country_name = t.origin_country
LEFT JOIN FlavorProfiles f ON f.name = t.flavor_name
WHERE t.name IS NOT NULL AND t.description IS NOT NULL
  AND t.price IS NOT NULL AND t.stock_quantity IS NOT NULL
  AND t.category_name IS NOT NULL AND t.created_at IS NOT NULL AND t.updated_at IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM Products p WHERE p.name = t.name
);

-- Orders таблица с очисткой данных
DO $$
DECLARE
    order_rec RECORD;
    product_rec RECORD;
    v_order_id INTEGER;
BEGIN
    FOR order_rec IN SELECT * FROM temp_orders
                    WHERE username IS NOT NULL AND order_date IS NOT NULL AND status IS NOT NULL
                          AND total_amount IS NOT NULL AND delivery_address IS NOT NULL
                          AND payment_method IS NOT NULL AND order_detail_product_name IS NOT NULL
                          AND order_detail_quantity IS NOT NULL AND order_detail_price IS NOT NULL LOOP
        -- Вставляем Order и захватываем order_id
        INSERT INTO Orders (user_id, order_date, status, total_amount, delivery_address, payment_method)
        SELECT u.user_id,
               to_timestamp(order_rec.order_date, 'YYYY-MM-DD HH24:MI:SS')::timestamp,
               order_rec.status,
               order_rec.total_amount::numeric,
               order_rec.delivery_address,
               order_rec.payment_method
        FROM Users u
        WHERE u.username = order_rec.username
        ON CONFLICT (user_id, order_date) DO NOTHING
        RETURNING order_id INTO v_order_id;

        -- Если заказ был вставлен или найден (v_order_id не равен null), вставляем детали заказа
        IF v_order_id IS NULL THEN
            SELECT order_id INTO v_order_id
            FROM Orders o
            JOIN Users u ON u.user_id = o.user_id
            WHERE u.username = order_rec.username
              AND o.order_date = to_timestamp(order_rec.order_date, 'YYYY-MM-DD HH24:MI:SS')::timestamp;
        END IF;

        FOR product_rec IN SELECT * FROM Products p WHERE p.name = order_rec.order_detail_product_name LOOP
            INSERT INTO OrderDetails (order_id, product_id, quantity, price)
            VALUES (v_order_id, product_rec.product_id, order_rec.order_detail_quantity::integer, order_rec.order_detail_price::numeric)
            ON CONFLICT (order_id, product_id) DO NOTHING;
        END LOOP;
    END LOOP;
END $$;

-- Suppliers таблица с очисткой данных
INSERT INTO Suppliers (name, contact_name, phone_number, email, address)
SELECT DISTINCT name, contact_name, phone_number, email, address
FROM temp_suppliers
WHERE email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$'
  AND name IS NOT NULL AND contact_name IS NOT NULL
  AND phone_number IS NOT NULL AND address IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM Suppliers s WHERE s.name = temp_suppliers.name
);

-- ProductSuppliers таблица с очисткой данных
INSERT INTO ProductSuppliers (product_id, supplier_id, supply_price, supply_date)
SELECT p.product_id, s.supplier_id,
       t.supply_price::numeric,
       to_timestamp(t.supply_date, 'YYYY-MM-DD HH24:MI:SS')::timestamp
FROM temp_product_suppliers t
JOIN Products p ON p.name = t.product_name
JOIN Suppliers s ON s.name = t.supplier_name
WHERE t.supply_price IS NOT NULL AND t.supply_date IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM ProductSuppliers ps WHERE ps.product_id = p.product_id AND ps.supplier_id = s.supplier_id
);

-- Reviews таблица с очисткой данных
INSERT INTO Reviews (product_id, user_id, rating, comment, review_date)
SELECT p.product_id, u.user_id,
       t.rating::integer,
       t.comment,
       to_timestamp(t.review_date, 'YYYY-MM-DD HH24:MI:SS')::timestamp
FROM temp_reviews t
JOIN Products p ON p.name = t.product_name
JOIN Users u ON u.username = t.username
WHERE t.rating IS NOT NULL AND t.comment IS NOT NULL AND t.review_date IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM Reviews r WHERE r.product_id = p.product_id AND r.user_id = u.user_id
);

-- Wishlists таблица с очисткой данных
DO $$
DECLARE
    wishlist_rec RECORD;
    product_rec RECORD;
    v_wishlist_id INTEGER;
BEGIN
    FOR wishlist_rec IN SELECT * FROM temp_wishlists
                        WHERE username IS NOT NULL AND created_at IS NOT NULL
                              AND wishlist_product_name IS NOT NULL LOOP
        -- Вставляем Wishlist и захватываем wishlist_id
        INSERT INTO Wishlists (user_id, created_at)
        SELECT u.user_id, to_timestamp(wishlist_rec.created_at, 'YYYY-MM-DD HH24:MI:SS')::timestamp
        FROM Users u
        WHERE u.username = wishlist_rec.username
        ON CONFLICT (user_id) DO NOTHING
        RETURNING wishlist_id INTO v_wishlist_id;

        -- Если wishlist был вставлен или найден (v_wishlist_id не равен null), вставляем элементы списка желаемого
        IF v_wishlist_id IS NULL THEN
            SELECT wishlist_id INTO v_wishlist_id
            FROM Wishlists w
            JOIN Users u ON u.user_id = w.user_id
            WHERE u.username = wishlist_rec.username;
        END IF;

        FOR product_rec IN SELECT * FROM Products p WHERE p.name = wishlist_rec.wishlist_product_name LOOP
            INSERT INTO WishlistItems (wishlist_id, product_id)
            VALUES (v_wishlist_id, product_rec.product_id)
            ON CONFLICT (wishlist_id, product_id) DO NOTHING;
        END LOOP;
    END LOOP;
END $$;

-- ProductViews таблица с очисткой данных
INSERT INTO ProductViews (user_id, product_id, view_date)
SELECT u.user_id,
       p.product_id,
       to_timestamp(t.view_date, 'YYYY-MM-DD HH24:MI:SS')::timestamp
FROM temp_product_views t
JOIN Users u ON u.username = t.username
JOIN Products p ON p.name = t.product_name
WHERE t.view_date IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM ProductViews pv WHERE pv.user_id = u.user_id AND pv.product_id = p.product_id
);

-- Roles таблица с очисткой данных
INSERT INTO Roles (role_name)
SELECT DISTINCT role_name
FROM temp_roles
WHERE role_name IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM Roles r WHERE r.role_name = temp_roles.role_name
);

-- UserRoles таблица с очисткой данных
INSERT INTO UserRoles (user_id, role_id)
SELECT u.user_id, r.role_id
FROM temp_user_roles t
JOIN Users u ON u.username = t.username
JOIN Roles r ON r.role_name = t.role_name
WHERE t.role_name IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM UserRoles ur WHERE ur.user_id = u.user_id AND ur.role_id = r.role_id
);

-- Удаляем временные таблицы
DROP TABLE temp_users;
DROP TABLE temp_user_profiles;
DROP TABLE temp_categories;
DROP TABLE temp_flavor_profiles;
DROP TABLE temp_origins;
DROP TABLE temp_products;
DROP TABLE temp_orders;
DROP TABLE temp_suppliers;
DROP TABLE temp_product_suppliers;
DROP TABLE temp_reviews;
DROP TABLE temp_wishlists;
DROP TABLE temp_product_views;
DROP TABLE temp_roles;
DROP TABLE temp_user_roles;
