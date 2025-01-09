-- Создаем расширение `postgres_fdw`
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Создание сервера для подключения к OLTP
CREATE SERVER IF NOT EXISTS oltp_server2
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'localhost', dbname 'specialty_teas_coffees_spices', port '5432');

-- Настройка пользователя для подключения к OLTP
CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
    SERVER oltp_server2
    OPTIONS (user 'postgres', password 'postgres');

-- Импорт таблиц из OLTP в текущую схему OLAP
IMPORT FOREIGN SCHEMA public
    FROM SERVER oltp_server2
    INTO public;

-- Создание функции ETL
CREATE OR REPLACE FUNCTION run_etl()
    RETURNS void AS $$
DECLARE
    current_date TIMESTAMP := CURRENT_TIMESTAMP;
BEGIN
    -- 1. Перенос пользователей в DimUsers
    INSERT INTO DimUsers (user_id, username, email, password, created_at, updated_at, start_date, end_date, is_current)
    SELECT
        u.user_id, u.username, u.email, u.password, u.created_at, u.updated_at, current_date, '9999-12-31', TRUE
    FROM public.Users u
             LEFT JOIN DimUsers du ON u.user_id = du.user_id
    WHERE du.user_id IS NULL;

    -- 2. Перенос профилей пользователей в DimUserProfiles
    INSERT INTO DimUserProfiles (profile_id, user_id, first_name, last_name, phone_number, address)
    SELECT
        up.profile_id, up.user_id, up.first_name, up.last_name, up.phone_number, up.address
    FROM public.UserProfiles up
             LEFT JOIN DimUserProfiles dup ON up.profile_id = dup.profile_id
    WHERE dup.profile_id IS NULL;

    -- 3. Перенос категорий в DimCategories
    INSERT INTO DimCategories (category_id, name, description)
    SELECT
        c.category_id, c.name, c.description
    FROM public.Categories c
             LEFT JOIN DimCategories dc ON c.category_id = dc.category_id
    WHERE dc.category_id IS NULL;

    -- 4. Перенос профилей вкусов в DimFlavorProfiles
    INSERT INTO DimFlavorProfiles (flavor_id, name)
    SELECT
        f.flavor_id, f.name
    FROM public.FlavorProfiles f
             LEFT JOIN DimFlavorProfiles df ON f.flavor_id = df.flavor_id
    WHERE df.flavor_id IS NULL;

    -- 5. Перенос стран происхождения в DimOrigin
    INSERT INTO DimOrigins (origin_id, country_name)
    SELECT
        o.origin_id, o.country_name
    FROM public.Origins o
             LEFT JOIN DimOrigins dot ON o.origin_id = dot.origin_id
    WHERE dot.origin_id IS NULL;

    -- 6. Перенос товаров в DimProducts
    INSERT INTO DimProducts (product_id, name, description, price, stock_quantity, category_id, origin_id, flavor_id, created_at, updated_at)
    SELECT
        p.product_id, p.name, p.description, p.price, p.stock_quantity, p.category_id, p.origin_id, p.flavor_id, p.created_at, p.updated_at
    FROM public.Products p
             LEFT JOIN DimProducts dp ON p.product_id = dp.product_id
    WHERE dp.product_id IS NULL;

    -- 7. Перенос поставщиков в DimSuppliers
    INSERT INTO DimSuppliers (supplier_id, name, contact_name, phone_number, email, address)
    SELECT
        s.supplier_id, s.name, s.contact_name, s.phone_number, s.email, s.address
    FROM public.Suppliers s
             LEFT JOIN DimSuppliers ds ON s.supplier_id = ds.supplier_id
    WHERE ds.supplier_id IS NULL;

    -- 8. Перенос дат в DimDate
    INSERT INTO DimDates (date, day, month, year, quarter, week_of_year)
    SELECT DISTINCT
        order_date::DATE,
        EXTRACT(DAY FROM order_date),
        EXTRACT(MONTH FROM order_date),
        EXTRACT(YEAR FROM order_date),
        EXTRACT(QUARTER FROM order_date),
        EXTRACT(WEEK FROM order_date)
    FROM public.Orders
    ON CONFLICT (date) DO NOTHING;

    -- 9. Перенос заказов в FactOrders
    INSERT INTO FactOrders (order_id, user_id, order_date_id, status, total_order_amount, total_items, delivery_address, payment_method)
    SELECT
        o.order_id, o.user_id, dd.date_id, o.status, o.total_amount,
        (SELECT SUM(od.quantity) FROM public.OrderDetails od WHERE od.order_id = o.order_id),
        o.delivery_address, o.payment_method
    FROM public.Orders o
             JOIN DimDates dd ON o.order_date::DATE = dd.date
             LEFT JOIN FactOrders fo ON o.order_id = fo.order_id
    WHERE fo.order_id IS NULL;

    -- 10. Перенос деталей заказов в FactReviews
    INSERT INTO FactReviews (review_id, product_id, user_id, rating, comment, review_date_id)
    SELECT
        r.review_id, r.product_id, r.user_id, r.rating, r.comment, dd.date_id
    FROM public.Reviews r
             JOIN DimDates dd ON r.review_date::DATE = dd.date
             LEFT JOIN FactReviews fr ON r.review_id = fr.review_id
    WHERE fr.review_id IS NULL;

    -- 11. Перенос поставок в FactProductSupplier
    INSERT INTO FactProductSuppliers (product_supplier_id, product_id, supplier_id, supply_price, supply_date_id)
    SELECT
        ps.product_supplier_id, ps.product_id, ps.supplier_id, ps.supply_price, dd.date_id
    FROM public.ProductSuppliers ps
             JOIN DimDates dd ON ps.supply_date::DATE = dd.date
             LEFT JOIN FactProductSuppliers fps ON ps.product_supplier_id = fps.product_supplier_id
    WHERE fps.product_supplier_id IS NULL;

    -- 12. Перенос просмотров продуктов в FactProductView
    INSERT INTO FactProductViews (view_id, user_id, product_id, view_date_id)
    SELECT
        pv.view_id, pv.user_id, pv.product_id, dd.date_id
    FROM public.ProductViews pv
             JOIN DimDates dd ON pv.view_date::DATE = dd.date
             LEFT JOIN FactProductViews fpv ON pv.view_id = fpv.view_id
    WHERE fpv.view_id IS NULL;

    -- 13. Перенос списков желаемого в DimWishlist и FactWishlistItem
    -- Перенос данных в DimWishlist
    INSERT INTO DimWishlists (wishlist_id, user_id, created_at_id)
    SELECT
        w.wishlist_id,
        w.user_id,
        dd.date_id
    FROM public.Wishlists w
             JOIN DimDates dd ON w.created_at::DATE = dd.date
             LEFT JOIN DimWishlists dw ON w.wishlist_id = dw.wishlist_id
    WHERE dw.wishlist_id IS NULL;

    -- Перенос данных в FactWishlistItem
    INSERT INTO FactWishlistItems (wishlist_item_id, wishlist_id, product_id, added_date_id)
    SELECT
        wi.wishlist_item_id,
        wi.wishlist_id,
        wi.product_id,
        dd.date_id
    FROM public.WishlistItems wi
             JOIN public.Wishlists w ON wi.wishlist_id = w.wishlist_id -- Соединяем для получения created_at
             JOIN DimDates dd ON w.created_at::DATE = dd.date
             LEFT JOIN FactWishlistItems fwi ON wi.wishlist_item_id = fwi.wishlist_item_id
    WHERE fwi.wishlist_item_id IS NULL;

END;
$$ LANGUAGE plpgsql;

-- Запуск ETL
SELECT run_etl();

-- Ппримеры запросов чтобы убедиться, что данные были успешно загружены
SELECT * FROM DimUsers;
SELECT * FROM FactOrders;