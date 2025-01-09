-- Создание базы данных Dim_Specialty_Teas_Coffees_Spices
-- CREATE DATABASE dim_specialty_teas_coffees_spices;

-- Использование созданной базы данных
-- \c dim_specialty_teas_coffees_spices;

CREATE TABLE DimUsers (
    user_id SERIAL PRIMARY KEY, -- Добавлено UNIQUE constraint
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_date TIMESTAMP DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);

CREATE TABLE DimUserProfiles (
    profile_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    phone_number VARCHAR(22),
    address VARCHAR(255),
    FOREIGN KEY (user_id) REFERENCES DimUsers(user_id) ON DELETE CASCADE
);

CREATE TABLE DimCategories (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT
);

CREATE TABLE DimOrigins (
    origin_id SERIAL PRIMARY KEY,
    country_name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE DimFlavorProfiles (
    flavor_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE DimProducts (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INT NOT NULL,
    category_id INT,
    origin_id INT,
    flavor_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES DimCategories(category_id) ON DELETE SET NULL,
    FOREIGN KEY (origin_id) REFERENCES DimOrigins(origin_id) ON DELETE SET NULL,
    FOREIGN KEY (flavor_id) REFERENCES DimFlavorProfiles(flavor_id) ON DELETE SET NULL
);

CREATE TABLE DimSuppliers (
    supplier_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    contact_name VARCHAR(255),
    phone_number VARCHAR(25),
    email VARCHAR(255),
    address VARCHAR(255)
);

CREATE TABLE DimDates (
    date_id SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    week_of_year INT NOT NULL
);

CREATE TABLE DimWishlists (
    wishlist_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    created_at_id INT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES DimUsers(user_id),
    FOREIGN KEY (created_at_id) REFERENCES DimDates(date_id)
);

CREATE TABLE FactOrders (
    order_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    order_date_id INT NOT NULL,
    status VARCHAR(50) NOT NULL,
    total_order_amount DECIMAL(15, 2) NOT NULL,
    total_items INT NOT NULL,
    delivery_address VARCHAR(255) NOT NULL,
    payment_method VARCHAR(50),
    FOREIGN KEY (user_id) REFERENCES DimUsers(user_id),
    FOREIGN KEY (order_date_id) REFERENCES DimDates(date_id)
);

CREATE TABLE FactReviews (
    review_id SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    user_id INT NOT NULL,
    rating INT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    comment TEXT,
    review_date_id INT NOT NULL,
    FOREIGN KEY (product_id) REFERENCES DimProducts(product_id),
    FOREIGN KEY (user_id) REFERENCES DimUsers(user_id),
    FOREIGN KEY (review_date_id) REFERENCES DimDates(date_id)
);

CREATE TABLE FactProductSuppliers (
    product_supplier_id SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    supplier_id INT NOT NULL,
    supply_price DECIMAL(10, 2),
    supply_date_id INT NOT NULL,
    FOREIGN KEY (product_id) REFERENCES DimProducts(product_id),
    FOREIGN KEY (supplier_id) REFERENCES DimSuppliers(supplier_id),
    FOREIGN KEY (supply_date_id) REFERENCES DimDates(date_id)
);

CREATE TABLE FactProductViews (
    view_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    product_id INT NOT NULL,
    view_date_id INT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES DimUsers(user_id),
    FOREIGN KEY (product_id) REFERENCES DimProducts(product_id),
    FOREIGN KEY (view_date_id) REFERENCES DimDates(date_id)
);

CREATE OR REPLACE FUNCTION scd_type_2_trigger() RETURNS TRIGGER AS $$
BEGIN
    IF (OLD.username IS DISTINCT FROM NEW.username) OR
       (OLD.email IS DISTINCT FROM NEW.email) OR
       (OLD.password IS DISTINCT FROM NEW.password) THEN
        -- Set the old record as no longer current
        UPDATE DimUsers
        SET end_date = CURRENT_TIMESTAMP,
            is_current = FALSE
        WHERE user_id = OLD.user_id AND is_current = TRUE;

        -- Insert the new record
        INSERT INTO DimUsers (user_id, username, email, password, created_at, updated_at, start_date)
        VALUES (OLD.user_id, NEW.username, NEW.email, NEW.password, NEW.created_at, NEW.updated_at, CURRENT_TIMESTAMP);
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Создание триггера для обновления записей в таблице DimUsers
CREATE TRIGGER scd_type_2_update
BEFORE UPDATE ON DimUsers
FOR EACH ROW
EXECUTE FUNCTION scd_type_2_trigger();

CREATE TABLE FactWishlistItems (
    wishlist_item_id SERIAL PRIMARY KEY,
    wishlist_id INT NOT NULL,
    product_id INT NOT NULL,
    added_date_id INT NOT NULL,
    FOREIGN KEY (wishlist_id) REFERENCES DimWishlists(wishlist_id),
    FOREIGN KEY (product_id) REFERENCES DimProducts(product_id),
    FOREIGN KEY (added_date_id) REFERENCES DimDates(date_id)
);