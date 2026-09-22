-- Runs automatically on first boot of the `source_db` container
-- (mounted via docker-entrypoint-initdb.d in docker-compose.yml).

CREATE TABLE IF NOT EXISTS customers (
    customer_id   INT PRIMARY KEY,
    first_name    TEXT,
    last_name     TEXT,
    email         TEXT,
    phone         TEXT,
    signup_date   DATE,
    loyalty_tier  TEXT,
    updated_at    TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stores (
    store_id      INT PRIMARY KEY,
    store_name    TEXT,
    city          TEXT,
    state         TEXT,
    region        TEXT,
    store_type    TEXT,
    opened_date   DATE,
    updated_at    TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS products (
    product_id    INT PRIMARY KEY,
    product_name  TEXT,
    category      TEXT,
    sub_category  TEXT,
    brand         TEXT,
    unit_price    NUMERIC(10,2),
    is_active     BOOLEAN,
    updated_at    TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS employees (
    employee_id   INT PRIMARY KEY,
    first_name    TEXT,
    last_name     TEXT,
    store_id      INT REFERENCES stores(store_id),
    role          TEXT,
    hire_date     DATE,
    is_active     BOOLEAN,
    updated_at    TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id      INT PRIMARY KEY,
    customer_id   INT REFERENCES customers(customer_id),
    store_id      INT REFERENCES stores(store_id),
    employee_id   INT REFERENCES employees(employee_id),
    order_date    TIMESTAMP,
    order_status  TEXT,
    updated_at    TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id INT PRIMARY KEY,
    order_id      INT REFERENCES orders(order_id),
    product_id    INT REFERENCES products(product_id),
    quantity      INT,
    unit_price    NUMERIC(10,2),
    updated_at    TIMESTAMP NOT NULL DEFAULT now()
);
