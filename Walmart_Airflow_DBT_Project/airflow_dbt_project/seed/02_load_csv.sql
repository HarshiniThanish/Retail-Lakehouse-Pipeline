-- Loads the sample Walmart dataset CSVs into the tables created above.
\copy customers   FROM '/docker-entrypoint-initdb.d/walmart_dataset/customers.csv'   WITH (FORMAT csv, HEADER true);
\copy stores      FROM '/docker-entrypoint-initdb.d/walmart_dataset/stores.csv'      WITH (FORMAT csv, HEADER true);
\copy products    FROM '/docker-entrypoint-initdb.d/walmart_dataset/products.csv'    WITH (FORMAT csv, HEADER true);
\copy employees   FROM '/docker-entrypoint-initdb.d/walmart_dataset/employees.csv'   WITH (FORMAT csv, HEADER true);
\copy orders      FROM '/docker-entrypoint-initdb.d/walmart_dataset/orders.csv'      WITH (FORMAT csv, HEADER true);
\copy order_items FROM '/docker-entrypoint-initdb.d/walmart_dataset/order_items.csv' WITH (FORMAT csv, HEADER true);
