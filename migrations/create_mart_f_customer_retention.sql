CREATE TABLE IF NOT EXISTS mart.f_customer_retention(
    id serial PRIMARY KEY,
    new_customers_count INT,
    returning_customers_count INT,
    refunded_customer_count INT,
    period_name VARCHAR(6) DEFAULT 'weekly', 
    period_id INT,
    item_id INT,
    new_customers_revenue NUMERIC(10, 2),
    returning_customers_revenue NUMERIC(10, 2),
    customers_refunded NUMERIC(10, 2),
                                        CHECK (period_name = 'weekly')
);