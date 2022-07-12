CREATE TABLE IF NOT EXISTS staging.user_order_log_inc (
	id INT,
	date_time TIMESTAMP,
	city_id INT,
	city_name VARCHAR(100),
	customer_id INT ,
	first_name VARCHAR(100),
	last_name VARCHAR(100),
	item_id INT,
	item_name VARCHAR(100),
	quantity BIGINT,
	payment_amount NUMERIC(10, 2),
	"status" VARCHAR(8) DEFAULT NULL
);