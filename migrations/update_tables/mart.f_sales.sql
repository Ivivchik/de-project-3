DELETE FROM mart.f_sales
WHERE date_id::TEXT::DATE IN (
	SELECT DISTINCT date_time::DATE
    FROM staging.user_order_log
    WHERE date_time::DATE = '{{ds}}');

INSERT INTO mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount)
    SELECT DISTINCT dc.date_id,
           item_id,
           customer_id,
           city_id,
           quantity, 
           CASE
           WHEN "status" = 'refunded'
           THEN payment_amount * -1
           ELSE payment_amount
           END AS payment_amount
           FROM staging.user_order_log uol
           LEFT JOIN mart.d_calendar AS dc ON uol.date_time::DATE = dc.date_actual
           WHERE uol.date_time::DATE = '{{ds}}';