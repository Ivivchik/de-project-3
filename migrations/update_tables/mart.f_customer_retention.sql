DELETE FROM mart.f_customer_retention
WHERE period_id IN (
	SELECT DISTINCT dc.week_of_year
    FROM staging.user_order_log uol
    JOIN mart.d_calendar dc on dc.date_actual = date_time::DATE
    WHERE date_time::DATE = '{{ds}}');


WITH t1 AS(
        SELECT DISTINCT dc.date_id,
                        item_id,
                        customer_id,
                        city_id,
                        quantity, 
                        CASE
                        WHEN "status" = 'refunded'
                        THEN payment_amount * -1
                        ELSE payment_amount
                        END AS payment_amount,
                        "status",
                        week_of_year
        FROM staging.user_order_log uol
        LEFT JOIN mart.d_calENDar AS dc ON uol.date_time::DATE = dc.date_actual
        WHERE uol.date_time::DATE = '{{ds}}'),
    t2 AS (
        SELECT customer_id,
               week_of_year,
               item_id,
               COUNT(CASE WHEN STATUS != 'refunded' THEN item_id END) AS non_refunded_count_order, 
               COUNT(CASE WHEN STATUS = 'refunded' THEN item_id END) AS customers_refunded , 
               SUM(payment_amount) AS sum_payment_amount
        FROM t1 GROUP BY week_of_year, customer_id,  item_id),
    t3 AS (
        SELECT *,
        CASE 
        WHEN customers_refunded >= 1 THEN 1
        WHEN non_refunded_count_order = 1 THEN 2
        WHEN non_refunded_count_order > 1 THEN 3
        END AS group_flag
        FROM t2)


INSERT INTO mart.f_customer_retention(new_customers_count, 
                                      returning_customers_count,
                                      refunded_customer_count,
                                      period_id,
                                      item_id,
                                      new_customers_revenue,
                                      returning_customers_revenue,
                                      customers_refunded)
    select  COUNT(CASE WHEN group_flag = 2 THEN customer_id END) AS new_customers_count,
            COUNT(CASE WHEN group_flag = 3 THEN customer_id END) AS returning_customers_count,
            COUNT(CASE WHEN group_flag = 1 THEN customer_id END) AS refunded_customer_count,
            week_of_year,
            item_id,
            SUM(CASE WHEN group_flag = 2 THEN sum_payment_amount ELSE 0 END) AS new_customers_revenue,
            SUM(CASE WHEN group_flag = 3 THEN sum_payment_amount ELSE 0 END) AS returning_customers_revenue,
            SUM(CASE when group_flag = 1 THEN customers_refunded ELSE 0 END) AS customers_refunded
            FROM t3 group by week_of_year, item_id;
