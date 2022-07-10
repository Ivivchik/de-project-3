alter table staging.user_order_log
add column "status" varchar(8);


alter table staging.user_order_log
add constarint status_constraint check ("status" is null or "status" in ('shipped', 'refunded'));
