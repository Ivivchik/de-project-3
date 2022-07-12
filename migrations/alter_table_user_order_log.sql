alter table staging.user_order_log
add column "status" varchar(8);

alter table staging.user_order_log
add constraint status_constraint check ("status" is null or "status" in ('shipped', 'refunded'));

alter table mart.f_sales
add column "status" varchar(8);

alter table mart.f_sales
add constraint f_sales_status_constraint check ("status" is null or "status" in ('shipped', 'refunded'));
