select count(*) from transportation.default.orders;

alter table transportation.default.orders
add columns (cost_price INT);

UPDATE transportation.default.orders
SET cost_price=300
WHERE order_id IN (1,2,4);


UPDATE transportation.default.orders
SET cost_price=700
WHERE order_id IN (3,5);

alter table transportation.default.orders
add columns (Profit INT);

