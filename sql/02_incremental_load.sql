
CREATE TABLE water_mark (
    last_load TIMESTAMP
);

--initial watermark value
insert into water_mark values('1900-01-01') 


-- increental load: insert new rows using watermark filter 
INSERT INTO fakestore_warehouse.facts_a (quantity, price, product_key, user_key, date_id)
SELECT stg_carts.quantity, stg_products.price, product_dim.product_key, user_dim.user_key, date_dim.date_id
FROM fakestore_project.stg_carts
JOIN fakestore_project.stg_products ON stg_products.id = stg_carts."productId"
JOIN fakestore_warehouse.product_dim ON stg_carts."productId" = product_dim.productid
JOIN fakestore_warehouse.user_dim ON stg_carts."userId" = user_dim.userid
JOIN fakestore_warehouse.date_dim ON stg_carts.date::timestamp = date_dim.date
WHERE stg_carts.date::timestamp > (SELECT last_load FROM fakestore_warehouse.water_mark);
