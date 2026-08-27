-- check for dupicate rows by product_key , user_key , quantity
select product_key, user_key, quantity, count(*)
from fakestore_warehouse.facts_a
group by product_key, user_key, quantity
having count(*) > 1;




-- we checked missing product_keys 
select * from fakestore_warehouse.facts_a
where product_key is null;



-- checeked  for missing user_keys after join
select * from fakestore_warehouse.facts_a
where user_key is null;