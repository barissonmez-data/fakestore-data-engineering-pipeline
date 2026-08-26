select product_key, user_key, quantity, count(*)
from fakestore_warehouse.facts_a
group by product_key, user_key, quantity
having count(*) > 1;