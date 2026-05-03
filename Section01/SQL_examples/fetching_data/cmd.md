# sql scripts
select product_id, product_name from products
order by product_id

select * from products
order by product_id

select * from products
where unit_price < 10
order by product_id

select * from products
where unit_price < 10
and units_in_stock > 0
order by product_id DESC


select * from products
order by unit_price DESC
LIMIT 10


select max(unit_price) from products

select max(unit_price) as max_price from products