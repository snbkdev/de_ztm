# sql scripts

select p.product_id, p.product_name, c.category_name from products p
join categories c on c.category_id = p.category_id
order by p.product_id