# sql scripts

select c.country, count(*) as customers_count from customers c
group by c.country

select c.city, count(*) as customers_count from customers c
where c.country = 'Brazil'
group by c.city
order by customers_count DESC

select c.country, c.city, count(*) as customers_count from customers c
group by c.country, c.city
order by customers_count DESC