# sql script

insert into customers(customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax)
values (
'Demo1', 'Demo Company', 'Jane Doe', 'Owner', '123 Demo street', 'London', NULL, 'E1 7HX', 'UK',
'01-555-0100', NULL
)

select * from customers
where customer_id = 'Demo1'

update customers
set phone = '01-555-9999', city='Edinbrugh'
where customer_id = 'Demo1'

delete from customers
where customer_id = 'Demo1'

create table customer_notes (
note_id serial PRIMARY KEY,
customer_id varchar(5) not null REFERENCES customers(customer_id),
note text not null,
created_at TIMESTAMP not null DEFAULT now())