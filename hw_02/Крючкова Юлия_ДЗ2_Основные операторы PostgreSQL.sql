-- Создаём product_cor из уникальных product_id из таблицы product
create table product_cor as
 select *
 from (
  select *
   ,row_number() over(partition by product_id order by list_price desc) as rn
  from product)
 where rn = 1;

-- 1. Вывести все уникальные бренды, у которых есть хотя бы один продукт со стандартной стоимостью выше 1500 долларов, и суммарными продажами не менее 1000 единиц.
with exp_brands as (
	SELECT product_id, brand
    FROM product_cor
    WHERE standard_cost > 1500
),
sales_over_1000  as (
	select oi.product_id,
		   sum(coalesce(oi.quantity,0))
	from order_items oi
	group by oi.product_id
	having sum(coalesce(oi.quantity,0)) > 1000
)
select distinct ep.brand
from exp_brands ep
join sales_over_1000 s using (product_id);

-- 2. Для каждого дня в диапазоне с 2017-04-01 по 2017-04-09 включительно вывести количество подтвержденных онлайн-заказов и количество уникальных клиентов, совершивших эти заказы.
with approved_online_orders as (
	select *
	from orders o 
	where o.order_date between '2017-04-01' and '2017-04-09'
		and o.online_order = true
		and o.order_status = 'Approved'
)
select 
	order_date,
	count(*) as total,
	count(distinct customer_id) as unique_customer
from approved_online_orders
group by order_date;

-- 3. Вывести профессии клиентов:
-- из сферы IT, чья профессия начинается с Senior;
-- из сферы Financial Services, чья профессия начинается с Lead.
-- Для обеих групп учитывать только клиентов старше 35 лет. Объединить выборки с помощью UNION ALL.

select job_title
from customer
where job_industry_category = 'IT'
	and job_title like 'Senior%'
	and extract(year from age(dob)) > 35
union all
select job_title
from customer
where job_industry_category = 'Financial Services'
	and job_title like 'Lead%'
	and extract(year from age(dob)) > 35;

-- 3. Если старше 35 лет на момент заказа, а не на текущую дату:
select distinct c.job_title
from customer c
join orders o using (customer_id)
where c.job_industry_category = 'IT'
  and c.job_title like 'Senior%'
  and c.dob < o.order_date - interval '35 years'
  and o.order_date between '2017-01-01' and '2017-12-31'
union all
select distinct c.job_title
from customer c
join orders o using (customer_id)
where c.job_industry_category = 'Financial Services'
  and c.job_title like 'Lead%'
  and c.dob < o.order_date - interval '35 years'
  and o.order_date between '2017-01-01' and '2017-12-31';
	
-- 4. Вывести бренды, которые были куплены клиентами из сферы Financial Services, но не были куплены клиентами из сферы IT.
with brands as (
	select distinct pc.brand, c.job_industry_category
	from orders o
	join customer c using (customer_id)
	join order_items oi using (order_id)
	join product_cor pc using (product_id)
	where c.job_industry_category in ('Financial Services', 'IT')
)
select fbr.brand
from brands as fbr
left join brands as itbr
	on fbr.brand = itbr.brand
	and itbr.job_industry_category = 'IT'
where fbr.job_industry_category = 'Financial Services'
and itbr.brand is null;

-- 5. Вывести 10 клиентов (ID, имя, фамилия), которые совершили наибольшее количество онлайн-заказов (в штуках) брендов Giant Bicycles, Norco Bicycles, Trek Bicycles,
-- при условии, что они активны и имеют оценку имущества (property_valuation) выше среднего среди клиентов из того же штата.
with avg_property_valuation_by_state as (
	select state, avg(property_valuation) as avg_prop
	from customer
	group by state 
),
approved_online_orders as (
	select order_id, customer_id
	from orders
	where online_order = True
	and order_status = 'Approved'
),
brand_products as (
	select product_id
	from product_cor
	where brand in ('Giant Bicycles', 'Norco Bicycles', 'Trek Bicycles')
)
select c.customer_id, c.first_name, c.last_name, count(o.order_id) as total
from customer c
join approved_online_orders o using (customer_id)
join order_items oi using (order_id)
join brand_products pc using (product_id)
join avg_property_valuation_by_state as avg_state using (state)
where c.deceased_indicator = 'N'
and c.property_valuation > avg_state.avg_prop 
group by c.customer_id, c.first_name, c.last_name
order by total desc
limit 10;

-- 6. Вывести всех клиентов (ID, имя, фамилия), у которых нет подтвержденных онлайн-заказов за последний год,
-- но при этом они владеют автомобилем и их сегмент благосостояния не Mass Customer.
with customer_with_car_not_mass as (
	select customer_id, first_name, last_name
	from customer
	where owns_car = 'Yes'
		and wealth_segment <> 'Mass Customer'
),
customer_with_online_orders as (
	select customer_id
	from orders
	where extract(year from order_date) = EXTRACT(YEAR FROM (select max(order_date) from orders))
	and online_order = true
	and order_status = 'Approved'
)
select c.customer_id, first_name, last_name
from customer_with_car_not_mass c
left join customer_with_online_orders o on c.customer_id = o.customer_id
where o.customer_id is null

-- 7. Вывести всех клиентов из сферы 'IT' (ID, имя, фамилия), которые купили 2 из 5 продуктов с самой высокой list_price в продуктовой линейке Road.
with it_customers as (
	select customer_id,
		   first_name,
		   last_name
	from customer c
	where job_industry_category = 'IT'
),
highest_price_road as (
	select product_id
	from product_cor pc
	where pc.product_line = 'Road'
	order by pc.list_price desc
	limit 5
)
select it.customer_id,
	   it.first_name,
	   it.last_name
from it_customers it
join orders o using(customer_id)
join order_items oi using(order_id)
join highest_price_road pc using(product_id)
group by customer_id, it.first_name, it.last_name
having count(distinct pc.product_id) = 2

-- 8. Вывести клиентов (ID, имя, фамилия, сфера деятельности) из сфер IT или Health, которые совершили не менее 3 подтвержденных заказов в период 2017-01-01 по 2017-03-01,
-- и при этом их общий доход от этих заказов превышает 10 000 долларов.
-- Разделить вывод на две группы (IT и Health) с помощью UNION.
with approved_orders as (
	select customer_id, order_id
	from orders o
	where o.order_date between '2017-01-01' and '2017-03-01'
	and o.order_status = 'Approved'
)
select c.customer_id, c.first_name, c.last_name, c.job_industry_category 
from customer c
join approved_orders o using (customer_id)
join order_items oi using (order_id)
where job_industry_category = 'IT'
group by c.customer_id, c.first_name, c.last_name, c.job_industry_category 
having sum(coalesce(oi.item_list_price_at_sale,0) * coalesce(oi.quantity,0)) > 10000 and count(distinct o.order_id) >= 3
union all
select c.customer_id, c.first_name, c.last_name, c.job_industry_category 
from customer c
join approved_orders o using (customer_id)
join order_items oi using (order_id)
where job_industry_category = 'Health'
group by c.customer_id, c.first_name, c.last_name, c.job_industry_category 
having sum(coalesce(oi.item_list_price_at_sale,0) * coalesce(oi.quantity,0)) > 10000 and count(distinct o.order_id) >= 3
