-- 1. Вывести распределение (количество) клиентов по сферам деятельности, отсортировав результат по убыванию количества.
select 
	coalesce(c.job_industry_category, 'Unknown'),
	count(*) as cnt
from customer c 
group by coalesce(c.job_industry_category, 'Unknown')
order by cnt desc
;

-- 2. Найти общую сумму дохода (list_price*quantity) по всем подтвержденным заказам за каждый месяц по сферам деятельности клиентов.
-- Отсортировать результат по году, месяцу и сфере деятельности.
with orders_with_revenue as (
	select (coalesce(oi.item_list_price_at_sale, 0) * coalesce(oi.quantity, 0))::numeric as revenue
		,o.customer_id 
		,extract (year from o.order_date) as year
		,extract (month from o.order_date) as month
	from orders o
	join order_items oi using (order_id)
	where o.order_status = 'Approved'
)
select
	 ov."year" 
	,ov."month"
	,coalesce(c.job_industry_category, 'Unknown')
	,sum(ov.revenue) as total_revenue
from orders_with_revenue ov
join customer c using (customer_id)
group by ov.month, ov.year, coalesce(c.job_industry_category, 'Unknown')
order by ov.year, ov.month, coalesce(c.job_industry_category, 'Unknown')
;

-- 3. Вывести количество уникальных онлайн-заказов для всех брендов в рамках подтвержденных заказов клиентов из сферы IT.
-- Включить бренды, у которых нет онлайн-заказов от IT-клиентов, — для них должно быть указано количество 0.

-- для проверки скрипта
INSERT INTO public.product_cor
(product_id, brand, product_line, product_class, product_size, list_price, standard_cost, rn)
VALUES(101, 'Test', 'Road', 'medium', 'medium', 742.54, 667.4, 1);

with it_online_orders as (
	select 
		distinct o.order_id
		,pc.brand 
	from
		orders o
	join
		customer c using (customer_id)
	join 
		order_items oi using (order_id)
	join 
		product_cor pc using(product_id)
	where 
		c.job_industry_category = 'IT'
		and o.order_status = 'Approved'
		and o.online_order is true
)
SELECT 
    pc.brand,
    count(distinct bo.order_id) as unique_online_orders
from product_cor pc
left join it_online_orders bo using (brand)
group by pc.brand
order by pc.brand
;

--очищаем данные
DELETE FROM public.product_cor
WHERE product_id=101;

-- 4. Найти по всем клиентам: сумму всех заказов (общего дохода), максимум, минимум и количество заказов, а также среднюю сумму заказа по каждому клиенту. 
-- Отсортировать результат по убыванию суммы всех заказов и количества заказов.
-- Выполнить двумя способами: используя только GROUP BY и используя только оконные функции. Сравнить результат.
with first_query as (
	select
		o.customer_id
		,sum(coalesce(oi.quantity,0) * coalesce(oi.item_list_price_at_sale,0)) over(w)as sum_sales
		,max(coalesce(oi.quantity,0) * coalesce(oi.item_list_price_at_sale,0)) over(w) as max_sales
		,min(coalesce(oi.quantity,0) * coalesce(oi.item_list_price_at_sale,0)) over(w) as min_sales
		,count(o.order_id) over(w) as cnt_orders
		,avg(coalesce(oi.quantity,0) * coalesce(oi.item_list_price_at_sale,0)) over(w) as avg_sum
		,row_number() over (partition by customer_id) as rn
	from orders o
	join order_items oi using (order_id)
	where o.order_status = 'Approved'
	window w as (partition by o.customer_id)
	order by sum_sales desc, cnt_orders desc
),
first_query_ranked as (
	select 
		customer_id
		,sum_sales
		,max_sales
		,min_sales
		,cnt_orders
		,avg_sum
	from first_query
	where rn = 1
),
second_query as (
	select 
		o.customer_id
		,sum(coalesce(oi.quantity,0) * coalesce(oi.item_list_price_at_sale,0)) as sum_sales
		,max(coalesce(oi.quantity,0) * coalesce(oi.item_list_price_at_sale,0)) as max_sales
		,min(coalesce(oi.quantity,0) * coalesce(oi.item_list_price_at_sale,0)) as min_sales
		,count(distinct o.order_id) as cnt_orders
		,avg(coalesce(oi.quantity,0) * coalesce(oi.item_list_price_at_sale,0)) as avg_sum
	from orders o
	join order_items oi using (order_id)
	where o.order_status = 'Approved'
	group by o.customer_id
	order by sum_sales desc, cnt_orders desc
)
--приведение к типу char для корректного сравнения
select
	customer_id
	,round(sum_sales)::char
	,round(max_sales)::char
	,round(min_sales)::char
	,cnt_orders
	,round(avg_sum)::char
from first_query_ranked 
except
select 
	customer_id
	,round(sum_sales)::char
	,round(max_sales)::char
	,round(min_sales)::char
	,cnt_orders
	,round(avg_sum)::char
from second_query 
;

-- 5. Найти имена и фамилии клиентов с топ-3 минимальной и топ-3 максимальной суммой транзакций за весь период (учесть клиентов, у которых нет заказов, приняв их сумму транзакций за 0).
with customer_orders as (
	select
		c.customer_id
		,c.first_name
		,c.last_name
		,sum(coalesce(oi.quantity,0) * coalesce(oi.item_list_price_at_sale,0))::numeric as sum_sales
	from customer c
	left join orders o on o.customer_id = c.customer_id
		and o.order_status = 'Approved'
	left join order_items oi using (order_id)
	group by c.customer_id, c.first_name, c.last_name
),
customers_ranked_min as (
	select *
 		,dense_rank() over (order by sum_sales) as dr
	from customer_orders 
),
customers_ranked_max as (
	select *
 		,dense_rank() over (order by sum_sales desc) as dr
	from customer_orders 
)
SELECT * FROM customers_ranked_min where dr <= 3
union all
SELECT * FROM customers_ranked_max where dr <= 3
;

-- 6. Вывести только вторые транзакции клиентов (если они есть) с помощью оконных функций. Если у клиента меньше двух транзакций, он не должен попасть в результат.
with orders_with_row as (
	select
		o.*
		,row_number() over(partition by o.customer_id order by o.order_date)
	from orders o
	where o.order_status = 'Approved'
)
select *
from orders_with_row
where row_number = 2
;

-- 7. Вывести имена, фамилии и профессии клиентов, а также длительность максимального интервала (в днях) между двумя последовательными заказами. 
-- Исключить клиентов, у которых только один или меньше заказов.
with orders_with_previous as (
	select
		o.customer_id
		,o.order_date - LAG(o.order_date) OVER(PARTITION BY o.customer_id ORDER BY o.order_date) as time_diff
	from orders o
),
customers_with_max_time_diff as (
	select
		o.customer_id
		,max(o.time_diff) max_interval
	from orders_with_previous o
	where time_diff is not null
	group by o.customer_id
)
select c.first_name
		,c.last_name
		,c.job_title
		,cr.max_interval
from customer c
join customers_with_max_time_diff cr using(customer_id)
;
-- 8. Найти топ-5 клиентов (по общему доходу) в каждом сегменте благосостояния (wealth_segment). 
-- Вывести имя, фамилию, сегмент и общий доход. Если в сегменте менее 5 клиентов, вывести всех.
with customer_with_total_revenue as (
	select 
			c.customer_id
			,c.wealth_segment 
			,sum(coalesce(oi.quantity,0) * coalesce(oi.item_list_price_at_sale,0)) as sum_sales
		from customer c
		join orders o using (customer_id)
		join order_items oi using (order_id)
		where o.order_status = 'Approved'
		group by c.customer_id, c.wealth_segment
),
customer_ranked as (
	select 
		cwr.customer_id
		,cwr.wealth_segment
		,cwr.sum_sales 
		,dense_rank() over (partition by cwr.wealth_segment order by cwr.sum_sales desc) as dr
	from customer_with_total_revenue cwr
),
customer_top_ids as (
	select
		customer_id
		,wealth_segment 
		,sum_sales 
	from customer_ranked
	where dr <= 5
)
select
	c.first_name
	,c.last_name
	,c.wealth_segment
	,cwr.sum_sales 
from customer c
join customer_top_ids cwr on c.customer_id = cwr.customer_id 
	and c.wealth_segment = cwr.wealth_segment
order by c.wealth_segment, cwr.sum_sales desc
;
