--создаём таблицу transaction min с одной колонкой, остальные создадутся автоматически во время импорта
CREATE TABLE public."transaction" (transaction_id INTEGER primary key)

--задаём ограничения колонок
ALTER TABLE public."transaction" ALTER COLUMN product_id SET NOT NULL;
ALTER TABLE public."transaction" ALTER COLUMN customer_id SET NOT NULL;
ALTER TABLE public."transaction" ALTER COLUMN order_status SET NOT NULL;

--задём корректные типы колонкам
ALTER TABLE public."transaction" ALTER COLUMN list_price TYPE numeric(10, 2) USING list_price::numeric(10, 2);
ALTER TABLE public."transaction" ALTER COLUMN standard_cost TYPE numeric(10, 2) USING standard_cost::numeric(10, 2);
ALTER TABLE public."transaction" ALTER COLUMN transaction_date TYPE date USING to_date(transaction_date, 'MM-DD-YY');

--создаём таблицу customer min с одной колонкой, остальные создадутся автоматически во время импорта
CREATE TABLE public."customer" (customer_id INTEGER primary key)

--задаём ограничения колонок
ALTER TABLE public.customer ALTER COLUMN first_name SET NOT NULL;
ALTER TABLE public.customer ALTER COLUMN last_name SET NOT NULL;

--задём корректные типы колонкам
ALTER TABLE public.customer ALTER COLUMN dob TYPE date USING to_date(dob, 'YYYY-MM-DD');
ALTER TABLE public.customer ALTER COLUMN address TYPE varchar(200) USING address::varchar(200);

--устраняем транзитивные зависимости из таблицы transaction
-- 1. Создаём таблицу Product
CREATE TABLE public."product" (
    product_id int4 PRIMARY KEY,
    brand varchar(50),
    product_line varchar(50),
    product_class varchar(50),
    product_size varchar(50)
);

-- 2. Копируем уникальные продукты из transaction (чтобы не было ошибок дублирования pk)
with  unique_products as  (
	select product_id
	from public."transaction"
	group by product_id
	having count(distinct brand || '|' || product_line || '|' || product_class || '|' || product_size) = 1
)
insert into public."product" (product_id, brand, product_line, product_class, product_size)
select distinct product_id, brand, product_line, product_class, product_size
from public."transaction"
where product_id in (select product_id from unique_products);

-- 2.1 Для не уникальных берём первую подошедшую строку (для упрощения очистки данных)
-- Отбрасываем 0, т.к. по логике таблицы это аналог продукта для которого id не известен
with first_acceptable_for_duplicates as (select distinct on (t.product_id)
    t.product_id,
    t.brand,
    t.product_line,
    t.product_class,
    t.product_size
from public."transaction" t
join (
    select product_id
    from public."transaction"
    group by product_id
    having count(distinct brand || '|' || product_line || '|' || product_class || '|' || product_size) > 1
) u on t.product_id = u.product_id
order by t.product_id, t.transaction_id
)
insert into public."product" (product_id, brand, product_line, product_class, product_size)
select product_id, brand, product_line, product_class, product_size
from first_acceptable_for_duplicates
where product_id <> 0;

-- 3. Обрезаем колонки в таблице transactions
ALTER TABLE public."transaction"
DROP COLUMN brand,
DROP COLUMN product_line,
DROP COLUMN product_class,
DROP COLUMN product_size;

-- 3.1 Оцениваем потери
select count(*)
from public."transaction" t
left join public."product" p on t.product_id = p.product_id 
where p.product_id is null;

--устраняем транзитивные зависимости для таблицы customer
--создадим таблицу location
CREATE TABLE public.location (
    location_id SERIAL PRIMARY KEY,
    address varchar(200) NOT NULL,
    postcode int4 NOT NULL,
    state varchar(50) NOT NULL,
    country varchar(50) NOT NULL,
    property_valuation int4 NULL
);
--заполним её уникальными значениями
insert into public."location" (address, postcode, state, country, property_valuation)
select distinct address, postcode, state, country, property_valuation
from public.customer;
--добавим в таблицу customer колонку location_id
ALTER TABLE public.customer ADD location_id int4;
--заполним location_id значениями location_id из таблицы location
update customer c 
set location_id = l.location_id
from "location" l
where c.address = l.address 
and c.postcode = l.postcode 
and c.state = l.state 
and c.country = l.country
and c.property_valuation = l.property_valuation;
--смотрим, есть ли не заполненные поля location_id в таблице customer, чтобы проверить, были ли все данные перенесены
select *
from customer
where location_id is null;
--удаляем колонки из таблицы customer, которые перенесли в таблицу location
ALTER TABLE public.customer 
DROP COLUMN address,
DROP COLUMN postcode,
DROP COLUMN state,
DROP COLUMN country,
DROP COLUMN property_valuation;
