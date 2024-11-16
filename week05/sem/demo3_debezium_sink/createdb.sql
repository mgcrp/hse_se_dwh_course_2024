CREATE TABLE public.manufacturers (
    manufacturer_id SERIAL,
    manufacturer_name VARCHAR(100) NOT NULL,
    manufacturer_legal_entity VARCHAR(100) NOT NULL,
    CONSTRAINT manufacturer_pk PRIMARY KEY (manufacturer_id)
)
;

CREATE TABLE public.categories (
    category_id SERIAL,
    category_name VARCHAR(100) NOT NULL,
    CONSTRAINT category_pk PRIMARY KEY (category_id)
)
;

CREATE TABLE public.products (
    category_id BIGINT,
    manufacturer_id BIGINT,
    product_id SERIAL,
    product_name VARCHAR(255) NOT NULL,
    product_picture_url VARCHAR(255) NOT NULL,
    product_description VARCHAR(255) NOT NULL,
    product_age_restriction INTEGER NOT NULL,
    CONSTRAINT product_pk PRIMARY KEY (product_id),
    CONSTRAINT category_fk FOREIGN KEY (category_id) REFERENCES categories(category_id),
    CONSTRAINT manufacturer_fk FOREIGN KEY (manufacturer_id) REFERENCES manufacturers(manufacturer_id)
)
;

CREATE TABLE public.stores (
    store_id SERIAL,
    store_name VARCHAR(255) NOT NULL,
    store_country VARCHAR(255) NOT NULL,
    store_city VARCHAR(255) NOT NULL,
    store_address VARCHAR(255) NOT NULL,
    CONSTRAINT store_pk PRIMARY KEY (store_id)
)
;

CREATE TABLE public.customers (
    customer_id SERIAL,
    customer_fname VARCHAR(100) NOT NULL,
    customer_lname VARCHAR(100) NOT NULL,
    customer_gender VARCHAR(100) NOT NULL,
    customer_phone VARCHAR(100) NOT NULL,
    CONSTRAINT customer_pk PRIMARY KEY (customer_id)
)
;

CREATE TABLE public.price_change (
    product_id BIGINT NOT NULL,
    price_change_ts TIMESTAMP NOT NULL,
    new_price NUMERIC(9, 2) NOT NULL,
    CONSTRAINT product_pk2 PRIMARY KEY (product_id)
)
;

CREATE TABLE public.deliveries (
    delivery_id BIGINT NOT NULL,
    store_id BIGINT,
    product_id BIGINT NOT NULL,
    delivery_date DATE NOT NULL,
    product_count INTEGER NOT NULL,
    CONSTRAINT delivery_pk PRIMARY KEY (delivery_id),
    CONSTRAINT store_fk FOREIGN KEY (store_id) REFERENCES stores(store_id),
    CONSTRAINT product_fk FOREIGN KEY (product_id) REFERENCES price_change(product_id)
)
;

CREATE TABLE public.purchases (
    store_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    purchase_id SERIAL,
    purchase_date TIMESTAMP NOT NULL,
    purchase_payment_type VARCHAR(100) NOT NULL,
    CONSTRAINT store_fk2 FOREIGN KEY (store_id) REFERENCES stores(store_id),
    CONSTRAINT customer_fk FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    CONSTRAINT purchase_pk PRIMARY KEY (purchase_id)
)
;

CREATE TABLE public.purchase_items (
    product_id BIGINT NOT NULL,
    purchase_id BIGINT NOT NULL,
    product_count BIGINT NOT NULL,
    product_price NUMERIC(9, 2) NOT NULL,
    CONSTRAINT product_fk2 FOREIGN KEY (product_id) REFERENCES products(product_id),
    CONSTRAINT purchase_fk FOREIGN KEY (purchase_id) REFERENCES purchases(purchase_id),
    CONSTRAINT purchase_items_pk PRIMARY KEY (product_id, purchase_id)
)
;

CREATE VIEW public.gross_merchandise_value AS
SELECT
    pc.store_id AS store_id,
    pr.category_id AS category_id,
    SUM(pi.product_price * pi.product_count) AS sales_sum
FROM purchases pc 
JOIN purchase_items pi 
    ON pc.purchase_id = pi.purchase_id
JOIN products pr 
    ON pi.product_id = pr.product_id
GROUP BY
    pc.store_id, pr.category_id
ORDER BY
    pc.store_id, pr.category_id
;

alter table public.manufacturers replica identity full;
alter table public.categories replica identity full;
alter table public.products  replica identity full;
alter table public.stores  replica identity full;
alter table public.customers  replica identity full;
alter table public.price_change replica identity full;
alter table public.deliveries replica identity full;
alter table public.purchases replica identity full;
alter table public.purchase_items replica identity full;
