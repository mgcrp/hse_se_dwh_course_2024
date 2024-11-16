INSERT INTO manufacturers (manufacturer_name, manufacturer_legal_entity) VALUES
('Manufacturer A', 'OOO A'),
('Manufacturer B', 'OAO B'),
('Manufacturer C', 'IP C');

INSERT INTO categories (category_name) VALUES
('Electronics'),
('Clothing'),
('Groceries');

INSERT INTO products (
    product_id, product_name, product_picture_url, product_description, product_age_restriction, 
    category_id, manufacturer_id
) VALUES
(1, 'Smartphone', 'data:image/jpeg;base64,/9j/4AAQSk', 'lol', 12, 1, 1),
(2, 'T-Shirt', 'data:image/jpeg;base64,/9j/IJHDSUD', 'kek', 14, 2, 2),
(3, 'Bread', 'data:image/jpeg;base64,/9j/I6SSYA', 'puk', 0, 3, 3);

INSERT INTO stores (store_name, store_country, store_city, store_address) VALUES
('Store X', 'Russia', 'Moscow', '1 Chapel Hill'),
('Store Y', 'Belarus', 'Minsk', '438 DARK SPURT'),
('Store Z', 'Ukraine', 'Kiev', 'ul. Kosmonavtov 35-11');

INSERT INTO price_change (product_id, price_change_ts, new_price) VALUES
(1, '2023-10-15 12:00:00', 520.00),
(2, '2023-10-15 13:00:00', 14.00);

INSERT INTO deliveries (delivery_id, store_id, product_id, delivery_date, product_count) VALUES
(1, 1, 1, '2023-10-15', 100),
(2, 2, 2, '2023-10-14', 200);

INSERT INTO customers (customer_fname, customer_lname, customer_gender, customer_phone) VALUES
('John', 'Doe', 'man', '88005553535'),
('Jane', 'Smith', 'woman', '88006664548'),
('Alice', 'Johnson', 'woman', '89155673451');

INSERT INTO purchases (store_id, customer_id, purchase_date, purchase_payment_type) VALUES
(1, 1, '2023-10-15', 'cash'),
(2, 2, '2023-10-15', 'card'),
(3, 3, '2023-10-14', 'cash');

INSERT INTO purchase_items (product_id, purchase_id, product_count, product_price) VALUES
(1, 1, 2, 500.00),
(2, 2, 3, 15.00),
(3, 3, 5, 2.00);