-- Runs once, the first time the cdc-postgres container starts with an empty volume.
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    city        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id    SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers (customer_id),
    status      TEXT NOT NULL DEFAULT 'created',
    amount      NUMERIC(10, 2) NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- FULL: updates and deletes carry the whole old row ("before"), not just the key.
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;

INSERT INTO customers (name, city) VALUES
    ('Ayesha Rahman', 'Dhaka'),
    ('Tanvir Hasan', 'Chattogram'),
    ('Nusrat Jahan', 'Sylhet'),
    ('Rafiq Islam', 'Khulna'),
    ('Sadia Akter', 'Rajshahi');

INSERT INTO orders (customer_id, status, amount) VALUES
    (1, 'paid', 1250.00),
    (2, 'created', 499.50),
    (3, 'shipped', 3200.00);