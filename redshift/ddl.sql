CREATE SCHEMA IF NOT EXISTS dq_demo;

CREATE TABLE IF NOT EXISTS dq_demo.customer (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name  VARCHAR(100),
    email      VARCHAR(256),
    signup_date DATE,
    country     VARCHAR(50),
    lifetime_value DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS dq_demo.transactions (
    order_id VARCHAR(64) PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    amount DOUBLE PRECISION,
    payment_method VARCHAR(20),
    channel VARCHAR(20)
);
