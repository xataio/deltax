-- RTABench schema (5 tables).
-- Mirrors upstream rtabench's postgres/create.sql, with one change:
-- orders.order_id is plain integer (not serial) since the CSV provides IDs.

CREATE TABLE customers (
    customer_id integer NOT NULL PRIMARY KEY,
    name        text,
    birthday    date,
    email       text,
    address     text,
    city        text,
    zip         text,
    state       text,
    country     text
);

CREATE TABLE products (
    product_id  integer NOT NULL PRIMARY KEY,
    name        text,
    description text,
    category    text,
    price       decimal(10,2),
    stock       int
);

CREATE TABLE orders (
    order_id    integer     NOT NULL PRIMARY KEY,
    customer_id integer     NOT NULL,
    created_at  timestamptz NOT NULL
);

CREATE TABLE order_items (
    order_id   integer NOT NULL,
    product_id integer NOT NULL,
    amount     integer NOT NULL,
    PRIMARY KEY (order_id, product_id)
);

CREATE TABLE order_events (
    order_id         integer     NOT NULL,
    counter          integer,
    event_created    timestamptz NOT NULL,
    event_type       text        NOT NULL,
    satisfaction     real        NOT NULL,
    processor        text        NOT NULL,
    backup_processor text,
    event_payload    jsonb
);
