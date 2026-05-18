create table dim_store
(
    store_id       serial primary key,
    store_name     text not null,
    store_location text not null,
    store_city     text not null,
    store_state    text,
    store_country  text not null,
    store_phone    text unique,
    store_email    text unique
);

create table dim_customer_pet
(
    pet_id             serial primary key,
    customer_pet_type  text not null,
    customer_pet_name  text not null,
    customer_pet_breed text not null
);

create table dim_customer
(
    customer_id          serial primary key,
    customer_first_name  text                                     not null,
    customer_last_name   text                                     not null,
    customer_age         int                                      not null,
    customer_email       text unique,
    customer_country     text                                     not null,
    customer_postal_code text,
    customer_pet_id      int references dim_customer_pet (pet_id) not null
);

create table dim_seller
(
    seller_id          serial primary key,
    seller_first_name  text not null,
    seller_last_name   text not null,
    seller_email       text unique,
    seller_country     text not null,
    seller_postal_code text
);

create table dim_supplier
(
    supplier_id      serial primary key,
    supplier_name    text not null,
    supplier_contact text not null,
    supplier_email   text unique,
    supplier_phone   text unique,
    supplier_address text not null,
    supplier_city    text not null,
    supplier_country text not null
);

create table dim_product
(
    product_id           serial primary key,
    product_name         text          not null,
    product_category     text          not null,
    product_price        numeric(5, 2) not null,
    product_quantity     int           not null,
    product_weight       numeric(5, 2),
    product_color        text          not null,
    product_size         text          not null,
    product_brand        text          not null,
    product_material     text          not null,
    product_description  text          not null,
    product_rating       numeric(5, 2) not null,
    product_reviews      int           not null,
    product_release_date date          not null,
    product_expiry_date  date          not null
);

create table fact_sale
(
    sale_id          serial primary key,
    sale_date        date,
    sale_customer_id int references dim_customer (customer_id),
    sale_seller_id   int references dim_seller (seller_id),
    sale_product_id  int references dim_product (product_id),
    sale_store_id    int references dim_store (store_id),
    sale_supplier_id int references dim_supplier (supplier_id),
    sale_quantity    int,
    sale_total_price numeric(5, 2),
    pet_category     text
);