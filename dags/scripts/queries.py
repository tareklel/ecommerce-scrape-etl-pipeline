create_staging_ounass = """
CREATE TABLE IF NOT EXISTS ounass_staging (
    site varchar, 
    crawl_date date, 
    country varchar, 
    url varchar, 
    portal_itemid int,
    product_name varchar,
    gender varchar, 
    brand varchar,
    category varchar, 
    subcategory varchar, 
    price varchar, 
    currency varchar,
    price_discount float, 
    sold_out boolean, 
    primary_label varchar,
    image_url varchar, 
    text varchar);
"""

create_staging_farfetch = """
CREATE TABLE IF NOT EXISTS farfetch_staging (
    site varchar, 
    crawl_date date, 
    country varchar, 
    url varchar, 
    portal_itemid int,
    product_name varchar,
    gender varchar, 
    brand varchar,
    category varchar, 
    subcategory varchar, 
    price varchar, 
    currency varchar,
    price_discount float, 
    sold_out boolean, 
    primary_label varchar,
    image_url varchar, 
    text varchar);
"""

create_factOunass = """
CREATE TABLE IF NOT EXISTS fact_ounass (
    ounass_scrape_id varchar NOT NULL,
    crawl_date date, 
    country varchar, 
    ounass_product_id int,
    gender varchar, 
    brand varchar,
    category varchar, 
    price int, 
    currency varchar,
    price_discount float,
    CONSTRAINT factOunass_pkey PRIMARY KEY (ounass_scrape_id)
    );
"""

create_factFarfetch = """
CREATE TABLE IF NOT EXISTS factFarfetch (
    farfetch_scrape_id varchar NOT NULL,
    crawl_date date, 
    country varchar, 
    farfetch_product_id int,
    gender varchar, 
    brand varchar,
    category varchar, 
    price int, 
    currency varchar,
    price_discount float,
    CONSTRAINT factFarfetch_pkey PRIMARY KEY (farfetch_scrape_id)
    );
"""

create_dimBrand = """
CREATE TABLE IF NOT EXISTS dimBrand(
    brand varchar NOT NULL,
    ounass_brand varchar,
    farfetch_brand varchar,
    CONSTRAINT dimBrand_pkey PRIMARY KEY (brand)
);
"""
create_dimCategory = """
CREATE TABLE IF NOT EXISTS dimCategory(
    category varchar NOT NULL,
    ounass_category varchar,
    farfetch_category varchar,
    CONSTRAINT dimCategory_pkey PRIMARY KEY (category)
);
"""
create_dimGender = """
CREATE TABLE IF NOT EXISTS dimGender(
    gender varchar NOT NULL,
    ounass_gender varchar,
    farfetch_gender varchar,
    CONSTRAINT dimGender_pkey PRIMARY KEY (gender)
);
"""
create_obtBrandPricing = """
CREATE TABLE IF NOT EXISTS obtBrandPricing(
    crawl_date date,
    brand varchar,
    category varchar,
    country varchar,
    count_products int,
    avg_price float,
    max_price float,
    min_price float,
    count_discounts int,
    perc_discounted float,
    avg_discount float,
    max_discount float
    );
"""

drop_tables = """
DROP TABLE IF EXISTS ounass_staging;
DROP TABLE IF EXISTS farfetch_staging;
DROP TABLE IF EXISTS factOunass;
DROP TABLE IF EXISTS factFarfetch;
DROP TABLE IF EXISTS dimBrand;
DROP TABLE IF EXISTS dimCategory;
DROP TABLE IF EXISTS dimGender;
DROP TABLE IF EXISTS obtBrandPricing;
"""

create_tables = [
    create_staging_ounass, 
    create_staging_farfetch, 
    create_factFarfetch,
    create_factOunass,
    create_dimBrand,
    create_dimCategory,
    create_dimGender,
    create_obtBrandPricing
]

create_all_tables = ' '.join(str(x) for x in create_tables)
