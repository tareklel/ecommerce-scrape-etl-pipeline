create_staging_ounass = """
CREATE TABLE IF NOT EXISTS ounass_staging (
    site varchar, 
    crawl_date date, 
    country varchar, 
    url varchar, 
    portal_itemid varchar,
    product_name varchar,
    gender varchar, 
    brand varchar,
    category varchar, 
    subcategory varchar, 
    price varchar, 
    currency varchar,
    price_discount varchar, 
    sold_out boolean, 
    primary_label varchar,
    image_url varchar, 
    text varchar(max));
"""

create_staging_farfetch = """
CREATE TABLE IF NOT EXISTS farfetch_staging (
    site varchar, 
    crawl_date bigint, 
    country varchar, 
    url varchar,
    subfolder varchar,
    portal_itemid varchar,
    product_name varchar,
    gender varchar, 
    brand varchar,
    category varchar, 
    subcategory varchar, 
    price varchar, 
    currency varchar,
    price_discount varchar, 
    sold_out boolean, 
    primary_label varchar,
    image_url varchar, 
    text varchar(max));
"""

create_factOunass = """
CREATE TABLE IF NOT EXISTS factOunass (
    ounass_scrape_id INT IDENTITY(0,1) NOT NULL,
    crawl_date date, 
    country varchar, 
    ounass_product_id varchar,
    gender varchar, 
    brand varchar,
    category varchar, 
    price int, 
    currency varchar,
    CONSTRAINT factOunass1_pkey PRIMARY KEY (ounass_scrape_id)
    );
"""

create_factFarfetch = """
CREATE TABLE IF NOT EXISTS factFarfetch (
    farfetch_scrape_id INT IDENTITY(0,1) NOT NULL,
    crawl_date date, 
    country varchar, 
    farfetch_product_id varchar,
    gender varchar, 
    brand varchar,
    category varchar, 
    price int, 
    currency varchar,
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
    crawl_date date NOT NULL, 
    country varchar, 
    gender varchar, 
    category varchar, 
    brand varchar, 
    portal varchar NOT NULL,
    avg_price_usd float,
    max_price_usd float,
    min_price_usd float,
    stddev_price_usd float,
    count_products int
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
truncate_tables = """
TRUNCATE TABLE factFarfetch;
TRUNCATE TABLE factOunass;
TRUNCATE TABLE obtBrandPricing
"""

load_factOunass = """
INSERT INTO factOunass(crawl_date, 
country, ounass_product_id, gender, 
brand, category, price, currency)
select
    DISTINCT 
      crawl_date,  
      country,  
      portal_itemid as ounass_product_id, 
      dimgender.gender,  
      dimbrand.brand, 
      dimcategory.category,  
      cast(replace(price,',','') as float) as price,  
      currency 
from
    ounass_staging
join
    dimgender on dimgender.ounass_gender = ounass_staging.gender
join
    dimcategory on dimcategory.ounass_category = ounass_staging.category
join
    dimbrand on dimbrand.ounass_brand = ounass_staging.brand
where
    ounass_staging.gender is not null 
    and ounass_staging.category is not null 
    and ounass_staging.brand is not null
    and ounass_staging.gender in ('Women', 'Men')
    and ounass_staging.category in ('clothing', 'shoes', 'bags')
    ;
"""

load_factFarfetch = """
INSERT INTO factFarfetch(crawl_date, country, farfetch_product_id, 
                        gender, brand, category, price, currency)
select
    DISTINCT 
      CAST(DATEADD(second, crawl_date/1000,'1970/1/1') AS DATE) as crawl_date,  
      country,  
      portal_itemid as farfetch_product_id, 
      dimgender.gender,  
      dimbrand.brand, 
      dimcategory.category,  
      case when left(currency, 1) = '$' THEN ROUND(cast(replace(replace(price,',',''), '$','') as float)/3.22, 2) else  cast(replace(replace(price,',',''), '$','') as float) END as price,
      case when left(currency, 1) = '$' then 'KWD' ELSE currency END as currency
from
    farfetch_staging
left join
    dimgender on dimgender.farfetch_gender = farfetch_staging.gender
left join
    dimcategory on dimcategory.farfetch_category = farfetch_staging.category
left join
    dimbrand on dimbrand.farfetch_brand = farfetch_staging.brand
where
    farfetch_staging.gender is not null 
    and farfetch_staging.category is not null 
    and farfetch_staging.brand is not null
    and farfetch_staging.price is not null
    and farfetch_staging.portal_itemid is not null
    and farfetch_staging.gender in ('women', 'men')
    and farfetch_staging.category in ('Clothing', 'Shoes', 'Bags')
    and farfetch_staging.portal_itemid != 'item'
    ;
"""
load_obt = """
INSERT INTO obtbrandpricing(crawl_date, country, gender, category, brand, portal,
avg_price_usd, max_price_usd, min_price_usd, stddev_price_usd, count_products)
WITH L1 AS
(
select 
	'ounass' as portal,
    crawl_date,
    country,
    brand,
    category,
    gender,
  	case 
  		when country = 'sa' then round(price / 3.76, 2)
  		when country = 'ae' then round(price / 3.67, 2)
  		when country = 'kw' then round(price / 0.31, 2)
  		when country = 'qa' then round(price / 3.64, 2)
  	ELSE NULL
  	end as price_usd
from factOunass
union all
select 
	'farfetch' as portal,
    crawl_date,
    country,
    brand,
    category,
    gender,
    case 
  		when country = 'sa' then round(price / 3.76, 2)
  		when country = 'ae' then round(price / 3.67, 2)
  		when country = 'kw' then round(price / 0.31, 2)
  		when country = 'qa' then round(price / 3.64, 2)
  	ELSE NULL
  	end as price_usd
from factFarfetch
)
SELECT 
	crawl_date, country, gender, category, brand, portal,
  avg(price_usd) as avg_price_usd,
  max(price_usd) as max_price_usd,
  min(price_usd) as min_price_usd,
  round(stddev(price_usd), 2) as stddev_price_usd,
  count(price_usd) as count_products
FROM L1 
group by 1,2,3,4,5,6
ORDER BY crawl_date, country, gender, category, brand, portal
"""
