-- creating table for silver.dim_locations
create table silver.dim_locations (
location_key serial primary key,
location_name varchar(20),
supplier_name varchar(20)
);
---pushing data to silver level into table .dim_locations----
INSERT INTO silver.dim_locations (location_name, supplier_name)
select distinct trim(location),trim(supplier_name)
from bronze.stg_supply_chain
where location is not null
	and supplier_name is not null


--creating table for silver.dim_products
create table silver.dim_products (
product_key serial primary key,
SKU varchar(50) unique not null,
Product_type varchar(100)
);
---pushing data to silver level in to table called .dim_products---
insert into silver.dim_products (sku,product_type)
select distinct
upper(trim(sku)), trim(product_type) 
from bronze.stg_supply_chain
where sku is not null
	and sku <> ' '
	and sku <> 'NULL';


-- creating table for fact.supply_chain
CREATE TABLE silver.fct_supply_chain (
    fact_key SERIAL PRIMARY KEY,
    product_key INT REFERENCES silver.dim_products(product_key),
    location_key INT REFERENCES silver.dim_locations(location_key),
    price DECIMAL(15,2),
    revenue_generated DECIMAL(15,2),
    stock_levels INT,
    shipping_costs DECIMAL(15,2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

---pushing data to silver level table .fact supply_chain
INSERT INTO silver.fct_supply_chain (product_key, location_key, price, revenue_generated, stock_levels, shipping_costs)
SELECT 
    p.product_key, 
    l.location_key,
    CAST(b.price AS DECIMAL(15,2)),
    CAST(b.revenue_generated AS DECIMAL(15,2)),
    CAST(b.stock_levels AS INTEGER),
    CAST(b.shipping_costs AS DECIMAL(15,2))
FROM bronze.stg_supply_chain b
JOIN silver.dim_products p ON UPPER(TRIM(b.sku)) = p.sku
JOIN silver.dim_locations l ON TRIM(b.location) = l.location_name 
    AND TRIM(b.supplier_name) = l.supplier_name;


select * from silver.fct_supply_chain
select * from silver.dim_locations 
select * from silver.dim_products


-- saves logic in database

CREATE OR REPLACE PROCEDURE silver.sp_refresh_warehouse()
LANGUAGE plpgsql
AS $$
BEGIN
    -- STEP A: Wipe the old data so we don't have duplicates
    TRUNCATE TABLE silver.fct_supply_chain, silver.dim_products, silver.dim_locations CASCADE;

    -- STEP B: Re-run the logic you already wrote for Products
    INSERT INTO silver.dim_products (sku, product_type)
    SELECT DISTINCT UPPER(TRIM(sku)), TRIM(product_type) 
    FROM bronze.stg_supply_chain WHERE sku IS NOT NULL;

    -- STEP C: Re-run the logic you already wrote for Locations
    INSERT INTO silver.dim_locations (location_name, supplier_name)
    SELECT DISTINCT TRIM(location), TRIM(supplier_name) 
    FROM bronze.stg_supply_chain WHERE location IS NOT NULL;

    -- STEP D: Re-run the "Final Boss" JOIN logic for the Fact Table
    INSERT INTO silver.fct_supply_chain (product_key, location_key, price, revenue_generated, stock_levels, shipping_costs)
    SELECT p.product_key, l.location_key, b.price::DECIMAL, b.revenue_generated::DECIMAL, b.stock_levels::INT, b.shipping_costs::DECIMAL
    FROM bronze.stg_supply_chain b
    JOIN silver.dim_products p ON UPPER(TRIM(b.sku)) = p.sku
    JOIN silver.dim_locations l ON TRIM(b.location) = l.location_name AND TRIM(b.supplier_name) = l.supplier_name;

    RAISE NOTICE 'Success: Warehouse refreshed from Bronze to Silver!';
END;
$$;

call silver.sp_refresh_warehouse()


-------indexing for speed
-- 1. Indexing the Foreign Keys (The Joins)
-- When we JOIN Fact to Product, the DB uses this "Index" to find matches in milliseconds.
CREATE INDEX idx_fct_product_key ON silver.fct_supply_chain (product_key);
CREATE INDEX idx_fct_location_key ON silver.fct_supply_chain (location_key);

-- 2. Indexing the Filter columns
-- Since the CEO often filters or groups by Supplier, we index the name.
CREATE INDEX idx_dim_locations_name ON silver.dim_locations (supplier_name);


