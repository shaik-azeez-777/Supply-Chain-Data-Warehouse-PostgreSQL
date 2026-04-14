-- Create Star Schema Tables
-- drop table if exists silver.dim_locations cascade
-- drop table if exists silver.dim_products cascade
-- drop table if exists silver.fct_supply_Chain cascade
-- drop table if exists silver.rejected_records cascade




CREATE TABLE silver.dim_products (
    product_key SERIAL PRIMARY KEY,
    sku VARCHAR(50) UNIQUE NOT NULL,
    product_type VARCHAR(100)
);

CREATE TABLE silver.dim_locations (
    location_key SERIAL PRIMARY KEY,
    location_name VARCHAR(100),
    supplier_name VARCHAR(100),
    CONSTRAINT unique_location_supplier UNIQUE (location_name, supplier_name)
);

CREATE TABLE silver.fct_supply_chain (
    fact_key SERIAL PRIMARY KEY,
    product_key INT REFERENCES silver.dim_products(product_key),
    location_key INT REFERENCES silver.dim_locations(location_key),
    price DECIMAL(15,2),
    revenue_generated DECIMAL(15,2),
    stock_levels INT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE silver.rejected_records (
    id SERIAL PRIMARY KEY,
    data_content JSONB,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



CREATE OR REPLACE PROCEDURE silver.load_supply_chain_data()
LANGUAGE plpgsql AS $$
DECLARE fact_count INT;
BEGIN
    -- 1. IDENTIFY GARBAGE (Divert bad records)
    INSERT INTO bronze.stg_supply_chain_rejected (raw_record, error_reason)
    SELECT to_jsonb(b), 'Price/SKU Format Error'
    FROM bronze.stg_supply_chain b
    WHERE b.price !~ '^[0-9]+(\.[0-9]+)?$' OR b.sku !~ '^[a-zA-Z0-9_-]+$';

    -- 2. UPSERT DIMENSIONS (Merge Logic)
    INSERT INTO silver.dim_locations (location_name, supplier_name)
    SELECT DISTINCT TRIM(location), TRIM(supplier_name)
    FROM bronze.stg_supply_chain
    WHERE location IS NOT NULL AND supplier_name IS NOT NULL
    ON CONFLICT (location_name, supplier_name) DO NOTHING;

    -- 3. LOAD FACT (Final Join)
    TRUNCATE TABLE silver.fct_supply_chain;
    INSERT INTO silver.fct_supply_chain (product_key, location_key, price, revenue_generated, stock_levels)
    SELECT p.product_key, l.location_key, b.price::DECIMAL, b.revenue_generated::DECIMAL, b.stock_levels::INT
    FROM bronze.stg_supply_chain b
    JOIN silver.dim_products p ON UPPER(TRIM(b.sku)) = p.sku
    JOIN silver.dim_locations l ON TRIM(b.location) = l.location_name AND TRIM(b.supplier_name) = l.supplier_name
    WHERE b.price ~ '^[0-9]+(\.[0-9]+)?$';

    GET DIAGNOSTICS fact_count = ROW_COUNT;
    RAISE NOTICE 'Pipeline complete. Loaded % records.', fact_count;
END; $$;

call silver.load_supply_chain_data()

SELECT count(*) as total_facts FROM silver.fct_supply_chain;    product_key INT REFERENCES silver.dim_products(product_key),
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

