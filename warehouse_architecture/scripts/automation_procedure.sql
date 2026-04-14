
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

