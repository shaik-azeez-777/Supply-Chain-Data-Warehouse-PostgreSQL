CREATE OR REPLACE PROCEDURE silver.load_supply_chain_data()
LANGUAGE plpgsql AS $$
DECLARE fact_count INT;
BEGIN


    INSERT INTO silver.rejected_records (data_content)
    SELECT to_jsonb(b)
    FROM bronze.stg_supply_chain b
    WHERE b.price !~ '^[0-9]+(\.[0-9]+)?$';

    INSERT INTO silver.dim_locations (location_name, supplier_name)
    SELECT DISTINCT TRIM(location), TRIM(supplier_name)
    FROM bronze.stg_supply_chain
    WHERE location IS NOT NULL AND supplier_name IS NOT NULL
    ON CONFLICT (location_name, supplier_name) DO NOTHING;

    INSERT INTO silver.dim_products (sku, product_type)
    SELECT DISTINCT UPPER(TRIM(sku)), product_type
    FROM bronze.stg_supply_chain
    ON CONFLICT (sku) DO NOTHING;


    TRUNCATE TABLE silver.fct_supply_chain;

    INSERT INTO silver.fct_supply_chain (
        product_key,
        location_key,
        price,
        revenue_generated,
        stock_levels
    )
    SELECT 
        p.product_key,
        l.location_key,
        b.price::DECIMAL,
        b.revenue_generated::DECIMAL,
        b.stock_levels::INT
    FROM bronze.stg_supply_chain b
    JOIN silver.dim_products p
        ON UPPER(TRIM(b.sku)) = p.sku
    JOIN silver.dim_locations l
        ON TRIM(b.location) = l.location_name
        AND TRIM(b.supplier_name) = l.supplier_name
    WHERE b.price ~ '^[0-9]+(\.[0-9]+)?$';


    GET DIAGNOSTICS fact_count = ROW_COUNT;

    RAISE NOTICE 'Loaded % records into fact table', fact_count;

END;
$$;
