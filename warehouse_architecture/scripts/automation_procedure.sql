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
