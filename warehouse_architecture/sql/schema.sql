-- Create schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Bronze (raw layer)
CREATE TABLE IF NOT EXISTS bronze.stg_supply_chain (
    product_type TEXT,
    sku TEXT,
    price TEXT,
    availability TEXT,
    number_of_products_sold TEXT,
    revenue_generated TEXT,
    customer_demographics TEXT,
    stock_levels TEXT,
    lead_times TEXT,
    order_quantities TEXT,
    shipping_times TEXT,
    shipping_carriers TEXT,
    shipping_costs TEXT,
    supplier_name TEXT,
    location TEXT,
    lead_time_alt TEXT,
    production_volumes TEXT,
    mfg_lead_time TEXT,
    mfg_costs TEXT,
    inspection_results TEXT,
    defect_rates TEXT,
    transportation_modes TEXT,
    routes TEXT,
    costs TEXT
);

-- Silver (clean layer)

-- Dimension: products
CREATE TABLE IF NOT EXISTS silver.dim_products (
    product_key SERIAL PRIMARY KEY,
    sku VARCHAR(50) UNIQUE NOT NULL,
    product_type VARCHAR(100)
);

-- Dimension: locations
CREATE TABLE IF NOT EXISTS silver.dim_locations (
    location_key SERIAL PRIMARY KEY,
    location_name VARCHAR(100),
    supplier_name VARCHAR(100),
    CONSTRAINT unique_location_supplier UNIQUE (location_name, supplier_name)
);

-- Fact table
CREATE TABLE IF NOT EXISTS silver.fct_supply_chain (
    fact_key SERIAL PRIMARY KEY,
    product_key INT REFERENCES silver.dim_products(product_key),
    location_key INT REFERENCES silver.dim_locations(location_key),
    price DECIMAL(15,2),
    revenue_generated DECIMAL(15,2),
    stock_levels INT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Rejected records
CREATE TABLE IF NOT EXISTS silver.rejected_records (
    id SERIAL PRIMARY KEY,
    data_content JSONB,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
