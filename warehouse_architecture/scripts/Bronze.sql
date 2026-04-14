-- drop table if exists bronze.stg_supply_chain


CREATE TABLE bronze.stg_supply_chain (
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
    costs TEXT,            
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


COPY bronze.stg_supply_chain(
    product_type, sku, price, availability, number_of_products_sold, 
    revenue_generated, customer_demographics, stock_levels, lead_times, 
    order_quantities, shipping_times, shipping_carriers, shipping_costs, 
    supplier_name, location, lead_time_alt, production_volumes, 
    mfg_lead_time, mfg_costs, inspection_results, defect_rates, 
    transportation_modes, routes, costs
)
FROM 'C:\Program Files\PostgreSQL\18\pgAdmin 4\runtime\supply_chain_data.csv' 
WITH (FORMAT CSV, HEADER, DELIMITER ',');

select * from bronze.stg_supply_chain as bronze




