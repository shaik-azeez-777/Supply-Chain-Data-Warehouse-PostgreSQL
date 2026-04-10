drop table if exists bronze.stg_supply_chain


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


--cleaning the data handling nulls and duplicates---
---filtering nulls---
SELECT price FROM bronze.stg_supply_chain 
WHERE price LIKE '%$%' 
   OR price = '' 
   OR price IS NULL;


--duplicates----
SELECT UPPER(sku), COUNT(*) 
FROM bronze.stg_supply_chain 
GROUP BY UPPER(sku) 
HAVING COUNT(*) > 1;

SELECT sku FROM bronze.stg_supply_chain 
WHERE sku ~ '[\n\t\r]'; ---[] #a list of wanted characters,inside list \n means it represnts newline(like hitting eneter) \t means represnts tab,\r means represnts a carriage return 
-- Sometimes when data is exported from old Excel files or web scrapers,
-- a SKU might look like 'SKU01', but it actually has a hidden "Enter" at the end. 
-- TRIM() doesn't always catch these in every database system. If this query returns 0 rows, 
-- it means your text is "clean" and doesn't have hidden formatting breaks.



---find any price that not a clean number---

select price from bronze.stg_supply_chain
where price ~ '^[0-9.]+$'

SELECT sku FROM bronze.stg_supply_chain 
WHERE sku !~ '^SKU[0-9]{4}$';   -- '!' means "Show me the ones that DON'T match"

SELECT sku FROM bronze.stg_supply_chain WHERE sku ~ '[\s\t\n\r]'; 




