                                                                  airflow/scripts/load_bronze.py
import psycopg2

db_config = {
    'host': 'localhost',
    'port': '5432',
    'user': 'postgres',
    'password': 'your_password',
    'database': 'production_warehouse'
}

def load_to_bronze(file_path):
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    try:
        print("Truncating bronze table...")
        cur.execute("TRUNCATE TABLE bronze.stg_supply_chain;")

        with open(file_path, 'r') as f:
            cols = """(
                product_type, sku, price, availability,
                number_of_products_sold, revenue_generated,
                customer_demographics, stock_levels, lead_times,
                order_quantities, shipping_times, shipping_carriers,
                shipping_costs, supplier_name, location,
                lead_time_alt, production_volumes, mfg_lead_time,
                mfg_costs, inspection_results, defect_rates,
                transportation_modes, routes, costs
            )"""

            cur.copy_expert(
                f"COPY bronze.stg_supply_chain {cols} FROM STDIN WITH CSV HEADER",
                f
            )

        conn.commit()
        print("Loaded into bronze successfully")

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cur.close()
        conn.close()
