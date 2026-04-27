                                                                airflow/scripts/transform_silver.py
import psycopg2

database_config = {
    'host': 'localhost',
    'user': 'postgres',
    'database': 'production_warehouse',
    'password': 'your_password',
    'port': '5432'
}

def run_silver():
    conn = psycopg2.connect(**database_config)
    cur = conn.cursor()

    try:
        print("Running silver transformation...")
        cur.execute("CALL silver.load_supply_chain_data();")

        conn.commit()
        print("Silver layer updated")

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cur.close()
        conn.close()
