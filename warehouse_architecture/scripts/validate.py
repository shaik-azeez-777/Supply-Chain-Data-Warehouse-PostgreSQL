                                                                   airflow/scripts/validate.py
import psycopg2

database_config = {
    'host': 'localhost',
    'user': 'postgres',
    'database': 'production_warehouse',
    'password': 'your_password',
    'port': '5432'
}

def validate_data():
    conn = psycopg2.connect(**database_config)
    cur = conn.cursor()

    try:
        cur.execute("SELECT COUNT(*) FROM bronze.stg_supply_chain;")
        count = cur.fetchone()[0]

        if count == 0:
            raise Exception("No data loaded!")

        print(f"Validation passed. Rows in bronze: {count}")

    finally:
        cur.close()
        conn.close()
