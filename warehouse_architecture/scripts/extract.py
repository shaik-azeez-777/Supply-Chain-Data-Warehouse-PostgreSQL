import os
from glob import glob

landing_zone = '/mnt/c/Users/shaik/Downloads/OneDrive/Desktop/datawarehouse_automation_pipeline/landing_Zone'

def get_latest_file():
    files = glob(os.path.join(landing_zone, '*.csv'))

    print("DEBUG FILES FOUND:", files) 

    if not files:
        raise Exception("No CSV files found")

    latest_file = max(files, key=os.path.getctime)

    print(f"Latest file: {latest_file}")

    return latest_file
