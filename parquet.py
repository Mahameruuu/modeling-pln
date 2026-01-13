import pandas as pd
import yaml
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from datetime import datetime, timedelta
import os

# Load DB config
with open("config/db_config_tja2.yaml", "r") as f:
    db_cfg = yaml.safe_load(f)["mysql"]

pwd = quote_plus(db_cfg['password'])
conn_str = f"mysql+pymysql://{db_cfg['user']}:{pwd}@{db_cfg['host']}:{db_cfg['port']}/{db_cfg['database']}"
engine = create_engine(conn_str)

# Folder output Parquet per-hari
parquet_dir = "data_parquet_per_day/tja2/"
os.makedirs(parquet_dir, exist_ok=True)

# =====================
# LOOP TANGGAL
# =====================
start_date = datetime(2026, 1, 11)
end_date   = datetime(2026, 1, 12)

current_date = start_date

while current_date <= end_date:
    next_date = current_date + timedelta(days=1)

    query = f"""
        SELECT f_address_no, f_date_rec, f_value
        FROM tb_bat_history
        WHERE f_date_rec >= '{current_date:%Y-%m-%d}' 
          AND f_date_rec <  '{next_date:%Y-%m-%d}';
    """

    print(f"Fetching {current_date:%Y-%m-%d} ...")
    df_day = pd.read_sql(query, engine)

    parquet_file = os.path.join(
        parquet_dir,
        f"data_{current_date:%Y%m%d}.parquet"
    )

    df_day.to_parquet(parquet_file, index=False)

    print(f"Data tanggal {current_date:%Y-%m-%d} berhasil disimpan → {parquet_file}")

    current_date += timedelta(days=1)
