import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
from utils import get_s3_client, ensure_bucket_exists, upload_bytes, DEFAULT_BUCKET

def generate_sample_parquet():
    data = [
        {
            "order_id": "ORD-1001",
            "customer_id": "CUST-501",
            "order_date": "2026-11-01",
            "country": "USA",
            "product_id": "PROD-A",
            "quantity": 2,
            "unit_price": 50.0,
            "discount": 0.0,
            "shipping_cost": 5.0,
            "revenue": 100.0,
            "tax": 8.0
        },
        {
            "order_id": "ORD-1002",
            "customer_id": "CUST-502",
            "order_date": "2026-11-02",
            "country": "Canada",
            "product_id": "PROD-B",
            "quantity": 1,
            "unit_price": 200.0,
            "discount": 10.0,
            "shipping_cost": 15.0,
            "revenue": 190.0,
            "tax": 15.2
        },
        {
            "order_id": "ORD-1003",
            "customer_id": "CUST-503",
            "order_date": "2026-11-03",
            "country": "USA",
            "product_id": "PROD-C",
            "quantity": 5,
            "unit_price": 20.0,
            "discount": 5.0,
            "shipping_cost": 10.0,
            "revenue": 95.0,
            "tax": 7.6
        }
    ]
    
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()

if __name__ == "__main__":
    client = get_s3_client()
    ensure_bucket_exists(client, DEFAULT_BUCKET)
    
    parquet_bytes = generate_sample_parquet()
    s3_key = "tables/module06/data/orders_part_01.parquet"
    
    upload_bytes(client, DEFAULT_BUCKET, s3_key, parquet_bytes)
    print("MinIO warehouse bucket prepopulated successfully with 11-column order schema.")