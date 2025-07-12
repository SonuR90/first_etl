from datetime import datetime
from io import StringIO
import csv

from s3_handler import get_csv_files_from_s3, get_product_map, upload_to_s3
from validator import validate_row
from mysql_connector import get_cursor

date_str = datetime.today().strftime("%Y%m%d")

bucket_name = "test-namastekart"
prefix = f"incoming-files/{date_str}/"
success_prefix = f"success-files/{date_str}/"
rejected_prefix = f"rejected-files/{date_str}/"


all_files = get_csv_files_from_s3(bucket_name, prefix)

product_master_key = f"{prefix}product_master.csv"
product_map = get_product_map(bucket_name, product_master_key)

cursor, conn = get_cursor()
insert_query = """
INSERT INTO orders (order_id, order_date, product_id, quantity, sales, city)
VALUES (%s, %s, %s, %s, %s, %s)
"""

for key in all_files:
    if "product_master" in key:
        continue  

    obj = upload_to_s3.get_object(Bucket=bucket_name, Key=key)
    content = obj["Body"].read().decode("utf-8")
    reader = csv.DictReader(StringIO(content))
    rows = list(reader)

    rejected = []
    success_data = []

    for row in rows:
        errors = validate_row(row, product_map)
        if errors:
            row["rejection_reason"] = "; ".join(errors)
            rejected.append(row)
        else:
            success_data.append((
                row["order_id"],
                row["order_date"],
                row["product_id"],
                int(row["quantity"]),
                int(row["sales"]),
                row["city"]
            ))

    
    filename = key.split("/")[-1]
    success_path = f"{success_prefix}{filename}"
    rejected_path = f"{rejected_prefix}{filename}"
    error_path = f"{rejected_prefix}error_{filename}"

    if rejected:        
        upload_to_s3(bucket_name, rejected_path, content.encode("utf-8"))        
        error_buffer = StringIO()
        writer = csv.DictWriter(error_buffer, fieldnames=list(rejected[0].keys()))
        writer.writeheader()
        writer.writerows(rejected)
        upload_to_s3(bucket_name, error_path, error_buffer.getvalue().encode("utf-8"))

        print(f"File rejected: {filename}, errors found.")
    else:
        upload_to_s3(bucket_name, success_path, content.encode("utf-8"))
        if success_data:
            cursor.executemany(insert_query, success_data)
            print(f"Inserted {len(success_data)} rows from {filename} into DB.")

        print(f"File moved to success: {filename}")

conn.commit()
cursor.close()
conn.close()
