import os
import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("th2-mock-check-write-result").getOrCreate()

MOCK_STATUS = os.getenv("MOCK_STATUS", "OK")  # OK | NO_DATA | ERROR_DATA
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "/opt/spark/shared/dq/result.json")

if MOCK_STATUS == "NO_DATA":
    result = {
        "status": "NO_DATA",
        "message": "TH2: Không có dữ liệu trong ngày hôm nay.",
        "tables": []
    }
elif MOCK_STATUS == "ERROR_DATA":
    result = {
        "status": "ERROR_DATA",
        "message": "TH2: Phát hiện dữ liệu lỗi.",
        "tables": ["orders", "customers"]
    }
else:
    result = {
        "status": "OK",
        "message": "TH2: Dữ liệu bình thường.",
        "tables": []
    }

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

print(f"Written result: {result}")
spark.stop()