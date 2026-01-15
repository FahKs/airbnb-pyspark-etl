import os
import sys

# 1. ตั้งค่า Path ให้ชัดเจน
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]

if "SPARK_HOME" in os.environ: del os.environ["SPARK_HOME"]

from pyspark.sql import SparkSession

def start_spark():
    try:
        spark = SparkSession.builder \
            .master("local") \
            .appName("Airbnb_Final_Test") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.jars", r"C:\Data Engineer_project\jars\postgresql-42.7.8.jar") \
            .getOrCreate()
        return spark
    except Exception as e:
        print(f"💥 สร้าง Spark ไม่สำเร็จเพราะ: {e}")
        return None

# --- ส่วนของการรันงาน ---
spark = start_spark()

if spark:
    print("✨ หัวหน้าคนงาน Spark ตื่นแล้วจ้า!")
    files = ['listings', 'neighbourhoods', 'reviews']
    
    for file_name in files:
        path = f"C:/Data Engineer_project/dataset/raw/{file_name}.csv"
        print(f"🔄 กำลังอ่าน: {file_name}")
        
        df = spark.read.csv(path, header=True, inferSchema=True)
        df.show(3, vertical=True)
        
        # --- 📥 ส่วนโหลดลง Postgres (ต้องเคาะเข้ามาให้ตรงกับ df.show) ---
        print(f"📥 กำลังโหลด {file_name} ลง Postgres...")
        
        db_url = "jdbc:postgresql://localhost:5432/airbnb_raw"
        db_properties = {
            "user": "admin",
            "password": "password123", 
            "driver": "org.postgresql.Driver"
        }

        # สั่งเขียนลง DB
        df.write.jdbc(url=db_url, table=file_name, mode="overwrite", properties=db_properties)
        print(f"✅ {file_name} เข้าบ้านเรียบร้อย!")
        # ---------------------------------------------------------

else:
    print("❌ ไปต่อไม่ได้ เพราะ Spark ไม่ตื่นลูก")