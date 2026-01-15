import os
import sys
import datetime

# ล้างค่าเก่าที่อาจค้างอยู่ในเครื่องออกให้หมด
if "SPARK_HOME" in os.environ: del os.environ["SPARK_HOME"]
if "PYSPARK_PYTHON" in os.environ: del os.environ["PYSPARK_PYTHON"]

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def log_status(message):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] 🚀 {message}")

def clean_airbnb_data(df):
    """ฟังก์ชันจัดการข้อมูลให้สะอาด (หนูเอา Logic ที่แม่เคยสอนมาใส่ตรงนี้ได้เลย)"""
    # ตัวอย่าง: เติมค่าว่างเบื้องต้น
    df_cleaned = df.fillna({'name': 'Unknown', 'price': 0})
    return df_cleaned

def start_spark():
    try:
        # ใช้ Relative Path สำหรับไฟล์ JAR (ถ้าอยู่ในโฟลเดอร์ jars ในโปรเจกต์)
        jar_path = os.path.abspath("./jars/postgresql-42.7.8.jar")
        
        spark = SparkSession.builder \
            .master("local") \
            .appName("Airbnb_Pro_Pipeline") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.jars", jar_path) \
            .getOrCreate()
        return spark
    except Exception as e:
        log_status(f"💥 สร้าง Spark ไม่สำเร็จเพราะ: {e}")
        return None

# --- เริ่มรันงาน ---
spark = start_spark()

if spark:
    log_status("หัวหน้าคนงาน Spark พร้อมลุยงานแบบโปรแล้ว!")
    files = ['listings', 'neighbourhoods', 'reviews']
    
    # ข้อมูลการเชื่อมต่อฐานข้อมูล
    db_url = "jdbc:postgresql://localhost:5432/airbnb_raw"
    db_properties = {
        "user": "admin",
        "password": "password123", 
        "driver": "org.postgresql.Driver"
    }

    for file_name in files:
        # ใช้ Relative Path (./) จะทำให้รันได้ทุกเครื่อง
        path = f"./dataset/raw/{file_name}.csv"
        log_status(f"กำลังอ่านไฟล์: {file_name}")
        
        try:
            df = spark.read.csv(path, header=True, inferSchema=True)
            
            # ถ้าเป็นไฟล์ listings ให้ผ่านการคลีนก่อน
            if file_name == 'listings':
                log_status("✨ กำลังทำความสะอาดข้อมูล Listings...")
                df = clean_airbnb_data(df)
            
            log_status(f"📥 กำลังส่ง {file_name} เข้าฐานข้อมูล Postgres...")
            df.write.jdbc(url=db_url, table=file_name, mode="overwrite", properties=db_properties)
            log_status(f"✅ {file_name} โหลดเสร็จเรียบร้อย!")
            
        except Exception as e:
            log_status(f"❌ เกิดข้อผิดพลาดกับไฟล์ {file_name}: {e}")

else:
    log_status("Spark ไม่ทำงาน ตรวจสอบ JAVA_HOME อีกครั้งนะลูก")