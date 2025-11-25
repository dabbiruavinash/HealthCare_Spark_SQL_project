from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import pandas as pd

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("HealthcareAnalytics") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Define Schemas
patients_schema = StructType([
    StructField("patient_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("gender", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("emergency_contact", StringType(), True),
    StructField("blood_type", StringType(), True),
    StructField("created_date", DateType(), True)
])

doctors_schema = StructType([
    StructField("doctor_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("specialization", StringType(), True),
    StructField("license_number", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("email", StringType(), True),
    StructField("hire_date", DateType(), True),
    StructField("salary", DoubleType(), True),
    StructField("department", StringType(), True)
])

appointments_schema = StructType([
    StructField("appointment_id", IntegerType(), True),
    StructField("patient_id", IntegerType(), True),
    StructField("doctor_id", IntegerType(), True),
    StructField("appointment_date", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("reason", StringType(), True),
    StructField("duration_minutes", IntegerType(), True),
    StructField("room_number", StringType(), True)
])

medical_records_schema = StructType([
    StructField("record_id", IntegerType(), True),
    StructField("patient_id", IntegerType(), True),
    StructField("doctor_id", IntegerType(), True),
    StructField("visit_date", DateType(), True),
    StructField("diagnosis", StringType(), True),
    StructField("symptoms", StringType(), True),
    StructField("treatment_plan", StringType(), True),
    StructField("follow_up_required", StringType(), True),
    StructField("follow_up_date", DateType(), True)
])

prescriptions_schema = StructType([
    StructField("prescription_id", IntegerType(), True),
    StructField("record_id", IntegerType(), True),
    StructField("medication_name", StringType(), True),
    StructField("dosage", StringType(), True),
    StructField("frequency", StringType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("refills_remaining", IntegerType(), True)
])

lab_tests_schema = StructType([
    StructField("test_id", IntegerType(), True),
    StructField("record_id", IntegerType(), True),
    StructField("test_name", StringType(), True),
    StructField("test_date", DateType(), True),
    StructField("result_value", StringType(), True),
    StructField("normal_range", StringType(), True),
    StructField("units", StringType(), True),
    StructField("lab_technician", StringType(), True),
    StructField("status", StringType(), True)
])

billing_schema = StructType([
    StructField("bill_id", IntegerType(), True),
    StructField("patient_id", IntegerType(), True),
    StructField("appointment_id", IntegerType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("insurance_covered", DoubleType(), True),
    StructField("patient_balance", DoubleType(), True),
    StructField("bill_date", DateType(), True),
    StructField("due_date", DateType(), True),
    StructField("status", StringType(), True)
])

insurance_schema = StructType([
    StructField("insurance_id", IntegerType(), True),
    StructField("patient_id", IntegerType(), True),
    StructField("provider_name", StringType(), True),
    StructField("policy_number", StringType(), True),
    StructField("coverage_type", StringType(), True),
    StructField("deductible", DoubleType(), True),
    StructField("copay", DoubleType(), True),
    StructField("effective_date", DateType(), True),
    StructField("expiration_date", DateType(), True)
])
