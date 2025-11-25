def lab_test_trend_analysis(patients_df, medical_records_df, lab_tests_df):
    """
    Analyze trends in laboratory test results for diabetic patients
    """
    two_years_ago = datetime.now() - timedelta(days=730)
    
    # First, identify diabetic patients
    diabetic_patients = (medical_records_df
        .filter(lower(col("diagnosis")).like("%diabetes%"))
        .select("patient_id")
        .distinct()
    )
    
    test_trends = (patients_df
        .join(diabetic_patients, "patient_id")
        .join(medical_records_df, "patient_id")
        .join(lab_tests_df, "record_id")
        .filter(col("test_name").isin(["HbA1c", "Blood Glucose", "Cholesterol"]))
        .filter(col("test_date") >= two_years_ago)
        .filter(regexp_extract(col("result_value"), r"^(\d+(\.\d+)?)", 0) != "")
        .withColumn("numeric_result", regexp_extract(col("result_value"), r"^(\d+(\.\d+)?)", 1).cast("double"))
        .groupBy(
            "patient_id", "first_name", "last_name", "test_name",
            year("test_date").alias("test_year"),
            month("test_date").alias("test_month")
        )
        .agg(
            avg("numeric_result").alias("avg_result"),
            count("*").alias("test_count")
        )
    )
    
    window_spec = Window.partitionBy("patient_id", "test_name").orderBy("test_year", "test_month")
    
    trend_analysis = (test_trends
        .withColumn("prev_avg", lag("avg_result").over(window_spec))
        .withColumn("change_percentage",
            when(col("prev_avg").isNotNull() & (col("prev_avg") != 0),
                 ((col("avg_result") - col("prev_avg")) / col("prev_avg")) * 100
            ).otherwise(None)
        )
        .filter(col("prev_avg").isNotNull())
        .orderBy("patient_id", "test_name", desc("test_year"), desc("test_month"))
    )
    
    return trend_analysis