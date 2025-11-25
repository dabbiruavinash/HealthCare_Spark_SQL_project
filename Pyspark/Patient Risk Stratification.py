def patient_risk_stratification(patients_df, medical_records_df, prescriptions_df, lab_tests_df):
    """
    Stratify patients into risk categories based on multiple health factors
    """
    one_year_ago = datetime.now() - timedelta(days=365)
    
    patient_risk_factors = (patients_df
        .join(medical_records_df, "patient_id", "left")
        .join(prescriptions_df, "record_id", "left")
        .join(lab_tests_df, "record_id", "left")
        .filter(col("visit_date") >= one_year_ago)
        .groupBy("patient_id", "first_name", "last_name", "date_of_birth")
        .agg(
            countDistinct("diagnosis").alias("chronic_conditions"),
            countDistinct("prescription_id").alias("medications"),
            max("result_value").alias("max_blood_pressure"),
            avg(when(col("test_name") == "HbA1c", col("result_value").cast("double"))).alias("avg_hba1c"),
            countDistinct(when(col("follow_up_required") == "Y", "record_id")).alias("emergency_visits")
        )
    )
    
    result = (patient_risk_factors
        .withColumn("risk_score",
            col("chronic_conditions") * 2 +
            col("medications") * 1 +
            when(col("max_blood_pressure") > 140, 3).otherwise(0) +
            when(col("avg_hba1c") > 6.5, 2).otherwise(0) +
            col("emergency_visits") * 2
        )
        .withColumn("risk_category",
            when(col("risk_score") >= 10, "High Risk")
            .when(col("risk_score") >= 5, "Medium Risk")
            .otherwise("Low Risk")
        )
        .select("patient_id", "first_name", "last_name", "chronic_conditions", 
                "medications", "risk_score", "risk_category")
        .orderBy(desc("risk_score"))
    )
    
    return result