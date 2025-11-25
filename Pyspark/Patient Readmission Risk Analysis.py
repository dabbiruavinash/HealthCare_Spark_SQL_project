def patient_readmission_risk(patients_df, medical_records_df, prescriptions_df, lab_tests_df):
       
       # Identify patients at high risk of readmission within 30 days

        one_year_ago = datetime.now() - timedelta(days=365)

        readmission_candidates = (medical_records_df
        .filter(col("visit_date") >= one_year_ago)
        .filter(col("follow_up_required") == "Y")
        .join(prescription_df, "record_id", "left")
        .groupBy("patient_id", "record_id", "visit_date", "diagnosis", "follow_up_requried", "follow_up_date")
        .agg(countDistinct("prescription_id").alias("medication_count"),
        countDistinct("test_id").alias("test_count")))

        window_spec = Window.partitionBy("patient_id").orderBy("visit_date")

        readmission_analysis = (readmission_candidates
        .withColumn("next_visit_date", lead("visit_date").over(window_spec))
        .withColumn("was_readmitted", when((col("next_visit_date").isNotNull()) & (col("next_visit_date") <= date_add(col("visit_date"), 30)),1).otherwise(0))
        .withColumn("risk_score",
        when(col("medication_count") > 5, 3)
        .when(col("medication_count") > 2, 2).otherwise(1) + when(col("diagnosis").isin(["Heart failure", "COPD", "Pneumonia"]), 3)
        .when(col("diagnosis").isin(["Diabetes", "Hypertension"]), 2).otherwise(1))
        .join(patients_df.select("patient_id", "first_name", last_name"), "patient_id"))

result = (readmission_analysis.filter(col('was_readmitted") == 0)
              .withColumn("readmission_risk", when(col("risk_score") >= 5, "High Risk")
              .when(col("risk_score") >= 5, "High Risk")
              .when((col("risk_score") >= 3, "Medium Risk").otherwise("low Risk"))
              .select("patient_id", "first_name", "last_name", "diagnosis", "risk_score", "readmission_risk").orderBy(desc("risk_score")))
return result