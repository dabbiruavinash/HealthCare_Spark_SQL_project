def doctor_performance_analysis(doctors_df, medical_records_df):
    """
    Identify doctors with highest patient satisfaction and lowest readmission rates
    """
    six_months_ago = datetime.now() - timedelta(days=180)
    
    doctor_stats = (medical_records_df
        .join(doctors_df, "doctor_id")
        .filter(col("visit_date") >= six_months_ago)
        .groupBy("doctor_id", "first_name", "last_name", "specialization")
        .agg(
            countDistinct("record_id").alias("total_visits"),
            avg(when(col("follow_up_required") == "Y", 1).otherwise(0)).alias("readmission_rate"),
            countDistinct(
                when(col("visit_date") >= date_sub(col("follow_up_date"), 30), col("patient_id"))
            ).alias("timely_follow_ups")
        )
        .filter(col("total_visits") >= 10)
    )
    
    window_spec = Window.orderBy(desc("timely_follow_ups"), asc("readmission_rate"))
    
    ranked_doctors = (doctor_stats
        .withColumn("performance_rank", rank().over(window_spec))
        .filter(col("performance_rank") <= 10)
    )
    
    return ranked_doctors