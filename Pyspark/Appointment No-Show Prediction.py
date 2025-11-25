def appointment_no_show_prediction(patients_df, appointments_df):
    """
    Identify patterns in appointment no-shows and predict future no-shows
    """
    one_year_ago = datetime.now() - timedelta(days=365)
    
    appointment_patterns = (patients_df
        .join(appointments_df, "patient_id")
        .filter(col("appointment_date") >= one_year_ago)
        .groupBy("patient_id", "first_name", "last_name")
        .agg(
            count("*").alias("total_appointments"),
            count(when(col("status") == "No-Show", 1)).alias("no_show_count"),
            (count(when(col("status") == "No-Show", 1)) * 100.0 / count("*")).alias("no_show_rate"),
            avg(when(col("status") != "No-Show", hour("appointment_date"))).alias("avg_appointment_hour"),
            countDistinct(date_format("appointment_date", "E")).alias("days_of_week_visited")
        )
    )
    
    result = (appointment_patterns
        .withColumn("prediction",
            when((col("no_show_rate") > 30) | (col("total_appointments") < 3), "High No-Show Risk")
            .when((col("no_show_rate") >= 15) & (col("no_show_rate") <= 30), "Medium No-Show Risk")
            .otherwise("Low No-Show Risk")
        )
        .filter(col("prediction") != "Low No-Show Risk")
        .orderBy(desc("no_show_rate"))
    )
    
    return result