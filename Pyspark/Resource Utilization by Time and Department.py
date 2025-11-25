def resource_utilization_analysis(appointments_df, doctors_df):
    """
    Analyze peak utilization times and optimize resource allocation
    """
    six_months_ago = datetime.now() - timedelta(days=180)
    
    utilization = (appointments_df
        .join(doctors_df, "doctor_id")
        .filter(col("appointment_date") >= six_months_ago)
        .groupBy(
            "department",
            hour("appointment_date").alias("hour_of_day"),
            date_format("appointment_date", "E").alias("day_of_week")
        )
        .agg(
            count("*").alias("appointment_count"),
            avg("duration_minutes").alias("avg_duration"),
            count(when(col("status") == "Completed", 1)).alias("completed_count"),
            (count(when(col("status") == "Completed", 1)) * 100.0 / count("*")).alias("completion_rate")
        )
    )
    
    # Find peak hours for each department
    peak_hours = (utilization
        .groupBy("department", "day_of_week")
        .agg(max("appointment_count").alias("peak_count"))
        .join(utilization, ["department", "day_of_week", "appointment_count"])
        .select("department", "day_of_week", "hour_of_day", "peak_count")
    )
    
    return utilization.orderBy("department", "day_of_week", "hour_of_day")