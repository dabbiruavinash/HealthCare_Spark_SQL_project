def department_revenue_analysis(appointments_df, doctors_df, billing_df):
    """
    Calculate monthly revenue by department with year-over-year growth
    """
    two_years_ago = datetime.now() - timedelta(days=730)
    
    monthly_revenue = (billing_df
        .join(appointments_df, "appointment_id")
        .join(doctors_df, "doctor_id")
        .filter(col("bill_date") >= two_years_ago)
        .groupBy(
            "department",
            year("bill_date").alias("bill_year"),
            month("bill_date").alias("bill_month")
        )
        .agg(sum("total_amount").alias("monthly_revenue"))
    )
    
    window_spec = Window.partitionBy("department").orderBy("bill_year", "bill_month")
    
    result = (monthly_revenue
        .withColumn("prev_month_revenue", lag("monthly_revenue").over(window_spec))
        .withColumn("growth_percentage",
            when(col("prev_month_revenue").isNotNull(),
                 ((col("monthly_revenue") - col("prev_month_revenue")) / col("prev_month_revenue")) * 100
            ).otherwise(None)
        )
        .orderBy("department", desc("bill_year"), desc("bill_month"))
    )
    
    return result