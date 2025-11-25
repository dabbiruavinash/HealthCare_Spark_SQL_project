def insurance_coverage_gap_analysis(patients_df, insurance_df, billing_df):
    """
    Identify patients with potential insurance coverage gaps or high out-of-pocket costs
    """
    six_months_ago = datetime.now() - timedelta(days=180)
    
    result = (patients_df
        .join(insurance_df, "patient_id")
        .join(billing_df, "patient_id", "left")
        .filter(col("bill_date") >= six_months_ago)
        .groupBy(
            "p.patient_id", "first_name", "last_name", 
            "provider_name", "coverage_type", "deductible", 
            "copay", "expiration_date"
        )
        .agg(sum("patient_balance").alias("total_out_of_pocket"))
        .filter(col("total_out_of_pocket") > 0)
        .withColumn("coverage_status",
            when(col("expiration_date") < current_date(), "Coverage Expired")
            .when(col("expiration_date") < current_date() + 30, "Expiring Soon")
            .when(col("total_out_of_pocket") > col("deductible") * 0.8, "High Out-of-Pocket")
            .otherwise("Adequate Coverage")
        )
        .orderBy(desc("total_out_of_pocket"))
    )
    
    return result