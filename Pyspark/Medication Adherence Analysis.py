def medication_adherence_analysis(patients_df, medical_records_df, prescriptions_df):
    """
    Find patients who haven't refilled essential medications on time
    """
    essential_meds = ['Insulin', 'Warfarin', 'Levothyroxine', 'Metformin']
    
    result = (patients_df
        .join(medical_records_df, "patient_id")
        .join(prescriptions_df, "record_id")
        .filter(col("medication_name").isin(essential_meds))
        .filter(col("refills_remaining") > 0)
        .filter(col("end_date") < current_date() - 7)
        .withColumn("adherence_status",
            when(col("end_date") < current_date() - 30, "Critical Non-Adherence")
            .when(col("end_date") < current_date() - 15, "Moderate Non-Adherence")
            .otherwise("Within Guidelines")
        )
        .select(
            "patient_id", "first_name", "last_name", "medication_name",
            "end_date", "adherence_status", "refills_remaining"
        )
        .orderBy("adherence_status", "end_date")
    )
    
    return result