def patients_multiple_chronic_conditions(patients_df, medical_records_df):
    """
    Find patients diagnosed with at least 3 different chronic conditions in the last year
    """
    one_year_ago = datetime.now() - timedelta(days=365)
    chronic_conditions = ['Hypertension', 'Diabetes', 'Asthma', 'COPD', 'Heart Disease']
    
    result = (patients_df
        .join(medical_records_df, "patient_id")
        .filter(col("visit_date") >= one_year_ago)
        .filter(col("diagnosis").isin(chronic_conditions))
        .groupBy("patient_id", "first_name", "last_name")
        .agg(countDistinct("diagnosis").alias("condition_count"))
        .filter(col("condition_count") >= 3)
        .orderBy(desc("condition_count"))
    )
    
    return result

# Usage
# chronic_patients = patients_multiple_chronic_conditions(patients_df, medical_records_df)