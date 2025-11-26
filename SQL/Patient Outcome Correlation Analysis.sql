WITH treatment_outcomes AS (
    SELECT 
        mr.diagnosis,
        mr.treatment_plan,
        COUNT(DISTINCT mr.patient_id) as patient_count,
        AVG(CASE WHEN mr.follow_up_required = 'N' THEN 1 ELSE 0 END) as success_rate,
        AVG(b.total_amount) as avg_treatment_cost,
        AVG(MONTHS_BETWEEN(mr.follow_up_date, mr.visit_date)) as avg_follow_up_days,
        COUNT(DISTINCT pr.prescription_id) as avg_medications
    FROM medical_records mr
    JOIN billing b ON mr.patient_id = b.patient_id
    LEFT JOIN prescriptions pr ON mr.record_id = pr.record_id
    WHERE mr.visit_date >= ADD_MONTHS(TRUNC(SYSDATE), -24)
      AND mr.follow_up_date IS NOT NULL
    GROUP BY mr.diagnosis, mr.treatment_plan
),
outcome_analysis AS (
    SELECT 
        diagnosis,
        treatment_plan,
        patient_count,
        success_rate,
        avg_treatment_cost,
        avg_follow_up_days,
        avg_medications,
        (success_rate * 0.4) + ((1 / avg_treatment_cost) * 1000 * 0.3) + ((1 / avg_follow_up_days) * 0.3) as outcome_score
    FROM treatment_outcomes
    WHERE patient_count >= 10
)
SELECT 
    diagnosis,
    treatment_plan,
    patient_count,
    ROUND(success_rate * 100, 2) as success_percentage,
    avg_treatment_cost,
    avg_follow_up_days,
    ROUND(outcome_score, 4) as effectiveness_score,
    RANK() OVER (PARTITION BY diagnosis ORDER BY outcome_score DESC) as effectiveness_rank
FROM outcome_analysis
ORDER BY diagnosis, effectiveness_rank;