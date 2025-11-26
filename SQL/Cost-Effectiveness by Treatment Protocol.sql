WITH treatment_costs AS (
    SELECT 
        mr.diagnosis,
        SUBSTR(mr.treatment_plan, 1, 100) as treatment_type,
        COUNT(DISTINCT mr.patient_id) as patient_count,
        AVG(b.total_amount) as avg_cost_per_patient,
        AVG(CASE WHEN mr.follow_up_required = 'Y' THEN 1 ELSE 0 END) as readmission_rate,
        AVG(a.duration_minutes) as avg_treatment_duration
    FROM medical_records mr
    JOIN appointments a ON mr.patient_id = a.patient_id AND mr.doctor_id = a.doctor_id
    JOIN billing b ON a.appointment_id = b.appointment_id
    WHERE mr.visit_date >= ADD_MONTHS(TRUNC(SYSDATE), -24)
      AND b.total_amount > 0
    GROUP BY mr.diagnosis, SUBSTR(mr.treatment_plan, 1, 100)
),
ranked_treatments AS (
    SELECT *,
           RANK() OVER (PARTITION BY diagnosis ORDER BY avg_cost_per_patient ASC) as cost_rank,
           RANK() OVER (PARTITION BY diagnosis ORDER BY readmission_rate ASC) as effectiveness_rank
    FROM treatment_costs
    WHERE patient_count >= 5
)
SELECT 
    diagnosis,
    treatment_type,
    patient_count,
    avg_cost_per_patient,
    readmission_rate,
    (cost_rank + effectiveness_rank) as combined_score
FROM ranked_treatments
WHERE combined_score <= 5  -- Top 5 most cost-effective treatments per diagnosis
ORDER BY diagnosis, combined_score;