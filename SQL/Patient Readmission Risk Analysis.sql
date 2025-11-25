WITH readmission_candidates AS (
    SELECT 
        mr.patient_id,
        mr.record_id,
        mr.visit_date,
        mr.diagnosis,
        mr.follow_up_required,
        mr.follow_up_date,
        LEAD(mr.visit_date) OVER (PARTITION BY mr.patient_id ORDER BY mr.visit_date) as next_visit_date,
        COUNT(pr.prescription_id) as medication_count,
        COUNT(lt.test_id) as test_count
    FROM medical_records mr
    LEFT JOIN prescriptions pr ON mr.record_id = pr.record_id
    LEFT JOIN lab_tests lt ON mr.record_id = lt.record_id
    WHERE mr.visit_date >= ADD_MONTHS(TRUNC(SYSDATE), -12)
),
readmission_analysis AS (
    SELECT 
        rc.patient_id,
        p.first_name,
        p.last_name,
        rc.diagnosis,
        rc.visit_date,
        rc.next_visit_date,
        rc.medication_count,
        rc.test_count,
        CASE 
            WHEN rc.next_visit_date IS NOT NULL AND 
                 rc.next_visit_date <= rc.visit_date + 30 THEN 1 
            ELSE 0 
        END as was_readmitted,
        CASE 
            WHEN rc.medication_count > 5 THEN 3
            WHEN rc.medication_count > 2 THEN 2
            ELSE 1
        END +
        CASE 
            WHEN rc.diagnosis IN ('Heart Failure', 'COPD', 'Pneumonia') THEN 3
            WHEN rc.diagnosis IN ('Diabetes', 'Hypertension') THEN 2
            ELSE 1
        END as risk_score
    FROM readmission_candidates rc
    JOIN patients p ON rc.patient_id = p.patient_id
    WHERE rc.follow_up_required = 'Y'
)
SELECT 
    patient_id,
    first_name,
    last_name,
    diagnosis,
    risk_score,
    CASE 
        WHEN risk_score >= 5 THEN 'High Risk'
        WHEN risk_score >= 3 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as readmission_risk
FROM readmission_analysis
WHERE was_readmitted = 0  -- Only for patients who haven't been readmitted yet
ORDER BY risk_score DESC;