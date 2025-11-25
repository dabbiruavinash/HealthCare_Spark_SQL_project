with patient_risk_factors as (
select p.patient_id, p.first_name, p.last_name, p.date_of_birth, count(distinct mr.diagnosis) as chronic_conditions,
count(distinct pr.prescriptiion_id) as medications,
max(lt.result_value) as max_blood_pressure,
avg(case when lt.test_name = 'HbA1c' then to_number(lt.result_value) end) as avg_hba1c, 
count(distinct case when mr.follow_up_requried = 'Y' then mr.record_id end) as emergency_visits from patients p
LEFT join medical_records mr on p.patient_id = mr.patient_id
LEFT JOIN prescriptions pr ON mr.record_id = pr.record_id
LEFT JOIN lab_tests lt ON mr.record_id = lt.record_id
WHERE mr.visit_date >= ADD_MONTHS(TRUNC(SYSDATE), -12)
GROUP BY p.patient_id, p.first_name, p.last_name, p.date_of_birth),

risk_scores AS (
SELECT *,
           (chronic_conditions * 2 + 
            medications * 1 +
            CASE WHEN max_blood_pressure > 140 THEN 3 ELSE 0 END +
            CASE WHEN avg_hba1c > 6.5 THEN 2 ELSE 0 END +
            emergency_visits * 2) as risk_score FROM patient_risk_factors)

SELECT 
    patient_id,
    first_name,
    last_name,
    chronic_conditions,
    medications,
    risk_score, CASE 
        WHEN risk_score >= 10 THEN 'High Risk'
        WHEN risk_score >= 5 THEN 'Medium Risk' ELSE 'Low Risk' END as risk_category FROM risk_scores ORDER BY risk_score DESC;