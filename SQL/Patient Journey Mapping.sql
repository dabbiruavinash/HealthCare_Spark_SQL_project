WITH patient_journey AS (
    SELECT 
        p.patient_id,
        p.first_name,
        p.last_name,
        MIN(mr.visit_date) as first_visit_date,
        MAX(mr.visit_date) as last_visit_date,
        COUNT(DISTINCT mr.record_id) as total_visits,
        COUNT(DISTINCT mr.diagnosis) as unique_diagnoses,
        COUNT(DISTINCT pr.prescription_id) as total_prescriptions,
        SUM(b.total_amount) as total_billing_amount,
        ROUND(MONTHS_BETWEEN(MAX(mr.visit_date), MIN(mr.visit_date)), 2) as treatment_months
    FROM patients p
    LEFT JOIN medical_records mr ON p.patient_id = mr.patient_id
    LEFT JOIN prescriptions pr ON mr.record_id = pr.record_id
    LEFT JOIN billing b ON p.patient_id = b.patient_id
    GROUP BY p.patient_id, p.first_name, p.last_name
),
journey_segments AS (
    SELECT 
        pj.*,
        CASE 
            WHEN treatment_months > 24 THEN 'Long-term Patient'
            WHEN treatment_months BETWEEN 12 AND 24 THEN 'Medium-term Patient'
            WHEN treatment_months < 12 AND total_visits > 5 THEN 'Frequent Short-term'
            ELSE 'New/Occasional Patient'
        END as patient_segment,
        CASE 
            WHEN total_billing_amount > 10000 THEN 'High Revenue'
            WHEN total_billing_amount > 5000 THEN 'Medium Revenue'
            ELSE 'Low Revenue'
        END as revenue_segment
    FROM patient_journey pj
)
SELECT 
    patient_segment,
    revenue_segment,
    COUNT(*) as patient_count,
    AVG(total_visits) as avg_visits,
    AVG(total_billing_amount) as avg_revenue,
    AVG(treatment_months) as avg_treatment_duration
FROM journey_segments
GROUP BY patient_segment, revenue_segment
ORDER BY patient_segment, revenue_segment;