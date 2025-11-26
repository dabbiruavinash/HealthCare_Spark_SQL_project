WITH disease_metrics AS (
    SELECT 
        p.patient_id,
        p.first_name,
        p.last_name,
        mr.diagnosis,
        EXTRACT(YEAR FROM mr.visit_date) as visit_year,
        EXTRACT(MONTH FROM mr.visit_date) as visit_month,
        AVG(TO_NUMBER(REGEXP_SUBSTR(lt.result_value, '^\d+(\.\d+)?'))) as metric_value,
        lt.test_name,
        COUNT(*) over (PARTITION BY p.patient_id, mr.diagnosis) as total_readings
    FROM patients p
    JOIN medical_records mr ON p.patient_id = mr.patient_id
    JOIN lab_tests lt ON mr.record_id = lt.record_id
    WHERE mr.diagnosis IN ('Diabetes', 'Hypertension', 'COPD')
      AND lt.test_name IN ('HbA1c', 'Blood Pressure', 'FEV1')
      AND REGEXP_LIKE(lt.result_value, '^\d+(\.\d+)?')
),
progression_analysis AS (
    SELECT 
        patient_id,
        first_name,
        last_name,
        diagnosis,
        test_name,
        visit_year,
        visit_month,
        metric_value,
        LAG(metric_value) OVER (PARTITION BY patient_id, diagnosis, test_name ORDER BY visit_year, visit_month) as prev_metric,
        AVG(metric_value) OVER (PARTITION BY patient_id, diagnosis, test_name ORDER BY visit_year, visit_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg
    FROM disease_metrics
    WHERE total_readings >= 3
),
deterioration_alerts AS (
    SELECT *,
           CASE 
               WHEN test_name = 'HbA1c' AND metric_value > 7.5 AND prev_metric <= 7.5 THEN 'Diabetes Control Deterioration'
               WHEN test_name = 'Blood Pressure' AND metric_value > 140 AND prev_metric <= 140 THEN 'Hypertension Worsening'
               WHEN test_name = 'FEV1' AND metric_value < 60 AND prev_metric >= 60 THEN 'Lung Function Decline'
               ELSE 'Stable'
           END as alert_status
    FROM progression_analysis
    WHERE prev_metric IS NOT NULL)
SELECT * FROM deterioration_alerts
WHERE alert_status != 'Stable'
ORDER BY patient_id, diagnosis, visit_year DESC, visit_month DESC;